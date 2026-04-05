from pyspark.sql import SparkSession, functions as F, types as T
import time
from pyspark.sql.window import Window

# ウィンドウ処理付きSpark Streaming プログラム
# このプログラムは、Kafka から注文データをストリーミングで読み込み、
# スライディングウィンドウ処理を行いIceberg テーブルとMongoDBに書き込むものです。
# spark-submit /home/pyspark/programs/spark/streaming/spark_s_window.py

def main():
    # Spark セッションの作成
    spark = SparkSession.builder \
        .appName("KafkaToIcebergWithWindows") \
        .config("spark.mongodb.read.connection.uri", "mongodb://action:pass123@host.docker.internal:27017/user_data") \
        .config("spark.mongodb.write.connection.uri", "mongodb://action:pass123@host.docker.internal:27017/user_data") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Kafka 接続設定
    kafka_bootstrap = "kafka1.local.data.platform:9093,kafka2.local.data.platform:9093"
    kafka_topic = "orders_topic"  # 注文データ用のトピック
    sasl_username = "admin"
    sasl_password = "admin"

    # Kafka からストリーミング読み込み
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", kafka_topic) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("maxOffsetsPerTrigger", 100_000) \
        .option("kafka.max.poll.records", "2000") \
        .option("kafka.fetch.max.bytes", "20971520") \
        .option("kafka.max.partition.fetch.bytes", "5242880") \
        .option("kafka.max.poll.interval.ms", "900000") \
        .option("minPartitions", 2) \
        .option("kafka.sasl.jaas.config", 
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{sasl_username}" password="{sasl_password}";') \
        .load()

    # ─ Bronze テーブル DDL
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local_data_platform.brz_ingestion.orders_microbatch_window_raw (
            ID            STRING,
            `user_id`   INT,
            `product_id`       INT,
            `subtotal_usd`    DOUBLE,
            `tax_usd`       DOUBLE,
            `total_usd`     DOUBLE,
            `quantity`      INT,
            `status_flag`       INT,
            `created_at`     TIMESTAMP,
            processing_time TIMESTAMP,
            ingest_ts     TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(`created_at`))
        TBLPROPERTIES (
            'format-version'='2',
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes' = '268435456',
            'history.expire.max-snapshot-age-ms' = '2592000000',
            'history.expire.min-snapshots-to-keep' = '20'
        );
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local_data_platform.slv_entities.orders_slide_window (
            `user_id`    INT NOT NULL,
            session_start  TIMESTAMP NOT NULL,
            session_end    TIMESTAMP,
            `order_count`        BIGINT,
            `total_purchase_amount`      DOUBLE,
            `max_order_amount`    DOUBLE,
            `rank`        INT,
            ingest_ts      TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(session_start))
        TBLPROPERTIES (
            'format-version'='2', 
            'write.upsert.enabled'='true',
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes' = '268435456',
            'history.expire.max-snapshot-age-ms' = '2592000000',
            'history.expire.min-snapshots-to-keep' = '20',
            'identifier-fields'='user_id, session_start'
        );
    """)
    
    # ─ ALTER TABLE文の修正
    spark.sql("""
        ALTER TABLE local_data_platform.slv_entities.orders_slide_window
        SET IDENTIFIER FIELDS `user_id`, session_start
    """)

    # ─ Kafka → DataFrame
    schema = T.StructType([
        T.StructField("id",        T.StringType()),
        T.StructField("user_id", T.IntegerType()),
        T.StructField("product_id",     T.IntegerType()),
        T.StructField("subtotal_usd",  T.DoubleType()),
        T.StructField("tax_usd",    T.DoubleType()),
        T.StructField("total_usd",   T.DoubleType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("status_flag",    T.IntegerType()),
        T.StructField("created_at",  T.StringType())
    ])

    parsed = (
        kafka_df
        .select(F.from_json(F.col("value").cast("string"), schema).alias("x"))
        .select("x.*")
        .withColumn("created_at", F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("processing_time", F.current_timestamp())
        .withColumnRenamed("subtotal_usd", "subtotal_usd")
        .withColumnRenamed("quantity", "quantity")
        .withColumn("ingest_ts", F.current_timestamp())
    )

    # ─ Bronze-Raw append
    def write_raw(df, bid):
        if not df.isEmpty():
            df.writeTo("local_data_platform.brz_ingestion.orders_microbatch_window_raw").append()

    raw_query = (
        parsed.writeStream
        .foreachBatch(write_raw)
        .option("checkpointLocation", "s3a://process-bucket/chk/raw")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ─ Sliding 集計 → Iceberg & Mongo
    sliding_df = (
        parsed
        .withWatermark("`created_at`", "10 minutes")
        .dropDuplicatesWithinWatermark()
        .groupBy(
            # sliding window
            F.window(F.col("created_at"), "60 minutes", "30 seconds"), 
            F.col("user_id")
        )
        .agg(
            F.count("*").alias("order_count"),
            F.sum(F.col("total_usd")).alias("total_purchase_amount"),
            F.max(F.col("total_usd")).alias("max_order_amount")
        )
        # ROW_NUMBER()を削除
        .selectExpr(
            "`user_id`",
            "window.start as session_start",
            "window.end as session_end",
            "`order_count`", 
            "`total_purchase_amount`", 
            "`max_order_amount`",
            "current_timestamp() as ingest_ts"
        )
    )

    def write_sliding(df, bid):
        if df.isEmpty():
            return

        print(f"[BATCH {bid}] Window集計データ")
        df.show(n=10, truncate=False)

        try:
            # 最新セッションのみを取得（エイリアスを使用してカラム参照を明確化）
            latest_sessions = (
                df.alias("df1")
                .groupBy("user_id")
                .agg(
                    F.max("session_start").alias("latest_session_start")
                )
                .alias("df2")
                .join(
                    df.alias("df3"), 
                    (F.col("df2.user_id") == F.col("df3.user_id")) & 
                    (F.col("df2.latest_session_start") == F.col("df3.session_start")),
                    "inner"
                )
                .select("df3.*")  # df3のすべてのカラムを選択
                # ランキングを追加
                .withColumn("rank",
                    F.row_number().over(
                        Window.orderBy(F.col("total_purchase_amount").desc())
                    )
                )
            )

            # TOP10ランキングのみを抽出
            top10_users = latest_sessions.filter(F.col("rank") <= 10).orderBy("rank")
            
            print(f"[BATCH {bid}] 🏆 売上ランキング TOP10:")
            top10_users.select(
                "rank",
                "user_id", 
                "total_purchase_amount",
                "order_count",
                "session_start"
            ).show(n=10, truncate=False)

            # TOP10のみをIceberg Upsert Sink
            top10_users.writeTo("local_data_platform.slv_entities.orders_slide_window").append()

            # MongoDB 速報 Upsert（TOP10）
            (
                top10_users.withColumn("_id", F.col("user_id").cast("string"))
                .drop("ingest_ts")
                .write
                .format("mongodb")
                .mode("overwrite")
                .option("replaceDocument", "true")
                .option("database", "user_data")
                .option("collection", "user_sessions_rt")
                .save()
            )

        except Exception as e:
            print(f"Error in write_sliding: {e}")

    sliding_query = (
        sliding_df.writeStream
        .foreachBatch(write_sliding)
        .outputMode("update")  # ← 差分を毎バッチ渡す(30 sごとに更新させる)
        #.outputMode("append")  # ← 確定で一回だけ出力させる（60m + watermark）
        .option("checkpointLocation", "s3a://process-bucket/chk/sess")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 全ストリーミングクエリの完了を待機
    spark.streams.awaitAnyTermination()

    
if __name__ == "__main__":
    main()


#append


# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |ユーザーID|session_start      |session_end        |注文数|合計金額          |最高注文額|ingest_ts              |
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |98        |2025-06-26 04:00:00|2025-06-26 05:00:00|2     |4655.9400000000005|2556.68   |2025-06-26 05:10:52.132|
# |98        |2025-06-26 03:59:30|2025-06-26 04:59:30|2     |4655.9400000000005|2556.68   |2025-06-26 05:10:52.132|
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+


# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |ユーザーID|session_start      |session_end        |注文数|合計金額          |最高注文額|ingest_ts              |
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |98        |2025-06-26 04:00:30|2025-06-26 05:00:30|2     |4655.9400000000005|2556.68   |2025-06-26 05:11:00.008|
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+


# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |ユーザーID|session_start      |session_end        |注文数|合計金額          |最高注文額|ingest_ts              |
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |98        |2025-06-26 04:03:00|2025-06-26 05:03:00|2     |4655.9400000000005|2556.68   |2025-06-26 05:14:00.102|
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+


# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |ユーザーID|session_start      |session_end        |注文数|合計金額          |最高注文額|ingest_ts              |
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |98        |2025-06-26 04:04:00|2025-06-26 05:04:00|2     |4655.9400000000005|2556.68   |2025-06-26 05:15:00.068|
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+

# +----------+-------------------+-------------------+------+--------+----------+-----------------------+
# |ユーザーID|session_start      |session_end        |注文数|合計金額|最高注文額|ingest_ts              |
# +----------+-------------------+-------------------+------+--------+----------+-----------------------+
# |98        |2025-06-26 04:05:00|2025-06-26 05:05:00|3     |5211.55 |2556.68   |2025-06-26 05:16:00.064|
# +----------+-------------------+-------------------+------+--------+----------+-----------------------+

# 2025年  6月 26日 木曜日 05:21:55 UTC + watermark 10mなので、確定した行が出てきているのが以下。上も同じ。

# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |ユーザーID|session_start      |session_end        |注文数|合計金額          |最高注文額|ingest_ts              |
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |98        |2025-06-26 04:09:30|2025-06-26 05:09:30|5     |13777.689999999999|4467.98   |2025-06-26 05:20:30.051|
# |98        |2025-06-26 04:10:00|2025-06-26 05:10:00|5     |13777.689999999999|4467.98   |2025-06-26 05:20:30.051|
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+


#update

#そのウィンドウのデータが順次変更される

# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |ユーザーID|session_start      |session_end        |注文数|合計金額          |最高注文額|ingest_ts              |
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |98        |2025-06-26 05:27:30|2025-06-26 06:27:30|1     |651.17            |651.17    |2025-06-26 05:30:00.042|
# |98        |2025-06-26 05:25:30|2025-06-26 06:25:30|4     |8296.050000000001 |4020.67   |2025-06-26 05:30:00.042|
# |98        |2025-06-26 05:23:30|2025-06-26 06:23:30|8     |16402.39          |4020.67   |2025-06-26 05:30:00.042|
# |98        |2025-06-26 05:22:00|2025-06-26 06:22:00|10    |21080.209999999995|4020.67   |2025-06-26 05:30:00.042|
# |98        |2025-06-26 05:13:00|2025-06-26 06:13:00|21    |42285.42          |4020.67   |2025-06-26 05:30:00.042|
# |98        |2025-06-26 05:09:00|2025-06-26 06:09:00|26    |49800.64          |4056.8    |2025-06-26 05:30:00.042|
# |98        |2025-06-26 05:07:30|2025-06-26 06:07:30|27    |54268.62          |4467.98   |2025-06-26 05:30:00.042|
# |98        |2025-06-26 05:03:30|2025-06-26 06:03:30|29    |58922.39          |4467.98   |2025-06-26 05:30:00.042|
# |98        |2025-06-26 05:01:30|2025-06-26 06:01:30|29    |58922.39          |4467.98   |2025-06-26 05:30:00.042|
# |98        |2025-06-26 04:56:30|2025-06-26 05:56:30|30    |61479.07          |4467.98   |2025-06-26 05:30:00.042|
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+c

# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |ユーザーID|session_start      |session_end        |注文数|合計金額          |最高注文額|ingest_ts              |
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |98        |2025-06-26 05:30:30|2025-06-26 06:30:30|1     |1291.03           |1291.03   |2025-06-26 05:31:00.084|
# |98        |2025-06-26 05:27:30|2025-06-26 06:27:30|3     |2234.91           |1291.03   |2025-06-26 05:31:00.084|
# |98        |2025-06-26 05:25:30|2025-06-26 06:25:30|6     |9879.79           |4020.67   |2025-06-26 05:31:00.084|
# |98        |2025-06-26 05:23:30|2025-06-26 06:23:30|10    |17986.129999999997|4020.67   |2025-06-26 05:31:00.084|
# |98        |2025-06-26 05:22:00|2025-06-26 06:22:00|12    |22663.949999999993|4020.67   |2025-06-26 05:31:00.084|
# |98        |2025-06-26 05:13:00|2025-06-26 06:13:00|23    |43869.159999999996|4020.67   |2025-06-26 05:31:00.084|
# |98        |2025-06-26 05:09:00|2025-06-26 06:09:00|28    |51384.38          |4056.8    |2025-06-26 05:31:00.084|
# |98        |2025-06-26 05:07:30|2025-06-26 06:07:30|29    |55852.36          |4467.98   |2025-06-26 05:31:00.084|
# |98        |2025-06-26 05:03:30|2025-06-26 06:03:30|31    |60506.13          |4467.98   |2025-06-26 05:31:00.084|
# |98        |2025-06-26 05:01:30|2025-06-26 06:01:30|31    |60506.13          |4467.98   |2025-06-26 05:31:00.084|
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+


# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |ユーザーID|session_start      |session_end        |注文数|合計金額          |最高注文額|ingest_ts              |
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+
# |98        |2025-06-26 05:30:30|2025-06-26 06:30:30|7     |9951.650000000001 |3683.55   |2025-06-26 05:37:00.041|
# |98        |2025-06-26 05:27:30|2025-06-26 06:27:30|9     |10895.529999999999|3683.55   |2025-06-26 05:37:00.041|
# |98        |2025-06-26 05:25:30|2025-06-26 06:25:30|12    |18540.41          |4020.67   |2025-06-26 05:37:00.041|
# |98        |2025-06-26 05:23:30|2025-06-26 06:23:30|16    |26646.75          |4020.67   |2025-06-26 05:37:00.041|
# |98        |2025-06-26 05:22:00|2025-06-26 06:22:00|18    |31324.569999999996|4020.67   |2025-06-26 05:37:00.041|
# |98        |2025-06-26 05:13:00|2025-06-26 06:13:00|29    |52529.78          |4020.67   |2025-06-26 05:37:00.041|
# |98        |2025-06-26 05:09:00|2025-06-26 06:09:00|34    |60045.0           |4056.8    |2025-06-26 05:37:00.041|
# |98        |2025-06-26 05:07:30|2025-06-26 06:07:30|35    |64512.98          |4467.98   |2025-06-26 05:37:00.041|
# |98        |2025-06-26 05:03:30|2025-06-26 06:03:30|37    |69166.74999999999 |4467.98   |2025-06-26 05:37:00.041|
# |98        |2025-06-26 05:01:30|2025-06-26 06:01:30|37    |69166.74999999999 |4467.98   |2025-06-26 05:37:00.041|
# +----------+-------------------+-------------------+------+------------------+----------+-----------------------+