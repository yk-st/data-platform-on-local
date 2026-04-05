from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import StorageLevel

#spark-submit /home/pyspark/programs/spark/streaming/spark_streaming.py

# ── ❶ users を静的 DF としてロード ＋ broadcast ────────────────
def load_users_static(spark):
    users_df = (spark.read
                 .format("iceberg")
                 .load("local_data_platform.brz_ingestion.users")
                 .select("user_id", "zip_code", "address"))   # 本当に使う列だけ
    return F.broadcast(users_df)          # サイズが小さいうちは broadcast

# section2用のSpark Streaming プログラム
# このプログラムは、Kafka から注文データをストリーミングで読み込み、Iceberg テーブルに書き込むものです。
def sync_tee(df, epoch_id, users_static):

    df_cached = df.persist(StorageLevel.MEMORY_AND_DISK)

    # ── 1. コンソール出力 ─────────────────────────
    print(f"===== Batch {epoch_id} の先頭 20 行 =====")
    result = df_cached.join(users_static, on="user_id", how="left").withColumn("method", F.lit("Tee"))

    result.show(truncate=False, n=20)

    # ── 2. Iceberg へ Append ───────────────────────
    result.drop("method").write \
      .format("iceberg") \
      .mode("append") \
      .saveAsTable("local_data_platform.brz_ingestion.orders_microbatch")

    df_cached.unpersist()

def main():
    spark = (SparkSession.builder
             .appName("KafkaToIceberg_SyncTee")
             # Iceberg のコンパクションは後段バッチで行う想定
             .config("spark.databricks.delta.autoCompact.enabled", "false")
             .config("spark.databricks.delta.optimizeWrite.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # users 静的 DF をロード（起動時に 1 回だけ）
    users_static = load_users_static(spark)

    # Kafka からストリーミング読み込み
    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers",
                        "kafka1.local.data.platform:9093,kafka2.local.data.platform:9093")
                .option("subscribe", "orders_topic")
                .option("kafka.security.protocol", "SASL_PLAINTEXT")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config",
                        'org.apache.kafka.common.security.plain.PlainLoginModule '
                        'required username="admin" password="admin";')
                .load())

    # JSON パース用スキーマ
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

    parsed_df = (kafka_df
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
                 .select("data.*")
                 .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
                 .withColumn("processing_time", F.current_timestamp()))

    # Iceberg テーブルがなければ作成
    spark.sql("""
    CREATE TABLE IF NOT EXISTS local_data_platform.brz_ingestion.orders_microbatch (
      id string,
      user_id int,
      product_id int,
      subtotal_usd double,
      tax_usd double,
      total_usd double,
      quantity int,
      status_flag int,
      created_at timestamp,
      processing_time timestamp,
      zip_code string,
      address string
    )
    USING iceberg
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'format-version'       = '2',
        'write.distribution-mode'='hash',
        'write.target-file-size-bytes' = '268435456',
        'history.expire.max-snapshot-age-ms' = '2592000000',
        'history.expire.min-snapshots-to-keep' = '20',
        'write.merge.mode' = 'merge-on-read',
        'write.delete.mode' = 'merge-on-read'
    )
    PARTITIONED BY (days(`created_at`))
    """)

    # ── 1 本の writeStream で “console + iceberg” を同期ティー ─────────────
    query = (parsed_df.writeStream
             .foreachBatch(lambda df, eid: sync_tee(df, eid, users_static))
             .option("checkpointLocation",
                     "s3a://process-bucket/checkpoints/orders_sync_tee")
             .outputMode("append")
             .trigger(processingTime="60 seconds")
             .start())


    # 2つのストリームを用意するブランチ方式
    # usersとのJOINを実施
    console_query = (parsed_df
                     .join(users_static, on="user_id", how="left")
                     .withColumn("method", F.lit("Branch"))
                     .writeStream
                     .format("console")
                     .option("truncate", False)
                     .option("numRows", 20)
                     .option("checkpointLocation",
                             "s3a://process-bucket/checkpoints/orders_console")
                     .trigger(processingTime="60 seconds")
                     .start())

    # usersとのJOINを実施しないパターン
    console_query_no_join_users = (parsed_df
                     .withColumn("method", F.lit("Branch2"))
                     .writeStream
                     .format("console")
                     .option("truncate", False)
                     .option("numRows", 20)
                     .option("checkpointLocation",
                             "s3a://process-bucket/checkpoints/orders_console_no_join_users")
                     .trigger(processingTime="60 seconds")
                     .start())



# +------------------------------------+----------+------+---------+------+--------+----------+------+-------------------+-----------------------+------+
# |ID                                  |ユーザーID|製品ID|小計-金額|税金額|合計ー円|数量（個）|フラグ|作成日時           |processing_time        |method|
# +------------------------------------+----------+------+---------+------+--------+----------+------+-------------------+-----------------------+------+
# |0e2c8cec-9468-408e-aeff-e51f5221b718|12        |53    |1573.14  |157.31|1730.45 |2         |1     |2025-06-24 01:56:49|2025-06-24 01:57:00.024|Branch|
# |02d1a187-8edf-429e-984b-d806dfc74cc5|76        |19    |3428.36  |342.84|3771.2  |4         |1     |2025-06-24 01:56:54|2025-06-24 01:57:00.024|Branch|
# |a0afde5c-f31c-44b3-a4b3-f43f34c27d12|72        |12    |3171.84  |317.18|3489.02 |4         |1     |2025-06-24 01:56:59|2025-06-24 01:57:00.024|Branch|
# +------------------------------------+----------+------+---------+------+--------+----------+------+-------------------+-----------------------+------+


# +------------------------------------+----------+------+---------+------+--------+----------+------+-------------------+-----------------------+------+
# |ID                                  |ユーザーID|製品ID|小計-金額|税金額|合計ー円|数量（個）|フラグ|作成日時           |processing_time        |method|
# +------------------------------------+----------+------+---------+------+--------+----------+------+-------------------+-----------------------+------+
# |0e2c8cec-9468-408e-aeff-e51f5221b718|12        |53    |1573.14  |157.31|1730.45 |2         |1     |2025-06-24 01:56:49|2025-06-24 01:57:00.026|Tee   |
# |02d1a187-8edf-429e-984b-d806dfc74cc5|76        |19    |3428.36  |342.84|3771.2  |4         |1     |2025-06-24 01:56:54|2025-06-24 01:57:00.026|Tee   |
# |a0afde5c-f31c-44b3-a4b3-f43f34c27d12|72        |12    |3171.84  |317.18|3489.02 |4         |1     |2025-06-24 01:56:59|2025-06-24 01:57:00.026|Tee   |
# +------------------------------------+----------+------+---------+------+--------+----------+------+-------------------+-----------------------+------+

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()