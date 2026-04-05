from spark_utils import get_spark
from jobs.streaming.config import StreamingPipelineConfig
from pyspark.sql import functions as F
from pyspark.sql import Window
import sys

def aggregate_user_sales_rank(config, args):
    spark = get_spark()

    # Gold テーブル作成(確報値格納用)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local_data_platform.gld_mart.daily_user_ranking (
            as_of_date DATE,
            `user_id` INT,
            `total_purchase_amount`   DOUBLE,
            `max_order_amount`    DOUBLE,
            `order_count`        BIGINT,
            rank        INT
        ) USING iceberg
        PARTITIONED BY (as_of_date)
        TBLPROPERTIES (
            'openlineage.dataset.namespace'='local_data_platform.gld_mart',
            'openlineage.dataset.name'='daily_user_ranking',
            'write.format.default' = 'parquet',
            'format-version'       = '2',
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes' = '268435456',
            'history.expire.max-snapshot-age-ms' = '2592000000',
            'history.expire.min-snapshots-to-keep' = '20'
        );
    """)



    # spark.sql("""
    #     CREATE TABLE IF NOT EXISTS local_data_platform.slv_entities.orders_slide_window (
    #         `user_id`    INT NOT NULL,
    #         session_start  TIMESTAMP NOT NULL,
    #         session_end    TIMESTAMP,
    #         `order_count`        BIGINT,
    #         `total_purchase_amount`      DOUBLE,
    #         `max_order_amount`    DOUBLE,
    #         ingest_ts      TIMESTAMP


    # 直近 24 時間分を Raw から取得(対象テーブルは作成日時でパーティション済み)
    raw_24h = (
        spark.read
        .table("local_data_platform.brz_ingestion.orders_microbatch_window_raw")
        .where(F.col("created_at") >= F.lit(args.ingest_date).cast("timestamp") - F.expr("INTERVAL 24 HOURS"))
    )

    # Sliding 60m / 30s を Raw から DataFrame API で再構築
    window_df = (
        raw_24h
        .withColumn("as_of_date", F.to_date(F.col("created_at")))  # 先にカラムを作成
        .groupBy(
            "as_of_date",
            F.col("`user_id`")
        )
        .agg(
            F.count("*").alias("order_count"),
            F.sum(F.col("total_usd")).alias("total_purchase_amount"),
            F.max(F.col("total_usd")).alias("max_order_amount")
        )
        .withColumn(
            "rank",
            F.row_number().over(
                Window
                .partitionBy("as_of_date")
                .orderBy(F.col("total_purchase_amount").desc())
            )
        )
    )

    # # 日次 TOP-100 ランキング
    # gold_df = (
    #     window_df
    #     .groupBy("as_of_date", "`user_id`")
    #     .agg(
    #         F.sum("total_purchase_amount").alias("売上24h")
    #     )
    #     .withColumn(
    #         "rank",
    #         F.row_number().over(
    #             Window
    #             .partitionBy("as_of_date")
    #             .orderBy(F.col("売上24h").desc())
    #         )
    #     )
    #     .filter(F.col("rank") <= 100)
    # )

    # Iceberg Gold 上書き
    (
        window_df.write
        .format("iceberg")
        .mode("overwrite")
        .partitionBy("as_of_date")
        .save("local_data_platform.gld_mart.daily_user_ranking")
    )

    # Mongo 確報に overwrite
    (
        window_df.filter(F.col("rank") <= 100)
        .withColumn("_id",
            F.concat_ws("-", 
                F.col("as_of_date").cast("string"),
                F.col("`user_id`").cast("string")
            )
        )
        .write
        .format("mongodb")
        .mode("overwrite")
        .option("replaceDocument", "true")
        .option("database", "user_data")
        .option("collection", "ranking_final")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    args = StreamingPipelineConfig.parse_args()
    aggregate_user_sales_rank(StreamingPipelineConfig(), args)


# user_data> db.ranking_final.findOne()
# {
#   _id: '2025-09-11-98',
#   as_of_date: ISODate("2025-09-11T00:00:00.000Z"),
#   user_id: 98,
#   order_count: Long("273"),
#   total_purchase_amount: 1406340.8700000006,
#   max_order_amount: 44381.75,
#   rank: 1
# }


# >>> spark.read.table("local_data_platform.gld_mart.daily_user_ranking").orderBy(F.col("rank").asc()).show(n=3, truncate=False)
# +----------+-------+---------------------+----------------+-----------+----+    
# |as_of_date|user_id|total_purchase_amount|max_order_amount|order_count|rank|
# +----------+-------+---------------------+----------------+-----------+----+
# |2025-09-11|98     |1406340.8700000006   |44381.75        |273        |1   |
# |2025-09-11|9999   |259807.95            |29898.72        |15         |2   |
# |2025-09-11|7777   |233767.44            |38034.18        |14         |3   |
# +----------+-------+---------------------+----------------+-----------+----+