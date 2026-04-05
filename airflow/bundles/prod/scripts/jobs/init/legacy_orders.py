from pyspark.sql import functions as F
from jobs.base_extractor import BaseExtractor
from jobs.init.config import IntConfig
import sys
from pyspark.sql.types import DecimalType, IntegerType, LongType, TimestampType


class OrdersLegacyExtractor(BaseExtractor):
    def extract(self, spark):
        SRC_CSV_PATH = "s3a://data-source/legacy/legacy_orders.csv"

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # ─── 1) CSV 読み込み
        raw_df = (spark.read.option("header", "true").csv(SRC_CSV_PATH))

        decimal = DecimalType(10, 2)

        df = (raw_df
            .withColumnRenamed("ID",         "注文ID")
            .withColumnRenamed("製品ID",      "商品ID")
            .withColumnRenamed("小計-金額",   "小計_円")
            .withColumnRenamed("税金額",      "税額_円")
            .withColumnRenamed("合計ー円",    "合計_円")
            .withColumnRenamed("数量（個）",  "数量_個")
            .withColumn("注文ID",    F.col("注文ID"))
            # 数値列
            .withColumn("ユーザーID", F.col("ユーザーID").cast(IntegerType()))
            .withColumn("商品ID",    F.col("商品ID").cast(IntegerType()))
            .withColumn("小計_円",   F.col("小計_円").cast(decimal))
            .withColumn("税額_円",   F.col("税額_円").cast(decimal))
            .withColumn("合計_円",   F.col("合計_円").cast(decimal))
            .withColumn("数量_個",   F.col("数量_個").cast(IntegerType()))
            .withColumn("フラグ",    F.col("フラグ").cast(IntegerType()))
            .withColumn("作成日時", F.to_timestamp("作成日時"))
            .withColumn("ingest_date", F.to_date("作成日時"))
        )

        return df

    def target_table(self) -> str: 
        return "spark_catalog.legacy.legacy_orders"

def extract_orders(config, args):
    OrdersLegacyExtractor(config).run_v1(
        TGT_TABLE="spark_catalog.legacy.legacy_orders",
        TABLE_ROOT="s3a://misc-data-platform/warehouse/legacy.db/legacy_orders"
    )

if __name__ == "__main__":
    args=IntConfig.parse_args()
    extract_orders(IntConfig(), args)


# >>> spark.sql("show partitions spark_catalog.legacy.legacy_orders ").show(truncate=False)
# +----------------------+
# |partition             |
# +----------------------+
# |ingest_date=2024-07-30|
# |ingest_date=2024-08-23|
# |ingest_date=2024-09-06|
# |ingest_date=2024-09-08|
# |ingest_date=2024-10-13|
# |ingest_date=2024-10-17|
# |ingest_date=2024-11-07|
# |ingest_date=2024-11-19|
# |ingest_date=2024-11-28|
# |ingest_date=2024-12-23|
# |ingest_date=2025-01-14|
# |ingest_date=2025-01-16|
# |ingest_date=2025-01-22|
# |ingest_date=2025-02-17|
# |ingest_date=2025-04-14|
# |ingest_date=2025-04-21|
# |ingest_date=2025-04-26|
# |ingest_date=2025-04-29|
# |ingest_date=2025-04-30|
# |ingest_date=2025-05-01|
# +----------------------+