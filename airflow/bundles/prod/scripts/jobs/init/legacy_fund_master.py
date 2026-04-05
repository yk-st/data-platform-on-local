from pyspark.sql import functions as F
from jobs.base_extractor import BaseExtractor
from jobs.init.config import IntConfig
import sys
from pyspark.sql.types import DecimalType, IntegerType, LongType, TimestampType


class FundMasterLegacyExtractor(BaseExtractor):
    def extract(self, spark):
        SRC_CSV_PATH = "s3a://data-source/legacy/legacy_fund_master.csv"

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # ─── 1) CSV 読み込み
        raw_df = (spark.read.option("header", "true").csv(SRC_CSV_PATH))

        decimal_4_2 = DecimalType(4, 2)

        df = (raw_df
            # 数値列のキャスト
            .withColumn("信託報酬_率", F.col("信託報酬_率").cast(decimal_4_2))
            .withColumn("隠れコスト", F.col("隠れコスト").cast(decimal_4_2))
            .withColumn("有効レコード", F.col("有効レコード").cast("boolean"))
            # ingest_date を現在日付として追加
            .withColumn("ingest_date", F.current_date())
        )

        return df

    def target_table(self) -> str: 
        return "spark_catalog.legacy.legacy_fund_master"

def extract_fund_master(config, args):
    FundMasterLegacyExtractor(config, args).run_v1(
        TGT_TABLE="spark_catalog.legacy.legacy_fund_master",
        TABLE_ROOT="s3a://misc-data-platform/warehouse/legacy.db/legacy_fund_master"
    )

if __name__ == "__main__":
    args = IntConfig.parse_args()
    extract_fund_master(IntConfig(), args)
