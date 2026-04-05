from spark_utils import get_spark
from jobs.utilize.config import UtilizeConfig
from pyspark.sql import functions as F
import sys

COLD_TEMP_THRESHOLD = 10    # 気温がこの値以下ならクーポン対象
LTV_THRESHOLD        = 30000  # 累計購入金額がこの値超過でクーポン対象

def aggregate_user_sales(config, args):
    spark = get_spark()

    # 1. Load enriched data — ingest_date is included in source
    df = (
        spark.read
            .format("iceberg")
            #.option("branch", "wapuo")
            .table(config.get_dynamic_table_name(config.TABLE_USER_ORDERS_WIDE_WEATHER_ENRICHED, args))
            .alias("u")
    )

    # 2. Aggregate per user, include ingest_date in grouping
    agg_df = (
        df.groupBy("user_id", "ingest_date")
        .agg(
            F.sum("total_usd").alias("total_purchase_amount"),
            F.countDistinct("order_id").alias("order_count"),
            F.max(F.when(F.col("weather")=="RAINY", True).otherwise(False))
                .alias("rainy_flag"),
            F.max("temperature_celsius").alias("temperature_celsius"),
            F.max("weather_datetime").alias("weather_datetime")
        )
    )

    # 3. Calculate coupon eligibility
    # ルールベースモデルでクーポン対象を決める
    coupon_df = (
        agg_df.withColumn(
            "rainy_day_coupon_eligible",
            (F.col("rainy_flag") |
            (F.col("temperature_celsius") < F.lit(COLD_TEMP_THRESHOLD)) |
            (F.col("total_purchase_amount") > F.lit(LTV_THRESHOLD)))
        )
    ).drop("rainy_flag")

    coupon_df.writeTo(config.get_dynamic_table_name(config.TABLE_USER_CTX_FEATURE, args)).overwritePartitions()

    spark.stop()

if __name__ == "__main__":
    args = UtilizeConfig.parse_args()
    aggregate_user_sales(UtilizeConfig(), args)
