from spark_utils import get_spark
from jobs.utilize.config import UtilizeConfig
from pyspark.sql import functions as F
import sys

def write_user_ctx_mongo(config, args):
    spark = get_spark()

    df = (
        spark.read
            .format("iceberg")
            #.option("branch", "wapuo")
            .table(config.get_dynamic_table_name(config.TABLE_USER_CTX_FEATURE, args))
            .alias("u")
    )

    # Filter only coupon-eligible users and select key & flag
    result = (
        df.filter(F.col("rainy_day_coupon_eligible") == True)
        .select("user_id", "rainy_day_coupon_eligible")
    )

    # mongoDBへの書き込み
    result.write.mode("overwrite") \
        .format("mongodb") \
        .option("database", "user_data") \
        .option("collection", f"user_ctx_{args.env}") \
        .save()

    spark.stop()

if __name__ == "__main__":
    args = UtilizeConfig.parse_args()
    write_user_ctx_mongo(UtilizeConfig(), args)
