from spark_utils import get_spark
from jobs.enrich.config import EnrichConfig
import sys

class WideEnrichTableTransformer:
    def transform(self, user_orders_wide, zipcode):

        # 2️⃣ ジョイン
        df = (
            user_orders_wide
            .join(zipcode.withColumnRenamed("address", "geo_address"), on="zip_code", how="left")
        ).drop("user_id_hash")

        return df

def transform_wide_enrich_table(config, args):
    spark = get_spark()

    user_orders_wide = (
        spark.read
            .format("iceberg")
            #.option("branch", "wapuo")
            .table(config.get_dynamic_table_name(config.TABLE_USER_ORDERS_WIDE, args))
    )

    # ref tableはmainブランチから取得とする
    zipcode  = spark.table(config.TABLE_ZIP_GEOCODE)

    df = WideEnrichTableTransformer().transform(user_orders_wide, zipcode)

    # 書き込み
    df.writeTo(config.get_dynamic_table_name(config.TABLE_USER_ORDERS_WIDE_ENRICHED, args)).overwritePartitions()
    spark.stop()

if __name__ == "__main__":
    args = EnrichConfig.parse_args()
    transform_wide_enrich_table(EnrichConfig(), args)
