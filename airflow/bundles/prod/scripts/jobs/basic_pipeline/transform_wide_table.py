from spark_utils import get_spark
from jobs.basic_pipeline.config import BasicPipelineConfig
import sys

class WideTableTransformer:
    def transform(self, args, orders, products, users):

        # 2️⃣ ジョイン
        df = (
            orders
            .join(
                products, 
                (orders["product_id"] == products["product_id"]) & 
                (orders["ingest_date"] == products["ingest_date"]), 
                "left"
            )
            .join(
                users,
                (orders["user_id"] == users["user_id"]) &
                (orders["ingest_date"] == users["ingest_date"]),
                "left"
            )
        )

        # 3️⃣ 必要カラムを選択・エイリアス
        cols = [
            orders["order_id"],
            orders["user_id"],
            orders["product_id"],
            orders["subtotal_usd"],
            orders["tax_usd"],
            orders["total_usd"],
            orders["quantity"],
            orders["status_flag"],
            orders["created_at"].alias("order_created_at"),
            orders["parent_id"],

            products["ean_code"],
            products["product_title"],
            products["category"],
            products["vendor_name"],
            products["price_usd"],
            products["rating"],
            products["created_at"].alias("product_created_at"),
            products["updated_at"].alias("product_updated_at"),
            products["is_deleted"].alias("product_is_deleted"),
            products["deleted_at"].alias("product_deleted_at"),

            users["address"],
            users["email"],
            users["password"],
            users["user_name"],
            users["acquisition_channel"],
            users["birth_date"],
            users["zip_code"],
            users["created_at"].alias("user_created_at"),
            users["updated_at"].alias("user_updated_at"),
            users["is_deleted"].alias("user_is_deleted"),
            users["deleted_at"].alias("user_deleted_at"),

            orders["ingest_date"],
            orders["user_id_hash"]
        ]

        wide_df = df.select(*cols)

        return wide_df

def transform_wide_table(config, args):
    spark = get_spark()

    orders = (
        spark.read
            .format("iceberg")
            #.option("branch", args.env)
            .table(config.get_dynamic_table_name(config.TABLE_ORDERS, args))
            .alias("o")
    )

    products = (
        spark.read
            .format("iceberg")
            #.option("branch", args.env)
            .table(config.get_dynamic_table_name(config.TABLE_PRODUCTS, args))
            .alias("p")
    )

    users = (
        spark.read
            .format("iceberg")
            #.option("branch", "wapuo")
            .table(config.get_dynamic_table_name(config.TABLE_USERS, args))
            .alias("u")
    )
    
    # 1️⃣ ワイドテーブル変換
    df = WideTableTransformer().transform(args, orders, products, users)

    df.writeTo(config.get_dynamic_table_name(config.TABLE_USER_ORDERS_WIDE, args)).overwritePartitions()

    spark.stop()

if __name__ == "__main__":
    args = BasicPipelineConfig.parse_args()
    transform_wide_table(BasicPipelineConfig(), args)
