from spark_utils import get_spark
from jobs.basic_pipeline.config import BasicPipelineConfig
from pyspark.sql import functions as F
import sys

def aggregate_user_sales(config, args):
    spark = get_spark()

    df = (
        spark.read
            .format("iceberg")
            #.option("branch", args.env)
            .table(config.get_dynamic_table_name(config.TABLE_USER_ORDERS_WIDE, args))
    )

    agg_df = (
        df.groupBy("user_id", "user_name")
          .agg(
            F.count("order_id").alias("order_count"),      # 注文ID 件数集計
            F.sum("total_usd").alias("total_sales")        # 合計USD の合計
          )
    )

    agg_df.writeTo(config.get_dynamic_table_name(config.TABLE_USER_SALES, args)).overwritePartitions()

    spark.stop()

if __name__ == "__main__":
    args = BasicPipelineConfig.parse_args()
    aggregate_user_sales(BasicPipelineConfig(), args)
