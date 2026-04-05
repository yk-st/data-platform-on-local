from pyspark.sql import functions as F
from jobs.base_extractor import BaseExtractor
from jobs.basic_pipeline.config import BasicPipelineConfig
import sys
from utils.column_sanitize import sanitize_column_name
from pyspark.sql.types import DoubleType, IntegerType

class OrderExtractor(BaseExtractor):
    def extract(self, spark):

        # subquery = f"""
        #     (SELECT *
        #     FROM public.orders
        #     WHERE logical_date > DATE '{self.args.ingest_date}') AS orders_sub
        # """

        extract_mode = self.args.extract_mode
        ingest_date = self.args.ingest_date

        # WHERE句を条件に応じて動的に生成
        where_clause = (
            f"WHERE DATE(\"creation_date\") = DATE '{ingest_date}'"
            if extract_mode == 'logical_date' 
            else ""
        )
        
        subquery = f"""
            (SELECT 
                "id",
                "user_id",
                "product_id",
                CASE 
                    WHEN "subtotal_usd" = 'NaN'::numeric OR "subtotal_usd" IS NULL THEN NULL
                    ELSE "subtotal_usd" 
                END as "subtotal_usd",
                CASE 
                    WHEN "tax_usd" = 'NaN'::numeric OR "tax_usd" IS NULL THEN NULL
                    ELSE "tax_usd" 
                END as "tax_usd",
                CASE 
                    WHEN "total_usd" = 'NaN'::numeric OR "total_usd" IS NULL THEN NULL
                    ELSE "total_usd" 
                END as "total_usd",
                "quantity",
                "flag",
                "creation_date",
                "parent_id"
            FROM public.orders
            {where_clause}) AS orders_sub
        """

        print(subquery)

        options = self.build("creation_date", subquery, lower_bound="2024-01-01", upper_bound="2025-01-01", num_partitions=4)

        src_df = (spark.read.format("jdbc")
                 .options(**options)
                 .load())
        
        # 表記の揺れを治す
        extra_jitter_map = {
            "id": "order_id",
            "creation_date": "created_at",
            "flag": "status_flag"
        }
        cols = [F.col(c).alias(sanitize_column_name(c, extra_jitter_map)) for c in src_df.columns]
        print(f"Extracted columns: {cols}")
        print(f"args: {self.args}")

        src_df = src_df.select(*cols)

        src_df = src_df.withColumn("created_at", F.to_utc_timestamp(F.col("created_at"), "UTC")) 
        src_df = src_df.withColumn("ingest_date", F.lit(ingest_date).cast("timestamp"))
        src_df = src_df.withColumn("user_id_hash", F.xxhash64("user_id").cast("bigint"))

        src_df = self._clean_numeric_columns(src_df)

        return src_df

    def target_table(self) -> str: 
        return self.config.get_dynamic_table_name(self.config.TABLE_ORDERS, self.args)

    def _clean_numeric_columns(self, df):
        """
        数値カラムのNaN値とnull値を適切に処理する
        """
        # 数値カラムのリスト（変換後のカラム名）
        numeric_columns = [
            "subtotal_usd",
            "tax_usd", 
            "total_usd",
            "quantity"
        ]
        
        for col_name in numeric_columns:  # 修正: .items()を削除
            if col_name in df.columns:
                print(f"Processing column: {col_name}")
                
                # NaN値と無効値をnullに変換
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNull(), None)
                    .when(F.isnan(F.col(col_name)), None)  # NaN値を明示的に処理
                    .otherwise(F.col(col_name))
                )
                
        return df

def extract_orders(config, args):
    OrderExtractor(config, args).run()

if __name__ == "__main__":
    args=BasicPipelineConfig.parse_args()
    extract_orders(BasicPipelineConfig(), args)
