from pyspark.sql import functions as F
from jobs.base_extractor import BaseExtractor
from jobs.basic_pipeline.config import BasicPipelineConfig
from utils.column_sanitize import sanitize_column_name
import sys

class PeopleExtractor(BaseExtractor):
    def extract(self, spark):
        
        ingest_date = self.args.ingest_date

        # 説明のために共通の関数は呼び出さない。extract_orders.pyの取得方法を推奨
        src_df = (spark.read.format("jdbc")
                 .option("url", self.config.POSTGRES_URL)
                 .option("dbtable", "public.users")
                 .option("user", self.config.POSTGRES_USER)
                 .option("password", self.config.POSTGRES_PASSWORD)
                 .load())

        extra_jitter_map = {
            "id": "user_id"
        }
        cols = [F.col(c).alias(sanitize_column_name(c, extra_jitter_map)) for c in src_df.columns]
        print(f"Extracted columns: {cols}")

        src_df = src_df.select(*cols)

        # src_df = src_df.withColumn("作成日時", F.to_utc_timestamp(F.col("作成日時"), "Asia/Tokyo")) \
        #         .withColumn("更新日時", F.to_utc_timestamp(F.col("更新日時"), "Asia/Tokyo"))
        src_df = src_df.withColumn("created_at", F.to_utc_timestamp(F.col("created_at"), "UTC")) \
                .withColumn("updated_at", F.to_utc_timestamp(F.col("updated_at"), "UTC"))
        src_df = src_df.withColumn("ingest_date", F.lit(ingest_date).cast("timestamp"))
        src_df = src_df.withColumn("user_id_hash", F.xxhash64(F.col("user_id")).cast("bigint"))

        return src_df

    def target_table(self) -> str: 
        return self.config.get_dynamic_table_name(self.config.TABLE_USERS, self.args)

def extract_users(config, args):
    PeopleExtractor(config, args).run()

if __name__ == "__main__":
    args=BasicPipelineConfig.parse_args()
    extract_users(BasicPipelineConfig(), args)
