from pyspark.sql import functions as F
from jobs.base_extractor import BaseExtractor
from jobs.basic_pipeline.config import BasicPipelineConfig
from utils.column_sanitize import sanitize_column_name
import sys

class ProductExtractor(BaseExtractor):
    def extract(self, spark):

        ingest_date = self.args.ingest_date
    
        src_df = (spark.read.format("jdbc")
                 .option("url", self.config.POSTGRES_URL)
                 .option("dbtable", "public.products")
                 .option("user", self.config.POSTGRES_USER)
                 .option("password", self.config.POSTGRES_PASSWORD)
                 .load())
        extra_jitter_map = {
            "id": "product_id",
        }
        cols = [F.col(c).alias(sanitize_column_name(c, extra_jitter_map)) for c in src_df.columns]
        print(f"Extracted columns: {cols}")

        src_df = src_df.withColumn("created_at", F.to_utc_timestamp(F.col("created_at"), "UTC")) \
                .withColumn("updated_at", F.to_utc_timestamp(F.col("updated_at"), "UTC"))
        src_df = src_df.withColumn("ingest_date", F.lit(ingest_date).cast("timestamp"))

        return src_df.select(*cols, "ingest_date")

    def target_table(self) -> str: 
        return self.config.get_dynamic_table_name(self.config.TABLE_PRODUCTS, self.args)

def extract_products(config, args):
    ProductExtractor(config, args).run()

if __name__ == "__main__":
    args=BasicPipelineConfig.parse_args()
    extract_products(BasicPipelineConfig(), args)
