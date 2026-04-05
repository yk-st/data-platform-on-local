from pyspark.sql import functions as F
from jobs.base_extractor import BaseExtractor
from jobs.init.config import IntConfig
import sys

class AgeGroupExtractor(BaseExtractor):
    def extract(self, spark):
        age_df = (
            spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(self.config.AGE_GROUP_REF_S3_SOURCE)
        )
        return age_df

    def target_table(self) -> str: 
        return self.config.get_dynamic_table_name(self.config.TABLE_AGE_GROUP_REF, self.args)

def extract_orders(config, args):
    AgeGroupExtractor(config, args).run_overwrite()

if __name__ == "__main__":
    args=IntConfig.parse_args()
    extract_orders(IntConfig(), args)
