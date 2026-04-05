from pyspark.sql import functions as F
from jobs.base_extractor import BaseExtractor
from jobs.fund.config import FundConfig
import sys
from utils.column_sanitize import sanitize_column_name

class FundMasterExtractor(BaseExtractor):
    def extract(self, spark):
        df = (
            spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(f"{self.config.FUND_MASTER_S3_SOURCE}_{self.args.master_data}.csv")
        )
        ingest_date = self.args.ingest_date
        df = df.withColumn("ingest_date", F.lit(ingest_date).cast("timestamp"))
        return df

    def target_table(self) -> str: 
        return self.config.get_dynamic_table_name(self.config.TABLE_FUND_MASTER, self.args)

def extract_fund_master(config, args):
    FundMasterExtractor(config, args).run()

if __name__ == "__main__":
    args = FundConfig.parse_args()
    extract_fund_master(FundConfig(), args)
