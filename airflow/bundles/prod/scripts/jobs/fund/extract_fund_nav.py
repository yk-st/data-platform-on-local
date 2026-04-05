from pyspark.sql import functions as F
from jobs.base_extractor import BaseExtractor
from jobs.fund.config import FundConfig
import sys
from utils.column_sanitize import sanitize_column_name
from pyspark.sql.functions import current_date

class FundNavExtractor(BaseExtractor):
    def extract(self, spark):
        df = (
            spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(f"{self.config.FUND_NAV_S3_SOURCE}_{self.args.master_data}.csv")
        )
        # df = df.withColumn("ingest_date", current_date())
        df = df.withColumn(
            "base_date",
            F.to_date(F.col("base_date"), "yyyy-MM-dd")
        )
        return df

    def target_table(self) -> str: 
        return self.config.get_dynamic_table_name(self.config.TABLE_FUND_NAV, self.args)

def extract_fund_nav(config, args):
    FundNavExtractor(config, args).run("base_date")

if __name__ == "__main__":
    args = FundConfig.parse_args()
    extract_fund_nav(FundConfig(), args)
