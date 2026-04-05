from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from jobs.base_extractor import BaseExtractor
from jobs.init.config import IntConfig
import sys

class ZipgeocodeExtractor(BaseExtractor):
    def extract(self, spark):
        # 明示的なスキーマ定義（先頭ゼロ保持のためStringType使用）
        schema = StructType([
            StructField("zip_code", StringType(), True),
            StructField("address", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ])
        
        df = (
            spark.read
                .option("header", "true")
                .schema(schema)
                .option("encoding", "UTF-8")
                .csv(self.config.ZIP_GEOCODE_S3_SOURCE)
        )
        return df

    def target_table(self) -> str: 
        return self.config.get_dynamic_table_name(self.config.TABLE_ZIP_GEOCODE, self.args)

def extract_zipcode(config, args):
    ZipgeocodeExtractor(config, args).run_overwrite()

if __name__ == "__main__":
    args=IntConfig.parse_args()
    extract_zipcode(IntConfig(), args)
