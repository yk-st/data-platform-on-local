from pyspark.sql import functions as F, types as T
from jobs.base_extractor import BaseExtractor
from jobs.init.config import IntConfig
import sys

class ColumnAliasMapExtractor(BaseExtractor):
    def extract(self, spark):
        records = [
            ("^(愛称|ニックネーム|nickname)$",                 "fund_nickname",      1),
            ("^(運用会社|マネジメント会社|mgmt_?company)$",    "management_company",  1),
            ("^(ファンド名|fund(_)?name)$",                    "fund_name",     1),
            ("^(信託報酬_?率?|management[_ ]?fee|fee_rate)$",  "trust_fee_rate",  1),
            ("^(隠れコスト(率)?|hidden[_ ]?cost)$",            "hidden_cost",   1),
            ("^(有効レコード|is[_ ]?active|valid[_ ]?flag)$",  "valid_flag",    1),
        ]

        schema = (
            T.StructType()
            .add("regex",          T.StringType())
            .add("canonical_name", T.StringType())
            .add("priority",       T.IntegerType())
        )

        return spark.createDataFrame(records, schema).withColumn("updated_at", F.current_timestamp())

    def target_table(self) -> str: 
        return self.config.get_dynamic_table_name(self.config.TABLE_COLUMN_ALIAS_MAP, self.args)

def extract_column_alias_map(config, args):
    ColumnAliasMapExtractor(config, args).run_overwrite()

if __name__ == "__main__":
    args=IntConfig.parse_args()
    extract_column_alias_map(IntConfig(), args)
