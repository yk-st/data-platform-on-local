from spark_utils import get_spark
from jobs.fund.config import FundConfig
from pyspark.sql import functions as F, Window
import sys

class WideFromStarTransformer:
    def transform(self, fact, dim_fund, dim_date):
        """
        Gold (Star) から Silver (ワイド) へ変換。
        ・dim_fund は現在フラグのみ使用
        ・dim_date で日付メタを付与
        """

        wide = (
            fact.alias("f")
            # ── ファンド属性
            .join(
                dim_fund.alias("d"),
                (F.col("f.ファンドSK") == F.col("d.ファンドSK")) &
                (F.col("d.現在フラグ") == True)
            )
            # ── 日付属性（必要なら）
            .join(
                dim_date.alias("dt"),
                F.col("f.日付SK") == F.col("dt.日付SK")
            )
            # ── 整形
            .select(
                F.col("d.ファンドID"),
                F.col("f.基準日"),
                F.col("f.基準価額_円"),
                F.col("d.投資信託_分類"),
                F.col("d.信託報酬_率"),
                F.col("d.有効フラグ"),
                F.current_date().alias("取込日")
            )
            .orderBy("ファンドID", "基準日")
        )

        return wide


def transform_star_to_wide(config, args):
    spark = get_spark("silver-wide-from-star")

    # Gold ソース
    fact = spark.read.format("iceberg").table(
        config.get_dynamic_table_name(config.TABLE_FCT_FUND_PERFORMANCE, args)
    )
    dim_fund = spark.read.format("iceberg").table(
        config.get_dynamic_table_name(config.TABLE_DIM_FUND, args)
    )
    dim_date = spark.read.format("iceberg").table(
        config.get_dynamic_table_name(config.TABLE_DIM_DATE, args)
    )

    df = WideFromStarTransformer().transform(fact, dim_fund, dim_date)

    # Silver 再書き込み（overwritePartitions で差分更新も可）
    df.writeTo(
        config.get_dynamic_table_name(config.TABLE_FUND_DAILY_WIDE, args)
    ).overwritePartitions()

    spark.stop()


if __name__ == "__main__":
    args = FundConfig.parse_args()
    transform_star_to_wide(FundConfig(), args)