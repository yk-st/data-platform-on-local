#!/usr/bin/env python
# transform_star_schema_table.py
# ------------------------------------------------------------
# ・PySpark 3.5.5 / Iceberg 1.9
# ・PySpark には DataFrameWriterV2.merge() が無いので
#   → すべて SQL `MERGE INTO` で Upsert
# ・日本語カラム + サロゲートキー
# ------------------------------------------------------------
from pyspark.sql import functions as F, SparkSession
import uuid
import sys

from spark_utils import get_spark
from jobs.basic_pipeline.config import BasicPipelineConfig


# ────────────────────────────────────────────────
# 汎用 : DataFrame -> SQL MERGE INTO アップサート
# ────────────────────────────────────────────────
def upsert_via_merge(spark: SparkSession, df, target_table: str, key_cols: list[str]):
    """
    df          : アップサートしたいデータフレーム
    target_table: cat.db.table 形式
    key_cols    : 主キー（equality columns）となる列リスト
    """
    stage_view = f"tmp_stage_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(stage_view)

    on_expr = " AND ".join([f"t.`{c}` = s.`{c}`" for c in key_cols])

    spark.sql(f"""
        MERGE INTO {target_table} AS t
        USING {stage_view}        AS s
        ON {on_expr}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.catalog.dropTempView(stage_view)  # お掃除


# ────────────────────────────────────────────────
# Star 変換クラス
# ────────────────────────────────────────────────
class WideTableTransformer:
    def transform(self, spark: SparkSession, config, args, orders, products, users):

        # ────────── DIM_CUSTOMER ──────────
        dim_cust = (
            users
            .withColumn(
                "customer_sk",
                F.xxhash64(F.col("user_id").cast("string")).cast("bigint")
            )
            .withColumn("ingest_date", F.current_date())
        )
        upsert_via_merge(
            spark,
            dim_cust,
            config.get_dynamic_table_name(config.TABLE_DIM_CUSTOMER, args),
            key_cols=["customer_sk"]
        )

        # ────────── DIM_PRODUCT ──────────
        dim_prod = (
            products
            .withColumn(
                "product_sk",
                F.xxhash64(F.col("product_id").cast("string")).cast("bigint")
            )
            .withColumn("ingest_date", F.current_date())
        )
        upsert_via_merge(
            spark,
            dim_prod,
            config.get_dynamic_table_name(config.TABLE_DIM_PRODUCT, args),
            key_cols=["product_sk"]
        )

        # ────────── DIM_DATE ──────────
        dim_date = (
            orders
            .select(F.to_date("created_at").alias("date_sk"))
            .distinct()
            .withColumn("year",        F.year("date_sk"))
            .withColumn("month",        F.month("date_sk"))
            .withColumn("day",        F.dayofmonth("date_sk"))
            .withColumn("yyyymm",    F.expr("`year`*100 + `month`"))
            .withColumn("day_of_week",      F.dayofweek("date_sk"))
            .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
        )
        upsert_via_merge(
            spark,
            dim_date,
            config.get_dynamic_table_name(config.TABLE_DIM_DATE, args),
            key_cols=["date_sk"]
        )

        # ────────── FACT_ORDERS ──────────
        fact = (
            orders
            .join(dim_cust.select("customer_sk", "user_id"), on="user_id")
            .join(dim_prod.select("product_sk", "product_id"),      on="product_id")
            .withColumn("date_sk",      F.to_date("created_at"))
            .withColumn("ingest_date",  F.current_date())
            .withColumn("load_ts",      F.current_timestamp())
            .select(
                "order_id", "customer_sk", "product_sk", "date_sk",
                "quantity", "subtotal_usd", "tax_usd", "total_usd",
                "status_flag", "parent_id", "ingest_date", "load_ts"
            )
        )
        return fact


# ────────────────────────────────────────────────
# DAG で使う入口関数
# ────────────────────────────────────────────────
def transform_star_tables(config, args):
    spark = get_spark(app_name="star_schema_transform")

    # Bronze / Silver から読み込み
    orders = spark.read.format("iceberg").table(
        config.get_dynamic_table_name(config.TABLE_ORDERS, args)
    ).filter(
        F.col("ingest_date") == args.ingest_date  # 最新のデータのみ
    )
    products = spark.read.format("iceberg").table(
        config.get_dynamic_table_name(config.TABLE_PRODUCTS, args)
    ).filter(
        F.col("ingest_date") == args.ingest_date  # 最新のデータのみ
    )
    users = spark.read.format("iceberg").table(
        config.get_dynamic_table_name(config.TABLE_USERS, args)
    ).filter(
        F.col("ingest_date") == args.ingest_date  # 最新のデータのみ
    )

    # 変換
    fact_df = WideTableTransformer().transform(spark, config, args, orders, products, users)

    # FACT は追記 (Type 1)
    fact_df.writeTo(config.get_dynamic_table_name(config.TABLE_FACT_ORDERS, args)) \
          .using("iceberg") \
          .option("merge-schema", "true") \
          .append()

    spark.stop()


# ────────────────────────────────────────────────
# main
# ────────────────────────────────────────────────
if __name__ == "__main__":
    args = BasicPipelineConfig.parse_args()
    transform_star_tables(BasicPipelineConfig(), args)
