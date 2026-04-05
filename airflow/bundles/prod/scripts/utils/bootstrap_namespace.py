# bootstrap_namespace.py  ── 完全版
"""
Iceberg 名前空間を CREATE IF NOT EXISTS でブートストラップするユーティリティ。

使い方 その1：従来型 (複数回 --ns)
spark-submit … bootstrap_namespace.py \
  --catalog iceberg_silver_prod \
  --ns slv_analytics=s3a://bucket/warehouse/silver/analytics.db \
  --ns gld_presentation=s3a://local.data.platform/warehouse/gld/gld_presentation.db

使い方 その2：一括 (--namespaces / -N)
spark-submit … bootstrap_namespace.py \
  --catalog iceberg_silver_prod \
  --namespaces '
      slv_analytics=s3a://bucket/warehouse/silver/analytics.db,
      gld_presentation=s3a://local.data.platform/warehouse/gld/gld_presentation.db
  '
"""

import re, sys, argparse
from spark_utils import get_spark
from pyspark.sql import SparkSession
from base_config import BaseConfig

# ---------- 実処理 ----------
def create_namespaces(spark: SparkSession, catalog: str, items: list[tuple[str,str]]):
    for ns, loc in items:
        ns_fqn = f"{catalog}.{ns}" if catalog else ns
        spark.sql(f"""
            CREATE NAMESPACE IF NOT EXISTS {ns_fqn}
            LOCATION '{loc}'
        """)
        print(f"✔ created namespace {ns_fqn} -> {loc}")

# ---------- entry ----------
if __name__ == "__main__":
    args = BaseConfig.parse_args()
    spark = get_spark()

    pairs: list[tuple[str,str]] = []

    # ① --namespaces を優先して展開
    if args.namespaces:
        for token in re.split(r"[,\s]+", args.namespaces.strip()):
            if token:
                try:
                    ns, loc = token.split("=", 1)
                    pairs.append((ns, loc))
                except ValueError:
                    sys.exit(f"ERROR: --namespaces の形式が不正: {token}")

    # ② 旧 --ns も取り込む
    if args.ns:
        for token in args.ns:
            try:
                ns, loc = token.split("=", 1)
                pairs.append((ns, loc))
            except ValueError:
                sys.exit(f"ERROR: --ns の形式が不正: {token}")

    if not pairs:
        sys.exit("ERROR: 名前空間が 1 つも指定されていません。")

    create_namespaces(spark, catalog=args.catalog, items=pairs)
