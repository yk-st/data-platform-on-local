# detect_strategy_code_nopandas.py
from spark_utils import get_spark
from pyspark.sql import functions as F, types as T

TABLE_NAME  = "spark_catalog.legacy.legacy_fund_master"
STRAT_REGEX = r"^STRAT-[A-Z]{3}-\d{2}$"

spark = get_spark("detect-strategy-code")

df = spark.table(TABLE_NAME)

# 文字列系カラムを列挙
string_cols = [
    f.name for f in df.schema
    if isinstance(f.dataType, (T.StringType, T.VarcharType, T.CharType))
]
if not string_cols:
    raise RuntimeError("STRING 型の列が見つかりません")

# ── 1) 列ごとのヒット件数を Spark だけで集計 ─────────────────
hit_counts = df.select([
    F.sum(F.col(c).rlike(STRAT_REGEX).cast("int")).alias(c)
    for c in string_cols
])
print("=== 列ごとのヒット件数 ===")
hit_counts.show(truncate=False)

# ── 2) ヒット行を抽出（どの列でもマッチしたもの） ────────────────
condition = F.lit(False)
for c in string_cols:
    condition = condition | F.col(c).rlike(STRAT_REGEX)

matched = df.filter(condition)

# ── 3) フラグ列だけを作成して表示 ─────────────────
flag_cols = []
for c in string_cols:
    flag_col = f"match_in_{c}"
    matched = matched.withColumn(flag_col, F.col(c).rlike(STRAT_REGEX))
    flag_cols.append(flag_col)

# ここでフラグ列だけをセレクト
matched.select(*flag_cols).show(truncate=False)