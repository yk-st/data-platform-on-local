#!/usr/bin/env python
# tmpview_cdc_dedup.py
# ------------------------------------------------------------
# 旧スナップショット → 初期 CDC (version=1, is_current=true)
# 新スナップショット → 差分抽出 (I / U / D)
#   - 旧行は is_current=false へ *上書き*（重複しない）
#   - 新行は version = max+1, is_current=true
# すべて Spark の一時ビューだけで完結（Iceberg 等は未使用）
# Spark 3.5.5
# ------------------------------------------------------------
from pyspark.sql import functions as F, Window
from spark_utils import get_spark   # あなたのユーティリティ

spark = get_spark("tmpview_cdc_dedup")

# ───────────────────────────────────────
# 0) サンプル旧・新スナップショット
# ───────────────────────────────────────
schema = ["ユーザーID", "ユーザー名", "住所", "チャネル_取得元"]

# partition Aのデータと仮定
data_old = [
    (1, "佐藤 太郎", "東京都世田谷区…", "広告"),
    (2, "山田 花子", "大阪府大阪市…",   "自然検索"),
    (3, "John Doe",  "Kanagawa…",      "友人紹介"),
    (5, "Saito Doe", "Kanagawa…",      "友人紹介"),
]

# partition Bのデータと仮定
data_new = [
    (1, "佐藤 太郎", "東京都世田谷区…", "広告"),          # 変更なし
    (2, "山田 花子", "大阪府豊中市…",   "自然検索"),      # 住所変更
    (3, "John Doe",  "Kanagawa…",      "SNS"),           # チャネル変更
    (4, "高橋 健",   "千葉県千葉市…",   "広告"),          # 新規
]


old_df = spark.createDataFrame(data_old, schema)
new_df = spark.createDataFrame(data_new, schema)

NON_KEYS   = ["user_id", "address", "acquisition_channel"]
FINAL_COLS = schema + ["op_type", "cdc_ts", "version", "is_current"]

# ───────────────────────────────────────
# 1) 初期 CDC ビュー (旧スナップショット)
# ───────────────────────────────────────
cdc_prev = (
    old_df
    .withColumn("op_type",    F.lit("I"))
    .withColumn("cdc_ts",     F.current_timestamp())
    .withColumn("version",    F.lit(1))
    .withColumn("is_current", F.lit(True))
).select(*FINAL_COLS)

# ───────────────────────────────────────
# 2) 新旧スナップショット差分 (I / U / D)
# ───────────────────────────────────────
def with_hash(df):
    return df.withColumn("row_hash", F.xxhash64(*NON_KEYS))

old_h, new_h = map(with_hash, [old_df, new_df])

# 2-A) Insert / Update (after 行のみ)
# new_h(data_new)/old_h(data_old)でレコードのハッシュを算出したもの。
# 新しい方にデータがなければ(isNull)Iで、ユーザIDが一致していて、ハッシュが一致しなければ更新
delta_upd = (
    new_h.alias("n")
    .join(old_h.alias("o"), "user_id", "left")
    .where(
        (F.col("o.row_hash").isNull()) |
        (F.col("n.row_hash") != F.col("o.row_hash"))
    )
    .select(
        *[F.col(f"n.`{c}`") for c in schema],
        F.when(F.col("o.row_hash").isNull(), "I").otherwise("U").alias("op_type")
    )
)

# 2-B) Delete
# left_anti(左側のテーブル（DataFrame）のうち、右側のテーブルにマッチする行がないもの。いわゆる差集合)
# 古いデータにしかないものがあればそれは削除（Saito Doe）
delta_del = (
    old_h.alias("o")
    .join(new_h.alias("n"), "user_id", "left_anti")
    .select(
        F.col("o.`user_id`"),
        *[F.lit(None).cast("string").alias(c) for c in NON_KEYS],
        F.lit("D").alias("op_type")
    )
)

# 2-C) 結合 + タイムスタンプ
cdc_delta = (
    delta_upd.drop("row_hash")          # hash 列除去
    .unionByName(delta_del)
    .withColumn("cdc_ts", F.current_timestamp())
).select(*schema, "op_type", "cdc_ts")  # version / is_current は後付け

# ───────────────────────────────────────
# 3) 旧行を is_current=false に上書き
# ───────────────────────────────────────
# collect() を避けて、JOIN ベースで効率的に処理
affected_ids_df = cdc_delta.select("user_id").distinct()

cdc_prev_updated = (
    cdc_prev.alias("prev")
    .join(affected_ids_df.alias("affected"), "user_id", "left")
    .withColumn(
        "is_current",
        F.when(F.col("affected.user_id").isNotNull(), F.lit(False))
         .otherwise(F.col("prev.is_current"))
    )
    .select("prev.*", F.col("is_current"))
)

# ───────────────────────────────────────
# 4) 新行に version 付与 & is_current=true
# ───────────────────────────────────────
# 効率的な version 計算: Window関数を使用してShuffle回数を最小化
max_ver_df = cdc_prev.groupBy("user_id").agg(F.max("version").alias("max_ver"))

delta_next = (
    cdc_delta
    .join(max_ver_df, "user_id", "left")
    .withColumn("version", F.coalesce("max_ver", F.lit(0)) + 1)
    .withColumn("is_current", F.lit(True))
    .select(*FINAL_COLS)
)

# 【最適化メモ】大規模データ向けの代替アプローチ:
# Window関数を使ってJOINを減らす方法も可能:
# window_spec = Window.partitionBy("ユーザーID").orderBy("cdc_ts")
# delta_next_alt = (
#     cdc_delta
#     .withColumn("prev_max_ver", F.lag(F.col("version"), 1, 0).over(window_spec))
#     .withColumn("version", F.col("prev_max_ver") + 1)
#     .withColumn("is_current", F.lit(True))
# )

# ───────────────────────────────────────
# 5) 連結して最終 CDC ビュー (重複なし)
# ───────────────────────────────────────
v_cdc_people = (
    cdc_prev_updated.select(*FINAL_COLS)
    .unionByName(delta_next)
)

v_cdc_people.createOrReplaceTempView("v_cdc_people")

# ───────────────────────────────────────
# 6) 確認
# ───────────────────────────────────────
spark.sql("""
SELECT `user_id`, version, op_type,
       `user_name`, `address`, `acquisition_channel`, is_current
FROM v_cdc_people
ORDER BY `user_id`, version
""").show(truncate=False)

# ---- 後段処理では spark.table("v_cdc_people") で参照 ----
spark.stop()


# +----------+-------+-------+----------+---------------+---------------+----------+
# |ユーザーID|version|op_type|ユーザー名|住所           |チャネル_取得元|is_current|
# +----------+-------+-------+----------+---------------+---------------+----------+
# |1         |1      |I      |佐藤 太郎 |東京都世田谷区…|広告           |true      |
# |2         |1      |I      |山田 花子 |大阪府大阪市…  |自然検索       |false     |
# |2         |2      |U      |山田 花子 |大阪府豊中市…  |自然検索       |true      |
# |3         |1      |I      |John Doe  |Kanagawa…      |友人紹介       |false     |
# |3         |2      |U      |John Doe  |Kanagawa…      |SNS            |true      |
# |4         |1      |I      |高橋 健   |千葉県千葉市…  |広告           |true      |
# |5         |1      |I      |Saito Doe |Kanagawa…      |友人紹介       |false     |
# |5         |2      |D      |NULL      |NULL           |NULL           |true      |
# +----------+-------+-------+----------+---------------+---------------+----------+


# +----------+-------+-------+----------+---------------+---------------+----------+
# |ユーザーID|version|op_type|ユーザー名|住所           |チャネル_取得元|is_current|
# +----------+-------+-------+----------+---------------+---------------+----------+
# |1         |1      |I      |佐藤 太郎 |東京都世田谷区…|広告           |true      |
# |2         |1      |I      |山田 花子 |大阪府大阪市…  |自然検索       |false     |
# |2         |2      |U      |山田 花子 |大阪府豊中市…  |自然検索       |true      |
# |3         |1      |I      |John Doe  |Kanagawa…      |友人紹介       |false     |
# |3         |2      |U      |John Doe  |Kanagawa…      |SNS            |true      |
# |4         |1      |I      |高橋 健   |千葉県千葉市…  |広告           |true      |
# |5         |1      |I      |Saito Doe |Kanagawa…      |友人紹介       |false     |
# |5         |2      |D      |NULL      |NULL           |NULL           |true      |
# +----------+-------+-------+----------+---------------+---------------+----------+