"""
deterministic_features.py
-------------------------
* 列名ゆれを ref_schema.column_alias_map で統一
* ファンド名+運用会社を正規化し det_key を生成
* Iceberg temp テーブルに保存
"""

from spark_utils import get_spark
from pyspark.sql import functions as F
import re, unicodedata

spark = get_spark()

# ── ① alias テーブルを Broadcast
rules = [(re.compile(r.regex, re.I), r.canonical_name)
         for r in spark.table("local_data_platform.ref.column_alias_map")
                       .orderBy("priority")
                       .collect()]
bc_rules = spark.sparkContext.broadcast(rules)

def rename_cols(df):
    cols = df.columns
    for pat, cannon in bc_rules.value:
        for c in cols:
            if pat.match(c) and cannon not in cols:
                df = df.withColumnRenamed(c, cannon); cols.append(cannon)
    return df

# ── ② 読み込み & リネーム
v2 = rename_cols(spark.table("local_data_platform.brz_ingestion.fund_master"))
legacy = rename_cols(spark.table("spark_catalog.legacy.legacy_fund_master"))
# v2 = v2.filter(F.col("ingest_date") == "2025-09-05")
# legacy = legacy.filter(F.col("ingest_date") == "2025-09-05")

# ── ③ 正規化 UDF
@F.udf("string")
def norm(s):
    if s is None: return ""
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", s).lower())

v2 = v2.withColumn("fund_name_norm",  norm("fund_name")) \
        .withColumn("company_norm",    norm("management_company")) \
        .withColumn("nickname_norm",    norm("fund_nickname")) \
       .withColumn("det_key", F.concat_ws("_", "fund_name_norm", "company_norm", "nickname_norm"))
# v2.persist()

legacy = legacy.withColumn("fund_name_norm",  norm("fund_name")) \
                .withColumn("company_norm",    norm("management_company")) \
                .withColumn("nickname_norm",    norm("fund_nickname")) \
               .withColumn("det_key", F.concat_ws("_", "fund_name_norm", "company_norm", "nickname_norm"))
# legacy.persist()

# >>> v2.show(truncate=False)
# +-------+-------------+------------+----------+-------------------+--------------------------+-----------------+-----------+-------------------+-----------------+------------------------+------------------------------------------------------------+
# |fund_id|投資信託_分類|trust_fee_rate|valid_flag|fund_name          |nickname                  |mgmt_company     |ingest_date|fund_name_norm     |company_norm     |nickname_norm           |det_key                                                     |
# +-------+-------------+------------+----------+-------------------+--------------------------+-----------------+-----------+-------------------+-----------------+------------------------+------------------------------------------------------------+
# |FND001 |ACTIVE       |1.20        |true      |グローバルファンドA|成長最強ファンド          |ファンド運用会社A|2025-07-21 |グローバルファンドa|ファンド運用会社a|成長最強ファンド        |グローバルファンドa_ファンド運用会社a_成長最強ファンド      |
# |FND002 |INDEX        |0.25        |true      |債券ファンドB      |夜も快眠ファンド          |ファンド運用会社B|2025-07-21 |債券ファンドb      |ファンド運用会社b|夜も快眠ファンド        |債券ファンドb_ファンド運用会社b_夜も快眠ファンド            |
# |FND003 |INDEX        |0.15        |true      |世界株式ファンドC  |全部カントリー（ゼンカン）|ファンド運用会社C|2025-07-21 |世界株式ファンドc  |ファンド運用会社c|全部カントリー(ゼンカン)|世界株式ファンドc_ファンド運用会社c_全部カントリー(ゼンカン)|
# +-------+-------------+------------+----------+-------------------+--------------------------+-----------------+-----------+-------------------+-----------------+------------------------+------------------------------------------------------------+

# >>> legacy.show(truncate=False)
# +-------+-------------+------------+----------+-------------------+---------------------+-----------------------+-----------+-----------+-------------------+---------------------+------------------------+--------------------------------------------------------------+
# |fund_id|投資信託_分類|trust_fee_rate|valid_flag|fund_name          |nickname             |mgmt_company           |hidden_cost|ingest_date|fund_name_norm     |company_norm         |nickname_norm           |det_key                                                       |
# +-------+-------------+------------+----------+-------------------+---------------------+-----------------------+-----------+-----------+-------------------+---------------------+------------------------+--------------------------------------------------------------+
# |NFD001 |ACTIVE       |1.20        |true      |グローバルファンドA|成長最強ファンド     |ファンド運用会社A      |3.15       |2025-07-21 |グローバルファンドa|ファンド運用会社a    |成長最強ファンド        |グローバルファンドa_ファンド運用会社a_成長最強ファンド        |
# |NFD002 |INDEX        |0.25        |true      |債券ファンドB      |夜も快眠ファンド     |ファンド運用会社B（株）|0.22       |2025-07-21 |債券ファンドb      |ファンド運用会社b(株)|夜も快眠ファンド        |債券ファンドb_ファンド運用会社b(株)_夜も快眠ファンド          |
# |NFD003 |INDEX        |0.15        |true      |世界の株式ファンドc|全部カントリー(ｾﾞﾝｶﾝ)|ファンド運用会社C      |0.09       |2025-07-21 |世界の株式ファンドc|ファンド運用会社c    |全部カントリー(ゼンカン)|世界の株式ファンドc_ファンド運用会社c_全部カントリー(ゼンカン)|
# +-------+-------------+------------+----------+-------------------+---------------------+-----------------------+-----------+-----------+-------------------+---------------------+------------------------+--------------------------------------------------------------+

# >>> 

# ── ④ 決定論ペアを算出
pairs = (
    v2.select(F.col("fund_id").alias("v2_id"), "det_key")
            .join(
                legacy.select(F.col("fund_id").alias("lg_id"), "det_key"),
                on="det_key",
                how="inner")
)

# +------------------------------------------------------+------+------+
# |det_key                                               |v2_id |lg_id |
# +------------------------------------------------------+------+------+
# |グローバルファンドa_ファンド運用会社a_成長最強ファンド|FND001|NFD001|
# +------------------------------------------------------+------+------+

# ── ⑤ legacyにv2のビジネスキーをマッピング
legacy_with_v2_key = (
    legacy.join(
        pairs.select("det_key", F.col("v2_id").alias("business_key")),
        on="det_key",
        how="left"  # 一致しないlegacyレコードも保持
    )
)

legacy_with_v2_key.select("fund_id", "business_key").show(truncate=False)

v2_with_v2_key = (
    v2.join(
        pairs.select("det_key", F.col("v2_id").alias("business_key")),
        on="det_key",
        how="left"  # 一致しないv2レコードも保持
    )
)

v2_with_v2_key.show(truncate=False)

# # ── ④ 保存
# v2.write.mode("overwrite").format("iceberg") \
#       .saveAsTable("lakehouse.temp.det_features_v2")
# legacy.write.mode("overwrite").format("iceberg") \
#       .saveAsTable("lakehouse.temp.det_features_legacy")
# print("✅ det_features_* テーブルを保存しました")
