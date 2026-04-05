# validate_orders_dq_all.py
# ---------------------------------------------------------------------------
# pip install cuallee==0.15.*
# spark_utils.get_spark() で Iceberg 設定済み SparkSession を取得
# ---------------------------------------------------------------------------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from spark_utils import get_spark
from cuallee import Check, CheckLevel

# ──────────────────────────────────────────────────────────────
# 0) SparkSession & セッション UTC 強制
# ──────────────────────────────────────────────────────────────
spark = get_spark("dq-orders-cuallee")
session_tz = spark.conf.get("spark.sql.session.timeZone")
if session_tz.upper() != "ETC/UTC":
    raise SystemExit(f"❌ Spark session timeZone = {session_tz} (expected UTC)")

# Iceberg テーブル
ORDERS  = "local_data_platform.brz_ingestion.orders"
USERS  = "local_data_platform.brz_ingestion.users"
PRODUCT = "local_data_platform.brz_ingestion.products"

orders_df  = spark.read.format("iceberg").load(ORDERS)
users_df  = spark.read.format("iceberg").load(USERS)
product_df = spark.read.format("iceberg").load(PRODUCT)


# ──────────────────────────────────────────────────────────────
# 1) Cuallee 0.15 ですべてのルールを定義（is_custom も活用）
# ──────────────────────────────────────────────────────────────
check = (
    Check(CheckLevel.ERROR, "orders_dq")
    # ── 欠損 ───────────────────────────
      .is_complete("order_id").is_complete("user_id").is_complete("product_id")
      .is_complete("subtotal_usd").is_complete("tax_usd").is_complete("total_usd")
      .is_complete("quantity").is_complete("created_at")
    # ── 一意 ───────────────────────────
      .is_unique("order_id")
    # ── 非負 ───────────────────────────
    #   .is_greater_or_equal_than("subtotal_usd", 0)
    #   .is_greater_or_equal_than("tax_usd", 0)
    #   .is_greater_or_equal_than("total_usd", 0)
    #   .is_greater_or_equal_than("quantity", 1)
    # ── 作成日時が UTC で想定期間 ────────
      .is_between(
          "created_at",
          ("2000-01-01 00:00:00", "2100-01-01 00:00:00"),  # tuple(lower, upper)
          pct=1.0
      )
)

# 1-A) Subtotal + Tax ≒ Total
# def subtotal_match(df):
#     return orders_df.select(
#         "order_id",
#         "subtotal_usd", "tax_usd", "total_usd",
#         (F.abs(F.col("subtotal_usd") + F.col("tax_usd") - F.col("total_usd")) == 0)
#           .alias("subtotal_ok")
#     ).filter(F.col("order_id").isin(["98", "99", "102", "103", "104"])).show(n=150, truncate=False)

def subtotal_match(df):
    return df.select(
        "order_id",
        (F.abs(F.col("subtotal_usd") + F.col("tax_usd") - F.col("total_usd")) == 0)
          .alias("subtotal_ok")
    )


check.is_custom(
    column=["subtotal_usd", "tax_usd", "total_usd"],
    fn=subtotal_match,
    pct=1.0,
    options={"name": "subtotal_tax_total"}
)

# # 1-B) UTC パーティション整合性
# def utc_partition_match(df):
#     return df.select(
#         "注文ID",
#         (F.to_date("作成日時") == F.col("ingest_date")).alias("utc_partition_ok")
#     )

# check.is_custom(
#     column=["作成日時", "ingest_date"],
#     fn=utc_partition_match,
#     pct=1.0,
#     options={"name": "utc_partition_match"}
# )

# 1-C) 先頭ゼロ数量
def leading_zero_qty(df):
    return df.select(
        "order_id",
        (~F.col("quantity").cast("string").rlike(r"^0\d+$")).alias("qty_ok")
    )

check.is_custom(
    column=["quantity"],
    fn=leading_zero_qty,
    pct=1.0,
    options={"name": "leading_zero_quantity"}
)

# 1-D) 文字化け・半角カナ
def mojibake_check(df):
    regex = r"(�|[\uFF61-\uFF9F])"
    return df.select(
        "order_id",
        (~F.col("status_flag").rlike(regex)).alias("flag_clean")
    )

check.is_custom(
    column=["status_flag"],
    fn=mojibake_check,
    pct=1.0,
    options={"name": "mojibake_fullwidth"}
)

# 1-E) 参照整合性（ユーザー）
def fk_users(df, ref=users_df):
    return df.join(
        ref.selectExpr("`user_id` as ref_id"), df["user_id"] == F.col("ref_id"), "left"
    ).select("order_id", F.col("ref_id").isNotNull().alias("fk_ok"))

check.is_custom(
    column=["user_id"],
    fn=fk_users,
    pct=1.0,
    options={"name": "fk_orders_users"}
)

# 1-F) 参照整合性（商品）
def fk_product(df, ref=product_df):
    return df.join(
        ref.selectExpr("`product_id` as ref_id"), df["product_id"] == F.col("ref_id"), "left"
    ).select("order_id", F.col("ref_id").isNotNull().alias("fk_ok"))

check.is_custom(
    column=["product_id"],
    fn=fk_product,
    pct=0.9,
    options={"name": "fk_orders_product"}
)

# 1-G) 合計_円 外れ値 (IQR±1.5)
q1, q3 = orders_df.approxQuantile("total_usd", [0.25, 0.75], 0.05)
low, high = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)

def outlier_total(df, lo=low, hi=high):
    return df.select(
        "order_id",
        ((F.col("total_usd") >= lo) & (F.col("total_usd") <= hi)).alias("total_iqr_ok")
    )

check.is_custom(
    column=["total_usd"],
    fn=outlier_total,
    pct=1.0,
    options={"name": "outlier_total_yen"}
)

# ──────────────────────────────────────────────────────────────
# 2) 検証 & レポート
# ──────────────────────────────────────────────────────────────
result_df = check.validate(orders_df)

# result_df スキーマ:
# |id|timestamp|check|level|column|rule|rows|violations|pass_rate|status|

report_df = (
    result_df
      .select(
          F.concat_ws("::", "check", "rule").alias("check_name"),
          "column",
          "status",
          "violations",
          F.round("pass_rate", 4).alias("pass_rate")
      )
      .orderBy("check_name")
)

report_df.show(truncate=False)

# ──────────────────────────────────────────────────────────────
# 3) フェイル fast & 保存
# ──────────────────────────────────────────────────────────────
# if report_df.filter(F.col("status") == "FAIL").count() > 0:
#     report_df.writeTo("local_data_platform.qa.orders_dq_report").overwritePartitions()
#     raise SystemExit("❌ Data-quality checks failed – see qa.orders_dq_report")

# print("✅ All data-quality checks passed.")


# +--------+------------+-------+---------+-----------+
# |order_id|subtotal_usd|tax_usd|total_usd|subtotal_ok|
# +--------+------------+-------+---------+-----------+
# |21      |136         |14     |149      |false      |
# |42      |492         |49     |541      |true       |
# |44      |74          |7      |81       |true       |
# |49      |46          |5      |51       |true       |
# |64      |324         |32     |357      |false      |
# |80      |299         |30     |329      |true       |
# |86      |51          |5      |57       |false      |
# |93      |148         |15     |162      |false      |
# |96      |139         |14     |153      |true       |
# |3       |186         |19     |204      |false      |
# |9       |216         |22     |237      |false      |
# |17      |25          |2      |27       |true       |
# |34      |14          |1      |15       |true       |
# |62      |158         |16     |173      |false      |
# |70      |138         |14     |152      |true       |
# |71      |89          |9      |98       |true       |
# |75      |369         |37     |405      |false      |
# |79      |410         |41     |451      |true       |
# |87      |56          |6      |62       |true       |
# |91      |100         |10     |110      |true       |
# |94      |97          |10     |106      |false      |
# |103     |NULL        |19     |204      |NULL       |
# |5       |83          |8      |91       |true       |
# |7       |45          |5      |50       |true       |
# |27      |32          |3      |35       |true       |
# |43      |111         |11     |122      |true       |
# |56      |115         |12     |127      |true       |
# |67      |283         |28     |311      |true       |
# |108     |159         |16     |175      |true       |
# |22      |271         |27     |298      |true       |
# |26      |441         |44     |485      |true       |
# |50      |75          |7      |82       |true       |
# |2       |66          |7      |73       |true       |
# |4       |46          |5      |51       |true       |
# |6       |282         |28     |310      |true       |
# |11      |14          |1      |15       |true       |
# |14      |70          |7      |77       |true       |
# |15      |194         |19     |214      |false      |
# |20      |52          |5      |58       |false      |
# |23      |157         |16     |172      |false      |
# |25      |205         |21     |226      |true       |
# |29      |54          |5      |59       |true       |
# |30      |89          |9      |97       |false      |
# |33      |53          |5      |58       |true       |
# |40      |62          |6      |68       |true       |
# |41      |391         |39     |430      |true       |
# |46      |263         |26     |289      |true       |
# |47      |68          |7      |75       |true       |
# |55      |169         |17     |186      |true       |
# |66      |48          |5      |52       |false      |
# |90      |132         |13     |145      |true       |
# |102     |66          |7      |7256     |false      |
# |104     |46          |NULL   |51       |NULL       |
# |35      |97          |10     |107      |true       |
# |37      |300         |30     |330      |true       |
# |69      |135         |14     |149      |true       |
# |73      |249         |25     |274      |true       |
# |78      |141         |14     |156      |false      |
# |45      |68          |7      |75       |true       |
# |88      |77          |8      |85       |true       |
# |99      |1689        |169    |1857     |false      |
# |10      |71          |7      |78       |true       |
# |13      |209         |21     |230      |true       |
# |16      |43          |4      |48       |false      |
# |39      |68          |7      |74       |false      |
# |53      |237         |24     |261      |true       |
# |60      |21          |2      |24       |false      |
# |65      |59          |6      |65       |true       |
# |68      |58          |6      |64       |true       |
# |77      |23          |2      |25       |true       |
# |82      |148         |15     |162      |false      |
# |83      |52          |5      |58       |false      |
# |85      |299         |30     |329      |true       |
# |92      |159         |16     |175      |true       |
# |8       |295         |29     |324      |true       |
# |48      |95          |10     |105      |true       |
# |58      |68          |7      |75       |true       |
# |31      |342         |34     |376      |true       |
# |38      |77          |8      |84       |false      |
# |76      |16          |2      |17       |false      |
# |84      |72          |7      |80       |false      |
# |97      |27          |3      |29       |false      |
# |1       |11          |1      |12       |true       |
# |18      |391         |39     |430      |true       |
# |19      |213         |21     |234      |true       |
# |24      |96          |10     |105      |false      |
# |28      |369         |37     |406      |true       |
# |32      |316         |32     |348      |true       |
# |36      |87          |9      |95       |false      |
# |51      |79          |8      |87       |true       |
# |52      |33          |3      |36       |true       |
# |54      |53          |5      |58       |true       |
# |57      |50          |5      |55       |true       |
# |72      |39          |4      |43       |true       |
# |74      |225         |23     |248      |true       |
# |89      |371         |37     |409      |false      |
# |95      |56          |6      |62       |true       |
# |98      |1142        |114    |1257     |false      |
# |100     |307         |31     |337      |false      |
# |106     |11          |1      |12       |true       |
# |107     |-11         |-1     |-12      |true       |
# |12      |43          |4      |48       |false      |
# |59      |70          |7      |77       |true       |
# |61      |237         |24     |261      |true       |
# |63      |165         |17     |181      |false      |
# |81      |228         |23     |251      |true       |
# +--------+------------+-------+---------+-----------+