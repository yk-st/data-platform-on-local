from pyspark.sql import functions as F

# ────────────────────────────────
# 0) 共通設定
NS = "local_data_platform.slv_analytics"

# ▼ 52 の注文をワイド表から取得
wide_52 = (
    spark.read.table(f"{NS}.user_orders_wide")
         .filter(F.col("ユーザーID") == 52)
         .orderBy("注文ID")
)

# ▼ スター：Fact + Dim で再構築
fact   = spark.read.table(f"{NS}.fact_orders").alias("f")
dim_c  = spark.read.table(f"{NS}.dim_customer").alias("c")
dim_p  = spark.read.table(f"{NS}.dim_product").alias("p")
dim_d  = spark.read.table(f"{NS}.dim_date").alias("d")

star_52 = (
    fact.filter(F.col("f.customer_sk").isin(
                    dim_c.filter(F.col("ユーザーID") == 52)
                         .select("customer_sk")
                         .distinct()
                         .rdd.flatMap(lambda x: x).collect()
                ))
        .join(dim_c,  "customer_sk")
        .join(dim_p,  "product_sk")
        .join(dim_d,  fact.date_sk == dim_d.date_sk)
        # .select(   # wide 表と同じカラム順に並べ替え
        #     "注文ID", "ユーザーID", "ユーザー名", "メールアドレス","パスワード",
        #     "住所", "チャネル_取得元", "生年月日", "郵便番号",
        #     "商品ID", "商品_タイトル", "カテゴリ", "ベンダー名",
        #     "価格_円", "評価_RATING",
        #     "数量_個", "小計_円", "税額_円", "合計_円",
        #     "フラグ", "親ID", F.col("f.date_sk").alias("作成日")
        # ).orderBy("注文ID")
)

# ────────────────────────────────
# 1) 目視確認
wide_52.show(truncate=False)
star_52.show(truncate=False)

# # ────────────────────────────────
# # 2) 厳密比較（差集合がゼロなら一致）
# diff1 = wide_52.subtract(star_52)
# diff2 = star_52.subtract(wide_52)

# if diff1.count() == 0 and diff2.count() == 0:
#     print("✅ ワイドとスターの内容は完全に一致（ユーザーID=52）")
# else:
#     print("❌ 差分あり：")
#     diff1.show(); diff2.show()
