# anonymization_from_wide.py
# Spark 3.5.5 (Scala 2.13) / Python 3.12 を想定
# 目的: ワイドテーブルから月次×都道府県×年齢帯×カテゴリの匿名集計を行い
#       コンソールに出力するだけ（外部ファイル出力なし）

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

###############################################################################
# 0. パラメータ
###############################################################################

# 誕生日が実行日にずれないようにするため日付固定)
# 本書の内容再現のため
FIXED_DATE = F.lit("2025-09-06").cast("date")  # 年齢計算基準日 (F.current_date() でも可)
# こちらをコメントインすれば本来の動作
# FIXED_DATE = F.current_date()

TABLE_NAME   = "local_data_platform.slv_entities.user_orders_wide"
DEDUPE_USER  = True    # True: 同一 user_token の月次購入を 1 件に丸める
ADD_NOISE    = False   # True: total_sales に ±3 % ノイズ注入
K_THRESHOLD  = 2      # セル抑制閾値
HASH_LEN     = 16      # user_token の先頭文字数


# 本書で紹介しているオプション
AGE_WIDTH    = 5       # 年齢帯の幅（歳）: 5, 10, 15, 20など
USE_REGION   = False    # True: 都道府県を地方別にグループ化
MAX_ITER     = 0       # Other統合の最大繰り返し回数
ADD_DUMMY    = False   # True: k未満のグループにダミーデータを注入
FAIL_ON_AGE_K_VIOLATION = False   # True: 未達なら raise、False: K-匿名化が未達でも続行

# K=2が達成可能なオプション
# AGE_WIDTH    = 5       # 年齢帯の幅（歳）: 5, 10, 15, 20など
# USE_REGION   = False    # True: 都道府県を地方別にグループ化
# MAX_ITER     = 3       # Other統合の最大繰り返し回数
# ADD_DUMMY    = True   # True: k未満のグループにダミーデータを注入
#FAIL_ON_AGE_K_VIOLATION = False   # True: 未達なら raise、False: K-匿名化が未達でも続行


# AGE_WIDTH    = 10       # 年齢帯の幅（歳）: 5, 10, 15, 20など
# USE_REGION   = True    # True: 都道府県を地方別にグループ化
# MAX_ITER     = 3       # Other統合の最大繰り返し回数
# ADD_DUMMY    = True   # True: k未満のグループにダミーデータを注入
#FAIL_ON_AGE_K_VIOLATION = False   # True: 未達なら raise、False: K-匿名化が未達でも続行

spark = (
    SparkSession.builder.appName("anon_from_wide")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

###############################################################################
# 1. データ読込（ワイドテーブル）
###############################################################################
wide = spark.read.table(TABLE_NAME)

###############################################################################
# 2. 必要列の整形 & ハッシュ化
###############################################################################
wide = (
    wide
    # user_token (SHA-256 of ユーザーID)
    .withColumn(
        "user_token",
        F.sha2(F.col("user_id").cast("string"), 256).substr(1, HASH_LEN)
    )
    # period_month
    .withColumn("period_month", F.date_format(F.col("order_created_at"), "yyyy-MM"))
    # 年齢帯 (パラメータで指定した幅)
    .withColumn(
        "age_band",
        F.concat(
            (F.floor(F.months_between(FIXED_DATE, F.col("birth_date")) / 12 / AGE_WIDTH) * AGE_WIDTH).cast("int"),
            F.lit("-"),
            ((F.floor(F.months_between(FIXED_DATE, F.col("birth_date")) / 12 / AGE_WIDTH) * AGE_WIDTH) + AGE_WIDTH - 1).cast("int")
        )
    )
    # 住所 → 都道府県
    .withColumn(
        "prefecture",
        F.expr(r"regexp_extract(`address`, '^(.+?[都道府県])', 1)")
    )
    # 都道府県 → 地方（オプション）
    .withColumn(
        "region",
        F.when(F.col("prefecture").isin("青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県"), "東北")
        .when(F.col("prefecture").isin("茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県"), "関東")
        .when(F.col("prefecture").isin("新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県"), "中部")
        .when(F.col("prefecture").isin("三重県", "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県"), "近畿")
        .when(F.col("prefecture").isin("鳥取県", "島根県", "岡山県", "広島県", "山口県"), "中国")
        .when(F.col("prefecture").isin("徳島県", "香川県", "愛媛県", "高知県"), "四国")
        .when(F.col("prefecture").isin("福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"), "九州・沖縄")
        .otherwise("北海道")
    )
    # USE_REGION フラグに応じて使用する地域カラムを決定
    .withColumn(
        "area",
        F.when(F.lit(USE_REGION), F.col("region")).otherwise(F.col("prefecture"))
    )
    # 金額・数量を数値型で統一
    .withColumn("total_amount", F.col("total_usd").cast("double")) \
    .withColumn("quantity",     F.col("quantity").cast("int"))
    # 必要最小限の列だけ残す
    .select(
        "period_month", "area", "age_band", "category",
        "user_token",   "total_amount", "quantity"
    )
    # NULL 金額は除去
    .where(F.col("total_amount").isNotNull())
)

###############################################################################
# 2'. k-匿名化のバリデーション（period_month,age_band, area, category組合わせのみサンプルとして）
# 必要に応じてgroup byを増やせばよい
###############################################################################

# ユニーク個体で判定（DEDUPE_USERに追従）
base_for_k = (
    wide.select("period_month","user_token","age_band", "area", "category")
        .dropDuplicates(["period_month","user_token"])
    if DEDUPE_USER else
    wide.select("period_month","user_token","age_band", "area", "category")
)

# 準識別子の組み合わせにおける人数の算出
age_eq = (base_for_k
          .groupBy("period_month","age_band", "area", "category")
          .agg(F.countDistinct("user_token").alias("n")))

# 未達の組み合わせ(クラス)（n < K_THRESHOLD）の調査
viol_keys = (age_eq
             .where(F.col("n") < F.lit(K_THRESHOLD))
             .select("period_month", "age_band", "area", "category")
             .distinct())

viol_cnt = viol_keys.count()

if viol_cnt > 0 and FAIL_ON_AGE_K_VIOLATION:
    raise ValueError(f"[age_band k-check] {viol_cnt} group(s) violate k={K_THRESHOLD} at AGE_WIDTH={AGE_WIDTH}.")
else:
    pass  # すべて k を満たしている

###############################################################################
# 3. 重複購入の丸め（任意）
###############################################################################
if DEDUPE_USER:
    win = Window.partitionBy("period_month", "user_token", "category")
    wide = (
        wide.withColumn("dedup_rank", F.row_number().over(win))
            .where(F.col("dedup_rank") == 1)
            .drop("dedup_rank")
    )

###############################################################################
# 4. 集計 (月次 × 地域/都道府県 × 年齢帯 × カテゴリ)
###############################################################################
agg = (
    wide.groupBy("period_month", "area", "age_band", "category")
        .agg(
            F.count(F.lit(1)).alias("orders"),
            F.countDistinct("user_token").alias("unique_customers"),
            F.sum("total_amount").alias("total_sales"),
            F.sum("quantity").alias("total_qty")
        )
        .withColumn("avg_order_value", F.expr("round(total_sales / orders, 2)"))
)

###############################################################################
# 5. ノイズ注入 (任意 ±3 %)
###############################################################################
if ADD_NOISE:
    agg = (
        agg.withColumn(
            "total_sales",
            F.expr("total_sales * (1 + (rand() - 0.5) * 0.06)")
        )
        .withColumn("avg_order_value", F.expr("round(total_sales / orders, 2)"))
    )

###############################################################################
# 6. k匿名性チェック & セル抑制
###############################################################################
# k匿名性チェック
area_type = "region" if USE_REGION else "prefecture"
print(f"🔍 年齢幅 {AGE_WIDTH} 歳 × {area_type}別で集計を実行中...")
pre_suppression_violations = agg.where(F.col("unique_customers") < K_THRESHOLD).count()
total_groups = agg.count()

print(f"   📊 総グループ数: {total_groups}, k={K_THRESHOLD}未満: {pre_suppression_violations}")

if pre_suppression_violations > 0:
    violation_rate = (pre_suppression_violations / total_groups) * 100
    print(f"   ⚠️  セル抑制前のk匿名性違反: {pre_suppression_violations} グループ ({violation_rate:.1f}%)")
else:
    print(f"   ✅ k匿名性を完全に達成しています")

# セル抑制実行
agg = agg.withColumn(
    "category",
    F.when(F.col("unique_customers") < K_THRESHOLD, F.lit("Other")).otherwise(F.col("category"))
)

# Otherカテゴリの反復統合処理
iteration = 0
while iteration < MAX_ITER:
    iteration += 1
    print(f"   🔄 Other統合処理 {iteration}回目...")
    
    # 統合前の状態をチェック
    pre_merge_violations = agg.where(F.col("unique_customers") < K_THRESHOLD).count()
    
    # Otherカテゴリの再集計（同じ期間・地域・年齢帯をまとめる）
    agg = (
        agg.groupBy("period_month", "area", "age_band", "category")
           .agg(
               F.sum("orders").alias("orders"),
               F.sum("unique_customers").alias("unique_customers"), 
               F.sum("total_sales").alias("total_sales"),
               F.sum("total_qty").alias("total_qty")
           )
           .withColumn("avg_order_value", F.expr("round(total_sales / orders, 2)"))
    )
    
    # 統合後の状態をチェック
    post_merge_violations = agg.where(F.col("unique_customers") < K_THRESHOLD).count()
    
    print(f"      統合前k匿名性違反: {pre_merge_violations} → 統合後: {post_merge_violations}")
    
    # 改善がない、または完全達成した場合は終了
    if post_merge_violations == 0:
        print(f"   ✅ {iteration}回目の統合でk匿名性を完全達成")
        break
    elif pre_merge_violations == post_merge_violations:
        print(f"   ⚠️  統合による改善なし、反復処理を終了")
        break
    elif iteration == MAX_ITER:
        print(f"   ⚠️  最大反復回数({MAX_ITER})に到達、処理を終了")

# セル抑制後の最終チェック
final_post_violations = agg.where(F.col("unique_customers") < K_THRESHOLD).count()
if final_post_violations == 0:
    print("   ✅ セル抑制とOther統合によりk匿名性を完全達成")
else:
    print(f"   ⚠️  最終的なk匿名性違反: {final_post_violations} グループ")

# ダミーデータ注入（オプション）
if ADD_DUMMY and final_post_violations > 0:
    print(f"   🎭 ダミーデータ注入処理を実行中...")
    
    # k未満のグループを特定
    violation_groups = agg.where(F.col("unique_customers") < K_THRESHOLD)
    
    # 各違反グループにダミーレコードを追加
    dummy_records = []
    for row in violation_groups.collect():
        shortage = K_THRESHOLD - row["unique_customers"]
        
        for i in range(shortage):
            dummy_record = (
                row["period_month"],
                row["area"], 
                row["age_band"],
                row["category"],
                1,  # orders
                1,  # unique_customers (ダミー)
                float(row["total_sales"] * 0.1),  # 元データの10%程度の売上
                max(1, int(row["total_qty"] * 0.1)),  # 元データの10%程度の数量
                float(row["total_sales"] * 0.1)  # avg_order_value
            )
            dummy_records.append(dummy_record)
    
    if dummy_records:
        # ダミーデータのDataFrameを作成
        dummy_schema = agg.schema
        dummy_df = spark.createDataFrame(dummy_records, dummy_schema)
        
        # 元データとダミーデータを結合
        agg = agg.union(dummy_df)
        
        # 再集計してダミーを統合
        agg = (
            agg.groupBy("period_month", "area", "age_band", "category")
               .agg(
                   F.sum("orders").alias("orders"),
                   F.sum("unique_customers").alias("unique_customers"), 
                   F.sum("total_sales").alias("total_sales"),
                   F.sum("total_qty").alias("total_qty")
               )
               .withColumn("avg_order_value", F.expr("round(total_sales / orders, 2)"))
        )
        
        # ダミー注入後のチェック
        post_dummy_violations = agg.where(F.col("unique_customers") < K_THRESHOLD).count()
        print(f"   🎭 ダミーデータ注入完了: {len(dummy_records)}件追加")
        print(f"      注入後のk匿名性違反: {post_dummy_violations} グループ")
        
        if post_dummy_violations == 0:
            print("   ✅ ダミーデータ注入によりk匿名性を完全達成")
    else:
        print("   ⚠️  ダミーデータ生成に失敗")

###############################################################################
# 7. コンソール出力のみ
###############################################################################
print(f"\n📋 最終集計結果 (年齢幅: {AGE_WIDTH}歳, {area_type}別, k >= {K_THRESHOLD})")
print("=" * 80)

agg.orderBy("period_month", "area", "age_band", "category") \
   .show(200, truncate=False)

# k匿名性の最終確認
final_violations = agg.where(F.col("unique_customers") < K_THRESHOLD).count()
total_final_groups = agg.count()

print("=" * 80)
print(f"🔒 k匿名性最終チェック結果:")
print(f"   - 年齢幅: {AGE_WIDTH}歳")
print(f"   - 地域区分: {area_type}")
print(f"   - 総グループ数: {total_final_groups}")
print(f"   - k={K_THRESHOLD}未満のグループ: {final_violations}")
print(f"   - k匿名性達成率: {((total_final_groups - final_violations) / total_final_groups * 100):.1f}%")

if final_violations == 0:
    print("✅ k匿名性を完全に達成しています")
else:
    print("⚠️  一部グループでk匿名性違反が残存しています")

print("✅  Anonymization finished (console output only)")


# 年齢幅で見ている部分があるため9/6の固定時点
# +------------+--------+--------+--------+------+----------------+-----------+---------+---------------+
# |period_month|area    |age_band|category|orders|unique_customers|total_sales|total_qty|avg_order_value|
# +------------+--------+--------+--------+------+----------------+-----------+---------+---------------+
# |2024-05     |大阪府  |10-14   |Other   |1     |1               |51.0       |2        |51.0           |
# |2024-05     |大阪府  |10-14   |Other   |1     |1               |85.0       |4        |85.0           |
# |2024-05     |大阪府  |35-39   |Other   |1     |1               |153.0      |5        |153.0          |
# |2024-05     |大阪府  |45-49   |Other   |1     |1               |81.0       |1        |81.0           |
# |2024-05     |東京都  |20-24   |Other   |1     |1               |82.0       |1        |82.0           |
# |2024-05     |東京都  |20-24   |Other   |1     |1               |29.0       |1        |29.0           |
# |2024-05     |東京都  |25-29   |Other   |1     |1               |57.0       |1        |57.0           |
# |2024-05     |東京都  |40-44   |Other   |1     |1               |298.0      |4        |298.0          |
# |2024-05     |神奈川県|25-29   |Other   |1     |1               |149.0      |2        |149.0          |
# |2024-05     |神奈川県|40-44   |Other   |1     |1               |376.0      |5        |376.0          |
# |2024-06     |大阪府  |15-19   |Other   |1     |1               |329.0      |4        |329.0          |
# |2024-06     |大阪府  |35-39   |Other   |1     |1               |162.0      |2        |162.0          |
# |2024-06     |大阪府  |35-39   |Other   |1     |1               |84.0       |1        |84.0           |
# |2024-06     |大阪府  |35-39   |Other   |1     |1               |1857.0     |30       |1857.0         |
# |2024-06     |大阪府  |45-49   |Other   |1     |1               |80.0       |5        |80.0           |
# |2024-06     |大阪府  |55-59   |Other   |1     |1               |75.0       |1        |75.0           |
# |2024-06     |東京都  |25-29   |Other   |1     |1               |541.0      |5        |541.0          |
# |2024-06     |神奈川県|40-44   |Other   |1     |1               |357.0      |5        |357.0          |
# |2024-06     |神奈川県|40-44   |Other   |1     |1               |17.0       |1        |17.0           |
# |2024-06     |神奈川県|50-54   |Other   |1     |1               |485.0      |5        |485.0          |
# |2024-07     |大阪府  |55-59   |Other   |1     |1               |251.0      |4        |251.0          |
# |2024-07     |東京都  |15-19   |Other   |1     |1               |156.0      |3        |156.0          |
# |2024-07     |東京都  |25-29   |Other   |1     |1               |127.0      |5        |127.0          |
# |2024-07     |東京都  |30-34   |Other   |1     |1               |48.0       |3        |48.0           |
# |2024-07     |神奈川県|15-19   |Other   |1     |1               |107.0      |5        |107.0          |
# |2024-07     |神奈川県|50-54   |Other   |1     |1               |274.0      |3        |274.0          |
# |2024-08     |大阪府  |30-34   |Other   |1     |1               |175.0      |3        |175.0          |
# |2024-08     |大阪府  |30-34   |Other   |1     |1               |91.0       |1        |91.0           |
# |2024-08     |大阪府  |45-49   |Other   |1     |1               |77.0       |5        |77.0           |
# |2024-08     |大阪府  |60-64   |Other   |1     |1               |330.0      |5        |330.0          |
# |2024-08     |東京都  |25-29   |Other   |1     |1               |311.0      |5        |311.0          |
# |2024-08     |神奈川県|15-19   |Other   |1     |1               |149.0      |3        |149.0          |
# |2024-08     |神奈川県|20-24   |Other   |1     |1               |75.0       |4        |75.0           |
# |2024-08     |神奈川県|40-44   |Other   |1     |1               |181.0      |3        |181.0          |
# |2024-09     |大阪府  |10-14   |Other   |1     |1               |324.0      |4        |324.0          |
# |2024-09     |大阪府  |45-49   |Other   |1     |1               |122.0      |4        |122.0          |
# |2024-09     |大阪府  |60-64   |Other   |1     |1               |261.0      |3        |261.0          |
# |2024-09     |東京都  |25-29   |Other   |1     |1               |50.0       |4        |50.0           |
# |2024-09     |東京都  |30-34   |Other   |1     |1               |105.0      |1        |105.0          |
# |2024-09     |神奈川県|60-64   |Other   |1     |1               |35.0       |2        |35.0           |
# |2024-10     |大阪府  |35-39   |C01     |2     |2               |365.0      |6        |182.5          |
# |2024-10     |大阪府  |55-59   |Other   |1     |1               |68.0       |1        |68.0           |
# |2024-10     |東京都  |55-59   |Other   |1     |1               |52.0       |4        |52.0           |
# |2024-10     |神奈川県|20-24   |Other   |2     |1               |102.0      |4        |51.0           |
# |2024-10     |神奈川県|20-24   |Other   |1     |1               |145.0      |4        |145.0          |
# |2024-10     |神奈川県|45-49   |Other   |1     |1               |48.0       |3        |48.0           |
# |2024-10     |神奈川県|45-49   |Other   |1     |1               |406.0      |5        |406.0          |
# |2024-11     |大阪府  |15-19   |Other   |1     |1               |430.0      |5        |430.0          |
# |2024-11     |大阪府  |15-19   |Other   |1     |1               |27.0       |1        |27.0           |
# |2024-11     |大阪府  |50-54   |Other   |2     |1               |0.0        |0        |0.0            |
# |2024-11     |大阪府  |60-64   |Other   |1     |1               |95.0       |5        |95.0           |
# |2024-11     |東京都  |25-29   |Other   |1     |1               |98.0       |5        |98.0           |
# |2024-11     |神奈川県|25-29   |Other   |1     |1               |24.0       |2        |24.0           |
# |2024-12     |NULL    |NULL    |Other   |1     |1               |337.0      |4        |337.0          |
# |2024-12     |大阪府  |15-19   |Other   |1     |1               |62.0       |1        |62.0           |
# |2024-12     |大阪府  |30-34   |Other   |1     |1               |58.0       |1        |58.0           |
# |2024-12     |大阪府  |40-44   |Other   |1     |1               |25.0       |2        |25.0           |
# |2024-12     |大阪府  |45-49   |Other   |1     |1               |58.0       |1        |58.0           |
# |2024-12     |大阪府  |55-59   |Other   |1     |1               |186.0      |3        |186.0          |
# |2024-12     |大阪府  |60-64   |Other   |1     |1               |289.0      |5        |289.0          |
# |2024-12     |神奈川県|15-19   |Other   |1     |1               |106.0      |4        |106.0          |
# |2024-12     |神奈川県|25-29   |Other   |1     |1               |162.0      |2        |162.0          |
# |2024-12     |神奈川県|30-34   |Other   |1     |1               |234.0      |3        |234.0          |
# |2024-12     |神奈川県|45-49   |Other   |1     |1               |248.0      |4        |248.0          |
# |2024-12     |神奈川県|50-54   |Other   |1     |1               |451.0      |5        |451.0          |
# |2025-01     |大阪府  |20-24   |Other   |1     |1               |230.0      |4        |230.0          |
# |2025-01     |大阪府  |35-39   |Other   |1     |1               |55.0       |1        |55.0           |
# |2025-01     |大阪府  |55-59   |Other   |1     |1               |64.0       |4        |64.0           |
# |2025-01     |大阪府  |55-59   |Other   |1     |1               |214.0      |3        |214.0          |
# |2025-01     |大阪府  |60-64   |Other   |1     |1               |59.0       |1        |59.0           |
# |2025-01     |東京都  |25-29   |Other   |1     |1               |237.0      |5        |237.0          |
# |2025-01     |神奈川県|20-24   |Other   |1     |1               |110.0      |2        |110.0          |
# |2025-01     |神奈川県|45-49   |Other   |1     |1               |74.0       |1        |74.0           |
# |2025-01     |神奈川県|60-64   |Other   |1     |1               |409.0      |4        |409.0          |
# |2025-02     |大阪府  |55-59   |Other   |1     |1               |310.0      |5        |310.0          |
# |2025-02     |神奈川県|40-44   |Other   |1     |1               |173.0      |3        |173.0          |
# |2025-02     |神奈川県|60-64   |Other   |1     |1               |405.0      |5        |405.0          |
# |2025-03     |大阪府  |10-14   |Other   |1     |1               |261.0      |3        |261.0          |
# |2025-03     |大阪府  |25-29   |Other   |1     |1               |65.0       |1        |65.0           |
# |2025-03     |大阪府  |45-49   |Other   |1     |1               |172.0      |3        |172.0          |
# |2025-03     |大阪府  |55-59   |Other   |1     |1               |226.0      |3        |226.0          |
# |2025-03     |大阪府  |55-59   |Other   |1     |1               |58.0       |1        |58.0           |
# |2025-03     |神奈川県|10-14   |Other   |1     |1               |75.0       |4        |75.0           |
# |2025-03     |神奈川県|35-39   |Other   |1     |1               |348.0      |4        |348.0          |
# |2025-03     |神奈川県|50-54   |Other   |1     |1               |1257.0     |24       |1257.0         |
# |2025-04     |大阪府  |15-19   |Other   |2     |1               |7329.0     |4        |3664.5         |
# |2025-04     |大阪府  |30-34   |Other   |1     |1               |152.0      |3        |152.0          |
# |2025-04     |大阪府  |35-39   |Other   |2     |1               |408.0      |4        |204.0          |
# |2025-04     |大阪府  |40-44   |Other   |1     |1               |15.0       |1        |15.0           |
# |2025-04     |大阪府  |45-49   |C02     |2     |2               |92.0       |6        |46.0           |
# |2025-04     |大阪府  |50-54   |Other   |1     |1               |175.0      |3        |175.0          |
# |2025-04     |大阪府  |50-54   |Other   |1     |1               |43.0       |2        |43.0           |
# |2025-04     |大阪府  |60-64   |Other   |1     |1               |62.0       |5        |62.0           |
# |2025-04     |東京都  |30-34   |Other   |1     |1               |78.0       |4        |78.0           |
# |2025-04     |東京都  |40-44   |Other   |1     |1               |430.0      |5        |430.0          |
# |2025-04     |神奈川県|40-44   |Other   |1     |1               |87.0       |1        |87.0           |
# |2025-04     |神奈川県|50-54   |C04     |2     |2               |202.0      |3        |101.0          |
# |2025-05     |大阪府  |50-54   |Other   |1     |1               |12.0       |1        |12.0           |
# |2025-05     |東京都  |55-59   |Other   |1     |1               |58.0       |1        |58.0           |
# +------------+--------+--------+--------+------+----------------+-----------+---------+---------------+


# 結構頑張って探すとk=2くらいでも結構探すの大変です。ただ本のデータがわかれば見つけることが可能です(ちなみに、chatgpt o3にデータを渡して調べてもらいましたが2回間違えました。)

# |2025-04     |大阪府    |45-49   |C02     |2     |2               |92.0       |6        |46.0           |
# は以下のデータです。

# +------+----------+------+-------+-------+-------+-------+------+-------------------+----+-------------+---------------------------+--------+-------------+-------+-----------+-------------------+-------------------+-------------------+-----------------+-----------------------------------+------------------+----------+----------+---------------+----------+--------+-------------------+-------------------+-----------------------+---------------------+-----------+--------------------+
# |注文ID|ユーザーID|商品ID|小計_円|税額_円|合計_円|数量_個|フラグ|      注文_作成日時|親ID|    EANコード|              商品_タイトル|カテゴリ|ベンダー_名称|価格_円|評価_RATING|      商品_作成日時|      商品_更新日時|商品_論理削除フラグ|商品_論理削除日時|                               住所|    メールアドレス|パスワード|ユーザー名|チャネル_取得元|  生年月日|郵便番号|  ユーザー_作成日時|  ユーザー_更新日時|ユーザー_論理削除フラグ|ユーザー_論理削除日時|ingest_date|        user_id_hash|
# +------+----------+------+-------+-------+-------+-------+------+-------------------+----+-------------+---------------------------+--------+-------------+-------+-----------+-------------------+-------------------+-------------------+-----------------+-----------------------------------+------------------+----------+----------+---------------+----------+--------+-------------------+-------------------+-----------------------+---------------------+-----------+--------------------+
# |    14|        84|     2|     70|      7|     77|      5|     1|2025-04-20 22:03:21|NULL|1724396229455|      メンズTシャツ Lサイズ|     C02|         花王|  19765|          2|2024-09-18 01:11:09|2024-10-10 01:11:09|              false|             NULL|    大阪府大阪市平野区2丁目17番42号|user84@example.com|    pass84|   User 84|      Instagram|1975-09-16| 0052014|2024-05-25 22:03:21|2024-05-25 22:03:21|                  false|                 NULL| 2025-07-24|-4401175757844524499|
# |    34|        56|     2|     14|      1|     15|      1|     1|2025-04-05 22:03:21|NULL|1724396229455|      メンズTシャツ Lサイズ|     C02|         花王|  19765|          2|2024-09-18 01:11:09|2024-10-10 01:11:09|              false|             NULL|  大阪府大阪市天王寺区1丁目20番40号|user56@example.com|    pass56|   User 56|        Twitter|1978-03-13| 0099101|2025-01-22 22:03:21|2025-01-22 22:03:21|                  false|                 NULL| 2025-07-24| 8525542463368288474|
# +------+----------+------+-------+-------+-------+-------+------+-------------------+----+-------------+---------------------------+--------+-------------+-------+-----------+-------------------+-------------------+-------------------+-----------------+-----------------------------------+------------------+----------+----------+---------------+----------+--------+-------------------+-------------------+-----------------------+---------------------+-----------+--------------------+

# C04カテゴリ
# >>> spark.table("local_data_platform.slv_entities.user_orders_wide").filter(F.col("category")=="C04").filter("address LIKE '%神奈川%'").show(truncate=False)
# +--------+-------+----------+------------+-------+---------+--------+-----------+-------------------+---------+-------------+---------------------------+--------+-----------+---------+------+-------------------+-------------------+------------------+------------------+-----------------------------------+------------------+--------+---------+-------------------+----------+--------+-------------------+-------------------+---------------+---------------+-----------+--------------------+
# |order_id|user_id|product_id|subtotal_usd|tax_usd|total_usd|quantity|status_flag|order_created_at   |parent_id|ean_code     |product_title              |category|vendor_name|price_usd|rating|product_created_at |product_updated_at |product_is_deleted|product_deleted_at|address                            |email             |password|user_name|acquisition_channel|birth_date|zip_code|user_created_at    |user_updated_at    |user_is_deleted|user_deleted_at|ingest_date|user_id_hash        |
# +--------+-------+----------+------------+-------+---------+--------+-----------+-------------------+---------+-------------+---------------------------+--------+-----------+---------+------+-------------------+-------------------+------------------+------------------+-----------------------------------+------------------+--------+---------+-------------------+----------+--------+-------------------+-------------------+---------------+---------------+-----------+--------------------+
# |30      |24     |15        |89.00       |9.00   |97.00    |1       |1          |2025-04-01 07:03:21|NULL     |7034641801679|トイレットペーパー 12ロール|C04     |花王       |22754.00 |3.9000|2025-01-21 10:11:09|2025-01-25 10:11:09|false             |NULL              |神奈川県横浜市栄区5丁目9番49号     |user24@example.com|pass24  |User 24  |Facebook           |1973-01-14|0051509 |2025-03-09 07:03:21|2025-03-09 07:03:21|false          |NULL           |2025-09-05 |7788558767429488329 |
# |24      |46     |60        |96.00       |10.00  |105.00   |2       |1          |2025-04-25 07:03:21|NULL     |6703225397281|フェイスマスク 5枚         |C04     |花王       |40511.00 |4.9000|2025-03-16 10:11:09|2025-03-30 10:11:09|false             |NULL              |神奈川県横浜市旭区3丁目3番34号     |user46@example.com|pass46  |User 46  |Instagram          |1970-09-20|0030081 |2024-05-06 07:03:21|2024-05-06 07:03:21|false          |NULL           |2025-09-05 |-2062731188938574217|
# +--------+-------+----------+------------+-------+---------+--------+-----------+-------------------+---------+-------------+---------------------------+--------+-----------+---------+------+-------------------+-------------------+------------------+------------------+-----------------------------------+------------------+--------+---------+-------------------+----------+--------+-------------------+-------------------+---------------+---------------+-----------+--------------------+
