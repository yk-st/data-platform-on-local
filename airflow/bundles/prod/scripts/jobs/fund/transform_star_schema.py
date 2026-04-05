# ─────────────────────────────────────────────────────────
# aggregate_performance.py   Spark 3.5.5 / Iceberg 1.9
#   Raw      : brz_raw.fund_nav / brz_raw.fund_master
#   Dim/Facts: gld_fund.dim_fund (SCD2) / dim_date / fct_fund_performance
#   ※ Wide テーブルを経由せず Star 直行
# ─────────────────────────────────────────────────────────
from pyspark.sql import functions as F, Window
from spark_utils import get_spark
from jobs.fund.config import FundConfig


def aggregate_performance(config: FundConfig, args) -> None:
    spark = get_spark("gold-scd2")

    # ─── テーブル名解決 ───────────────────────────
    tbl_nav_raw    = config.get_dynamic_table_name(config.TABLE_FUND_NAV,    args)
    tbl_master_raw = config.get_dynamic_table_name(config.TABLE_FUND_MASTER, args)
    tbl_dim_fund   = config.get_dynamic_table_name(config.TABLE_DIM_FUND,    args)
    tbl_dim_date   = config.get_dynamic_table_name(config.TABLE_DIM_DATE,    args)
    tbl_fact       = config.get_dynamic_table_name(config.TABLE_FCT_FUND_PERFORMANCE, args)

    # ─── Raw 読み込み ────────────────────────────
    nav_df = (
        spark.read.format("iceberg")
             .table(tbl_nav_raw)
             .select(
                 F.col("fund_id"),
                 F.col("base_date").cast("date").alias("base_date"),
                 F.col("nav_price").cast("decimal(18,4)").alias("nav_price_usd")
             )
    )

    master_df = (
        spark.read.format("iceberg")
             .table(tbl_master_raw)
             .select(
                 F.col("fund_id"),
                 F.col("fund_category"),
                 F.col("trust_fee_rate"),
                 F.col("is_active").alias("is_active")
             )
    ).filter(F.col("ingest_date") == args.ingest_date)  # 最新のマスタのみ

    # ==================================================================
    # 1. dim_fund  (SCD-Type 2)
    # ==================================================================
    src_dim = (                             # ★ すべて src_ プレフィクス
        master_df
        .select(
            F.col("fund_id").alias("src_fund_id"),
            F.col("fund_category").alias("src_fund_category"),
            F.col("trust_fee_rate").alias("src_trust_fee_rate"),
            F.col("is_active").alias("src_is_active")
        )
        .dropDuplicates(["src_fund_id"])
    )

    cur_dim = (
        spark.read.format("iceberg")
             .table(tbl_dim_fund)
             .filter(F.col("is_current"))
             .select(
                 F.col("fund_id").alias("cur_fund_id"),
                 F.col("fund_category").alias("cur_fund_category"),
                 F.col("trust_fee_rate").alias("cur_trust_fee_rate"),
                 F.col("is_active").alias("cur_is_active")
             )
    )

    # cur_dim v1時点のデータ
    # src_dim v2時点のデータ
    # 組み合わせを出し(full)、属性(fund_categoryなどが)違う場合は、変更のフラグ(NEW/CHANGED/UNCHANGED)を立てる
    chg = (
        src_dim.join(cur_dim,
                     src_dim["src_fund_id"] == cur_dim["cur_fund_id"],
                     "full")
              .withColumn(
                  "_chg",
                  F.when(F.col("cur_fund_id").isNull(), "NEW")
                   .when(
                       (F.col("src_fund_category") != F.col("cur_fund_category")) |
                       (F.col("src_trust_fee_rate")  != F.col("cur_trust_fee_rate"))  |
                       (F.col("src_is_active")   != F.col("cur_is_active")),
                       "CHANGED"
                   )
                   .otherwise("UNCHANGED")
              )
              .withColumn("fund_id",
                          F.coalesce("src_fund_id", "cur_fund_id"))
    )

    # 1-A) 旧バージョン CLOSE
    (chg.filter(F.col("_chg") == "CHANGED")
         .select("fund_id")
         .createOrReplaceTempView("to_close"))

    spark.sql(f"""
        MERGE INTO {tbl_dim_fund} t
        USING to_close c
        ON  t.`fund_id` = c.`fund_id` AND t.`is_current` = true
        WHEN MATCHED THEN UPDATE SET
             t.`effective_end_date` = current_date(),
             t.`is_current` = false
    """)

    # 1-B) 新バージョン INSERT
    new_dim_rows = (
        chg.filter(F.col("_chg").isin("NEW", "CHANGED"))
           .withColumn(
               "fund_sk",
               F.xxhash64(
                   F.concat(
                       F.col("src_fund_id"),
                       F.lit("_"),
                       F.date_format(F.current_date(), "yyyyMMdd"),
                       F.lit("_"),
                       F.col("src_fund_category"),
                       F.lit("_"),
                       F.col("src_trust_fee_rate").cast("string"),
                       F.lit("_"),
                       F.col("src_is_active").cast("string")
                   )
               ).cast("bigint")
           )
           .withColumn("effective_start_date",  F.current_date())
           .withColumn("effective_end_date",  F.lit("9999-12-31").cast("date"))
           .withColumn("is_current",   F.lit(True))
           .select(
               "fund_sk", "fund_id",
               F.col("src_fund_category").alias("fund_category"),
               F.col("src_trust_fee_rate").alias("trust_fee_rate"),
               F.col("src_is_active").alias("is_active"),
               "effective_start_date", "effective_end_date", "is_current"
           )
    )
    if new_dim_rows.head(1):
        new_dim_rows.write.format("iceberg") \
                     .mode("append") \
                     .saveAsTable(tbl_dim_fund)

    # ==================================================================
    # 2. dim_date  — 不足分補充
    # ==================================================================
    missing_dates = spark.sql(f"""
        SELECT DISTINCT `base_date` AS `date_value`
        FROM {tbl_nav_raw}
        WHERE `base_date` NOT IN (SELECT `date_value` FROM {tbl_dim_date})
    """)

    if missing_dates.head(1):
        (missing_dates
            .withColumn("date_sk", F.date_format("date_value", "yyyyMMdd").cast("int"))
            .withColumn("year",     F.year("date_value"))
            .withColumn("month",     F.month("date_value"))
            .withColumn("day",     F.dayofmonth("date_value"))
            .withColumn("day_of_week",   F.dayofweek("date_value"))
            .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
            .withColumn("is_month_end", F.last_day("date_value") == F.col("date_value"))
            .withColumn("year_month",   F.date_format("date_value", "yyyyMM"))
            .withColumn("fiscal_year",
                F.when(F.month("date_value") <= 3,
                       F.year("date_value") - 1).otherwise(F.year("date_value")))
            .write.format("iceberg")
                   .mode("append")
                   .saveAsTable(tbl_dim_date)
        )

    # ==================================================================
    # 3. Fact Δ抽出 ＋ MERGE
    # ==================================================================
    # ── 直前日を事前に取っておく ──────────────────
    last_dt = (
        spark.read.format("iceberg")
             .table(tbl_fact)
             .agg(F.max("base_date"))
             .first()[0]
    )

    # 追加レコード抽出
    nav_delta = nav_df.filter(F.col("base_date") > last_dt) if last_dt else nav_df
    if not nav_delta.head(1):
        print("🔹 追加なし：処理終了")
        spark.stop()
        return
    # 再計算しちゃう場合
    # nav_delta = nav_df

    # 各ファンドの「最新価額」を既存 Fact から取得（各ファンド別に最新日が異なる可能性を考慮）
    last_fact_per_fund = (
        spark.read.format("iceberg")
             .table(tbl_fact)
             .withColumn("rn", F.row_number().over(
                 Window.partitionBy("fund_sk").orderBy(F.desc("base_date"))
             ))
             .filter(F.col("rn") == 1)
             .select("fund_sk", "base_date", "nav_price_usd")
    )

    # Δにまだ dim キー解決が出来ていないので fund_id で合わせる
    dim_fund_cur = (
        spark.read.format("iceberg")
             .table(tbl_dim_fund)
             .filter(F.col("is_current"))
             .select("fund_sk", "fund_id")
    )

    # 履歴テーブルから fund_id を取得するために全ての dim_fund を参照
    dim_fund_all = (
        spark.read.format("iceberg")
             .table(tbl_dim_fund)
             .select("fund_sk", "fund_id")
    )

    dim_date = (
        spark.read.format("iceberg")
             .table(tbl_dim_date)
             .select("date_sk", "date_value")
    )

    # nav_price_yesterday用のテーブルを fund_id キーに変換（履歴も含む全てのdim_fundでjoin）
    last_nav_stub = (
        last_fact_per_fund
            .join(dim_fund_all, "fund_sk")  # 履歴も含む全てのfund_skでjoin
            .select(
                F.col("fund_id").alias("fund_id"),
                F.col("base_date").alias("prev_base_date"),
                F.col("nav_price_usd").alias("prev_nav_price")
            )
    )

    # 全ファンドのΔデータに対して、各ファンドの最新価額を結合
    nav_with_history = (
        nav_delta.alias("n")
            .join(last_nav_stub.alias("p"), "fund_id", "left")
            .select(
                F.col("fund_id").alias("fund_id_hist"),
                "base_date",
                "nav_price_usd",
                "prev_base_date",
                "prev_nav_price"
            )
    )

    # Window関数でnav_price_yesterdayを計算（各ファンド内での時系列順序）
    win = Window.partitionBy("fund_id_hist").orderBy("base_date")

    fact_delta = (
        nav_with_history.alias("n")
            .join(dim_fund_cur.alias("d"), F.col("n.fund_id_hist") == F.col("d.fund_id"))
            .join(dim_date.alias("dt"), F.col("n.base_date") == F.col("dt.date_value"))
            .withColumn(
                "nav_price_yesterday",
                F.when(
                    F.lag("nav_price_usd").over(win).isNull(),
                    F.col("prev_nav_price")  # 初回レコードは既存の最新価額を使用
                ).otherwise(
                    F.lag("nav_price_usd").over(win)  # 通常のWindow関数
                )
            )
            .withColumn(
                "price_change_usd",
                F.when(F.col("nav_price_yesterday").isNull(), F.lit(0))
                .otherwise(F.col("nav_price_usd") - F.col("nav_price_yesterday"))
            )
            .withColumn(
                "return_rate_pct",
                F.when(F.col("nav_price_yesterday").isNull(), F.lit(0))
                .otherwise(F.col("price_change_usd") / F.col("nav_price_yesterday") * 100)
            )
            .select(
                "dt.date_sk",
                "d.fund_sk",
                "n.base_date",
                "nav_price_usd",
                "price_change_usd",
                "return_rate_pct"
            )
    )
    fact_delta.createOrReplaceTempView("fact_delta")

    spark.sql(f"""
        MERGE INTO {tbl_fact} t
        USING fact_delta s
        ON  t.`fund_sk` = s.`fund_sk`
        AND t.`date_sk`    = s.`date_sk`
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("✅ Dim/Facts への統合完了")
    spark.stop()


if __name__ == "__main__":
    args = FundConfig.parse_args()
    aggregate_performance(FundConfig(), args)
