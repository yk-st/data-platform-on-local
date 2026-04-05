# PySpark + cuallee
from pyspark.sql import functions as F, Window as W
from cuallee import Check, CheckLevel
from spark_utils import get_spark

# /home/pyspark/programs/spark/data_quality/spark_dim_fact_qu.py

# ──────────────────────────────────────────────────────────────
# 0) SparkSession & セッション UTC 強制
# ──────────────────────────────────────────────────────────────
spark = get_spark("dq-orders-cuallee")
spark.sparkContext.setLogLevel("ERROR") 

# half-open: [効力開始日, 効力終了日)
# eff_to が NULL の場合は番兵 '9999-12-31' を採用
SENTRY = "9999-12-31"

def no_overlap_check(df):
    """
    オーバーラップをチェックして、違反のないDataFrameを返す
    """
    eff_to = F.coalesce(F.col("effective_end_date"), F.to_date(F.lit(SENTRY)))
    w = W.partitionBy("fund_id").orderBy(F.col("effective_start_date").asc(), eff_to.asc())
    
    prev_to = F.lag(eff_to).over(w)
    ok = (F.col("effective_start_date") >= prev_to) | prev_to.isNull()
    
    return df.withColumn("no_overlap", ok)

# ----- チェック定義 -----
check = Check(CheckLevel.ERROR, "SCD2: 期間オーバーラップ無し")

check.is_custom(
    column=["fund_id", "effective_start_date", "effective_end_date"],
    fn=no_overlap_check,
    pct=1.0  # 100%の行がパスすることを期待
)

# 実行
# df は dim_fund を Spark で読んだ DataFrame
df= spark.read.table("local_data_platform.slv_fund.dim_fund")


result = check.validate(df)
result.show(truncate=False)
