from pyspark.sql import functions as F
from spark_utils import get_spark
spark = get_spark()

orders = spark.read.table("local_data_platform.slv_entities.user_orders_wide")

### ① キャッシュしたい DF
daily = (
    orders
      .withColumn("ingest_date", F.to_date("ingest_date"))
      .groupBy("ingest_date")
      .agg(F.sum("total_usd").alias("total_usd"))
)


### ② キャッシュ & マテリアライズ
# cached = daily.cache()
daily.count()                           # materialize

### ③ “別の演算” で **再利用**
#    ここで初めて InMemoryTableScanExec が差し込まれる
reused = daily.filter("`total_usd` > 10000")   # 何でも良い
reused.count()

### ④ プラン確認
reused.explain("formatted")              # ← ここに InMemoryTableScanExec
print(
    reused._jdf.queryExecution()
          .executedPlan()
          .treeString()
)

spark.stop()
