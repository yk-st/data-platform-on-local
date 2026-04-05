from pyspark.sql import functions as F
from spark_utils import get_spark
from pyspark import StorageLevel

spark = get_spark()

orders = spark.read.table("local_data_platform.slv_entities.user_orders_wide")

### ① キャッシュしたい DF
daily = (
    orders
      .withColumn("ingest_date", F.to_date("ingest_date"))
      .groupBy("ingest_date")
      .agg(F.sum("total_usd").alias("total_usd"))
)

### ② キャッシュ & マテリアライズ(MEMORY_AND_DISK_DESER)
cached = daily.cache()
cached.count()                           # materialize

### ③ “別の演算” で **再利用**
#    ここで初めて InMemoryTableScanExec が差し込まれる
reused = cached.filter("`total_usd` > 10000")   # 何でも良い
reused.count()

### ④ プラン確認
reused.explain("formatted")              # ← ここに InMemoryTableScanExec
print(
    reused._jdf.queryExecution()
          .executedPlan()
          .treeString()
)

cached.unpersist()