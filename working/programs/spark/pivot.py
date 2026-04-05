from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PivotExample").getOrCreate()

# サンプルデータの作成
data = [
    ("2024-01-01", "A", 100),
    ("2024-01-01", "B", 200),
    ("2024-01-02", "A", 150),
    ("2024-01-02", "B", 250)
]

columns = ["date", "category", "value"]
df = spark.createDataFrame(data, columns)

# 縦持ちから横持ちへの変換
pivot_df = df.groupBy("date").pivot("category").agg({"value": "sum"})
pivot_df.show()

# pivot_df(横持ち。Categoryが増えると使いにくい)
# +----------+---+---+
# |      date|  A|  B|
# +----------+---+---+
# |2024-01-02|150|250|
# |2024-01-01|100|200|
# +----------+---+---+

# df(縦持ち。Categoryが増えても使いやすい(集計などがやりやすい))
# +----------+--------+-----+
# |      date|category|value|
# +----------+--------+-----+
# |2024-01-01|       A|  100|
# |2024-01-01|       B|  200|
# |2024-01-02|       A|  150|
# |2024-01-02|       B|  250|
# +----------+--------+-----+