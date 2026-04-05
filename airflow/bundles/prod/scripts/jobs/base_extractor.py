from abc import ABC, abstractmethod
from pyspark.sql import functions as F
from spark_utils import get_spark
from pyspark.sql import DataFrame


class BaseExtractor(ABC):
    """
    ・extract() で取得した DF に **updated_at (timestamp)** が含まれていること
    ・Bronze Iceberg テーブルは PARTITIONED BY (ingest_date DATE)
    """

    def __init__(self, config, args=None):
        self.config = config                      # config.ICEBERG_BASE_PATH を想定
        self.args = args

    def build(self, pal_key, subquery, fetch_size=1000, lower_bound="1", upper_bound="10000", num_partitions=4) -> dict[str, str]:
        """Return the dict you can hand to `.options()`."""

        # subquery の None チェック
        if subquery is None:
            raise ValueError("subquery cannot be None. Please provide a valid SQL subquery.")
        
        if not isinstance(subquery, str) or not subquery.strip():
            raise ValueError("subquery must be a non-empty string.")
        
        return {
            # connection
            "url":      self.config.POSTGRES_URL,
            "user":     self.config.POSTGRES_USER,
            "password": self.config.POSTGRES_PASSWORD,
            # SELECT * FROM public.orders WHERE logical_date > DATE '{wm}'のようなSQLを想定する
            "dbtable":  subquery,                  # push-down + watermark

            # chunking
            "fetchsize":        fetch_size,
            # parallel ingestion
            "partitionColumn":  pal_key,
            "lowerBound":       str(lower_bound),
            "upperBound":       str(upper_bound),
            "numPartitions":    num_partitions,

            # consistency (READ_COMMITTED is default – no need to set)
            # "isolationLevel": "READ_COMMITTED",
        }


    # ------- サブクラス実装 ---------------------------------
    @abstractmethod
    def extract(self, spark) -> DataFrame: ...
    @abstractmethod
    def target_table(self) -> str: ...
    # -------------------------------------------------------

    def run(self, part_col="ingest_date") -> None:
        spark = get_spark()

        df = self.extract(spark)

        # 1) updated_at → ingest_date (DATE) を付与
        if part_col not in df.columns:
            raise ValueError(f"DataFrame に '{part_col}' 列がありません。")

        # df = df_raw.withColumn("ingest_date", F.to_date("作成日時"))

        # 2) Iceberg テーブルへパーティション単位で上書き
        df.writeTo(self.target_table()) \
          .overwritePartitions()                  # ← ingest_date ごとに置換／追加

        spark.stop()


    def run_overwrite(self) -> None:
        spark = get_spark()

        df_raw = self.extract(spark)

        # 2) Iceberg テーブルへパーティション単位で上書き
        df_raw.writeTo(self.target_table()) \
          .overwritePartitions()                  # ← ingest_date ごとに置換／追加

        spark.stop()


    def run_v1(self, TGT_TABLE, TABLE_ROOT) -> None:
        spark = get_spark()

        df_raw = self.extract(spark)

        # 4) S3 書き込み（overwrite）
        (df_raw.repartition(2)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("ingest_date")
        .parquet(TABLE_ROOT))

        # 5) HMS にパーティション登録
        # mcsk repaireでも可能
        dates = df_raw.select("ingest_date").distinct().rdd.flatMap(lambda r: r).collect()
        for d in dates:
            spark.sql(f"""
                ALTER TABLE {TGT_TABLE}
                ADD IF NOT EXISTS PARTITION (ingest_date = '{d}')
                LOCATION '{TABLE_ROOT}/ingest_date={d}'
            """)

        spark.stop()