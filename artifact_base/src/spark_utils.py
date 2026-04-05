# spark_utils.py
from pyspark.sql import SparkSession
from typing import Dict, Optional

# ❶ ここでクラスタ既定 master を一元管理
DEFAULT_MASTER = "spark://spark-master1.local.data.platform:7077"


def get_spark(
    app_name: str = "spark-app",
    master: Optional[str] = None,
    extra_conf: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    • 既存 SparkSession があれば再利用  
    • 無ければ spark-defaults.conf を基盤に新規作成  
      - master が未指定なら DEFAULT_MASTER を採用  
      - extra_conf で渡されたキーのみ既定を上書き
    """
    # ---- 既存セッションを再利用 ---------------------------------
    spark = SparkSession.getActiveSession()
    if spark:
        print("from existing SparkSession")
        return spark
    if "spark" in globals() and isinstance(globals()["spark"], SparkSession):
        print("from existing global SparkSession")
        return globals()["spark"]

    # ---- 新規作成 -----------------------------------------------
    builder = SparkSession.builder.appName(app_name)

    # spark.master の明示（未指定なら DEFAULT_MASTER）
    builder = builder.master(master or DEFAULT_MASTER)

    # 呼び出し側で上書きしたい conf があれば追加
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)
    print("created new SparkSession")
    return builder.getOrCreate()
