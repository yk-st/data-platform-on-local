import os
import sys
from datetime import timedelta
import pendulum

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models import Variable, Param

# パス設定
from pathlib import Path
cur = Path(__file__).resolve()
ENV = cur.parts[cur.parts.index("bundles") + 1]
SCRIPT_PATH = f"/opt/airflow/bundles/{ENV}/scripts"

if SCRIPT_PATH not in sys.path:
    sys.path.insert(0, SCRIPT_PATH)

from utils.BranchSparkSubmitOperator import BranchSparkSubmitOperator
from jobs.streaming.config import StreamingPipelineConfig
from utils.dag_extended import dag_extended
from shared.utils import iceberg_dataset

# DAG ファクトリー

def make_enrich_pipeline():
    config = StreamingPipelineConfig()

    default_args = {
        'owner': 'yuki',
        'start_date': pendulum.datetime(2025, 5, 22, tz="Asia/Tokyo"),
        'retries': 0,
        'retry_exponential_backoff': True,
        'retry_delay': timedelta(minutes=5),
        'max_retry_delay': timedelta(hours=1),
    }

    # ブランチング等は対応なし
    @dag_extended(
        dag_id=f"streaming_pipeline",
        default_args=default_args,
        # schedule_interval="0 1 * * *",
        # catchup=False,
        # start_date=days_ago(1),
        dagrun_timeout=timedelta(hours=2),
        tags=["etl", "streaming"],
        params={
        },
        render_template_as_native_obj=True
    )
    def pipeline():

        t_daily_user_ranking = BranchSparkSubmitOperator.submit(
            task_id="transform_daily_user_ranking",
            script="jobs/streaming/daily_user_ranking.py",
            conf={
                'spark.mongodb.read.connection.uri': f'mongodb://action:pass123@mongo.local.datasource:27017/user_data',
                'spark.mongodb.write.connection.uri': f'mongodb://action:pass123@mongo.local.datasource:27017/user_data',
            },
        )


        # タスク依存関係の連結
        t_daily_user_ranking

    return pipeline()

# 環境ごとに DAG を生成して登録
make_enrich_pipeline()