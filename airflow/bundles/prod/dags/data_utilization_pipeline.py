import os
import sys
from datetime import timedelta
import pendulum

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models import Variable, Param

from pathlib import Path
cur = Path(__file__).resolve()
ENV = cur.parts[cur.parts.index("bundles") + 1]
SCRIPT_PATH = f"/opt/airflow/bundles/{ENV}/scripts"

if SCRIPT_PATH not in sys.path:
    sys.path.insert(0, SCRIPT_PATH)

from utils.BranchSparkSubmitOperator import BranchSparkSubmitOperator
from jobs.utilize.config import UtilizeConfig
from utils.dag_extended import dag_extended
from shared.utils import iceberg_dataset, mongodb_dataset

# DAG ファクトリー

def make_data_utilize_pipeline():
    config = UtilizeConfig()

    # Dataset 定義
    wide_weather_enriched_ds = iceberg_dataset(config.TABLE_USER_ORDERS_WIDE_WEATHER_ENRICHED, env=ENV)
    user_ctx_feature_ds = iceberg_dataset(config.TABLE_USER_CTX_FEATURE, env=ENV)
    user_ctx_ds = mongodb_dataset(config.TABLE_USER_CTX_MONGO, env=ENV)

    default_args = {
        'owner': 'yuki',
        'start_date': pendulum.datetime(2025, 5, 22, tz="Asia/Tokyo"),
        'retries': 0,
        'retry_exponential_backoff': True,
        'retry_delay': timedelta(minutes=5),
        'max_retry_delay': timedelta(hours=1),
    }

    branch_default = ENV if ENV != "prod" else "main"

    @dag_extended(
        dag_id=f"data_utilize_pipeline",
        default_args=default_args,
        schedule=[wide_weather_enriched_ds],
        catchup=False,
        dagrun_timeout=timedelta(hours=2),
        tags=["etl"],
        params={
            "branch": Param(
                default=branch_default,
                type="string",
                description="実行ブランチ名（main/feature-xxx など）"
            ),
            "e2e_mode": Param(
                default="BRANCH",
                type="string",
                enum=["BRANCH", "ZERO_COPY"],
                title="実行モード",
                description="BRANCH: 通常のブランチテスト(mainの場合はmainブランチで実行), ZERO_COPY: ZERO_COPYテスト"
            ),
        },
        render_template_as_native_obj=True
    )
    def pipeline():

        # 3️⃣ ブートストラップ: Snapshot/Branch 作成
        t_bootstrap_branch = BranchSparkSubmitOperator.submit(
            task_id="bootstrap_branch",
            script="utils/bootstrap_branch.py",
            application_args=[
                # "--catalog", Variable.get("CATALOG", config.CATALOG_NAME),
                "--tables", config.get_iceberg_tables_string(),
            ],
        )

        t_merge_user_context = BranchSparkSubmitOperator.submit(
            task_id="merge_user_context",
            script="jobs/utilize/merge_user_context.py",
            inlets=[wide_weather_enriched_ds],
            outlets=[user_ctx_feature_ds],
        )

        # pyspark --packages org.mongodb.spark:mongo-spark-connector_2.13:10.5.0 \
        #         --conf spark.mongodb.read.connection.uri=mongodb://action:pass123@mongo.local.datasource:27017/user_prediction \
        #         --conf spark.mongodb.write.connection.uri=mongodb://action:pass123@mongo.local.datasource:27017/user_prediction


        t_write_user_ctx_mongo = BranchSparkSubmitOperator.submit(
            task_id="write_user_ctx_mongo",
            script="jobs/utilize/write_user_ctx_mongo.py",
            inlets=[user_ctx_feature_ds],  # wide_ds の Dataset イベントをトリガーとして使用
            outlets=[user_ctx_ds],
            #packages="org.mongodb.spark:mongo-spark-connector_2.13:10.5.0",
            conf={
                'spark.mongodb.read.connection.uri': f'mongodb://action:pass123@host.docker.internal:27017/{config.MONGO_DB}',
                'spark.mongodb.write.connection.uri': f'mongodb://action:pass123@host.docker.internal:27017/{config.MONGO_DB}',
            },
        )

        # タスク依存関係の連結
        t_bootstrap_branch >> t_merge_user_context >> t_write_user_ctx_mongo

    return pipeline()

# 環境ごとに DAG を生成して登録
make_data_utilize_pipeline()
