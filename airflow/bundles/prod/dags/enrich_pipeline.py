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
from jobs.enrich.config import EnrichConfig
from utils.dag_extended import dag_extended
from shared.utils import iceberg_dataset

# DAG ファクトリー

def make_enrich_pipeline():
    config = EnrichConfig()

    # Dataset 定義
    wide_ds = iceberg_dataset(config.TABLE_USER_ORDERS_WIDE, env=ENV)
    wide_enriched_ds = iceberg_dataset(config.TABLE_USER_ORDERS_WIDE_ENRICHED, env=ENV)
    wide_weather_enriched_ds = iceberg_dataset(config.TABLE_USER_ORDERS_WIDE_WEATHER_ENRICHED, env=ENV)
    zip_geocode_ds = iceberg_dataset(config.TABLE_ZIP_GEOCODE, env=ENV)

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
        dag_id=f"enrich_pipeline",
        default_args=default_args,
        schedule=[wide_ds],
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

        t_wide_table_enriched = BranchSparkSubmitOperator.submit(
            task_id="transform_wide_table_enriched",
            script="jobs/enrich/transform_wide_table_enrich.py",
            # inlets=[wide_ds, zip_geocode_ds],
            outlets=[wide_enriched_ds],
        )

        t_wide_table_weather_enriched = BranchSparkSubmitOperator.submit(
            task_id="transform_wide_table_weather_enriched",
            script="jobs/enrich/transform_weather_enrich.py",
            # inlets=[wide_enriched_ds],
            outlets=[wide_weather_enriched_ds],
        )

        # タスク依存関係の連結
        t_bootstrap_branch >> t_wide_table_enriched >> t_wide_table_weather_enriched

    return pipeline()

# 環境ごとに DAG を生成して登録
make_enrich_pipeline()
