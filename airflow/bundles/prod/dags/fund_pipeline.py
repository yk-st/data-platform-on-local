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
from jobs.fund.config import FundConfig
from utils.dag_extended import dag_extended
from shared.utils import iceberg_dataset, s3_dataset

# DAG ファクトリー

def fund_pipeline():

    config = FundConfig()

    # Dataset 定義
    fund_daily_wide_ds = iceberg_dataset(config.TABLE_FUND_DAILY_WIDE, env=ENV)
    fund_master_ds = iceberg_dataset(config.TABLE_FUND_MASTER, env=ENV)
    fund_nav_ds = iceberg_dataset(config.TABLE_FUND_NAV, env=ENV)
    fund_dim_date_ds = iceberg_dataset(config.TABLE_DIM_DATE, env=ENV)
    fund_dim_fund_ds = iceberg_dataset(config.TABLE_DIM_FUND, env=ENV)
    fund_fct_fund_performance_ds = iceberg_dataset(config.TABLE_FCT_FUND_PERFORMANCE, env=ENV)

    fund_master =  s3_dataset(config.FUND_MASTER_S3_SOURCE, env=ENV)
    fund_nav = s3_dataset(config.FUND_NAV_S3_SOURCE, env=ENV)

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
        dag_id=f"fund_pipeline",
        default_args=default_args,
        schedule=None,
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
            "master_data": Param(
                default="v1",
                type="string",
                enum=["v1", "v2"],
                title="マスターデータのバージョン",
                description="書籍説明用のモード。V1がオリジナルのマスター。V2はマスターの一部を変更したもの（FND002の信託報酬の引き下げと, FND003の追加）",
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

        t_extract_fund_master = BranchSparkSubmitOperator.submit(
            task_id="extract_fund_master",
            script="jobs/fund/extract_fund_master.py",
            inlets=[fund_master],
            outlets=[fund_master_ds],
        )

        t_extract_fund_nav = BranchSparkSubmitOperator.submit(
            task_id="extract_fund_nav",
            script="jobs/fund/extract_fund_nav.py",
            inlets=[fund_nav],
            outlets=[fund_nav_ds],
        )

        t_star_schema = BranchSparkSubmitOperator.submit(
            task_id="t_star_schema",
            script="jobs/fund/transform_star_schema.py",
            inlets=[fund_daily_wide_ds],
            outlets=[fund_dim_fund_ds, fund_dim_date_ds, fund_fct_fund_performance_ds]
        )

        # TODO Analytic Base Viewベースで復活するといいかも
        # t_transform_fund = BranchSparkSubmitOperator.submit(
        #     task_id="t_transform_fund",
        #     script="jobs/fund/transform_wide_table.py",
        #     inlets=[fund_master_ds, fund_nav_ds],
        #     outlets=[fund_daily_wide_ds],
        # )

        # タスク依存関係の連結
        t_bootstrap_branch >> [t_extract_fund_master , t_extract_fund_nav] >> t_star_schema

    return pipeline()

# 環境ごとに DAG を生成して登録
fund_pipeline()
