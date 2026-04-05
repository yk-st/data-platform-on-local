import os
import sys
from datetime import timedelta
from pendulum import timezone
import pendulum

from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.models import Variable, Param

from shared.utils import iceberg_dataset

from pathlib import Path
cur = Path(__file__).resolve()
ENV = cur.parts[cur.parts.index("bundles") + 1]
SCRIPT_PATH = f"/opt/airflow/bundles/{ENV}/scripts"

if SCRIPT_PATH not in sys.path:
    sys.path.insert(0, SCRIPT_PATH)

from jobs.basic_pipeline.config import BasicPipelineConfig
from utils.BranchSparkSubmitOperator import BranchSparkSubmitOperator
from utils.dag_extended import dag_extended

JST = timezone("Asia/Tokyo")

def make_basic_pipeline():
    config = BasicPipelineConfig()

    orders_ds   = iceberg_dataset(config.TABLE_ORDERS, ENV)
    products_ds = iceberg_dataset(config.TABLE_PRODUCTS, ENV)
    users_ds   = iceberg_dataset(config.TABLE_USERS, ENV)
    wide_ds     = iceberg_dataset(config.TABLE_USER_ORDERS_WIDE, ENV)
    sales_ds    = iceberg_dataset(config.TABLE_USER_SALES, ENV)

    default_args = {
        'owner': 'yuki',
        'start_date': pendulum.datetime(2025, 5, 22, tz="Asia/Tokyo"),
        'retries': 0,
        'retry_exponential_backoff': True,
        'retry_delay': timedelta(minutes=5),
        'max_retry_delay': timedelta(hours=1),
    }

    branch_default = ENV if ENV != "prod" else "main"

    # dag_run_conf_overrides_params = Trueになっている必要がある
    @dag_extended(
        dag_id=f"basic_pipeline",
        default_args=default_args,
        schedule=None,
        catchup=False,
        dagrun_timeout=timedelta(hours=2),
        tags=["etl"],
        params={
            "extract_mode": Param(
                default="logical_date", 
                type="string",
                enum=["logical_date", "ALL"],
                description="抽出モード（logical_dateを基準に抽出, ALL: 全件抽出）"
            ),
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

        print(f"cur------------->: {cur}")

        # 3️⃣ ブートストラップ: Snapshot/Branch 作成
        t_bootstrap_branch = BranchSparkSubmitOperator.submit(
            task_id="bootstrap_branch",
            script="utils/bootstrap_branch.py",
            application_args=[
                # "--catalog", Variable.get("CATALOG", config.CATALOG_NAME),
                "--tables", config.get_iceberg_tables_string(),
            ],
        )

        # 4️⃣ データ抽出タスク
        t_orders  = BranchSparkSubmitOperator.submit(
            task_id="extract_orders",
            script="jobs/basic_pipeline/extract_orders.py",
            outlets=[orders_ds],
        )
        t_products = BranchSparkSubmitOperator.submit(
            task_id="extract_products",
            script="jobs/basic_pipeline/extract_products.py",
            outlets=[products_ds],
        )
        t_users  = BranchSparkSubmitOperator.submit(
            task_id="extract_users",
            script="jobs/basic_pipeline/extract_users.py",
            outlets=[users_ds],
        )

        # 5️⃣ Wide テーブル変換
        t_wide = BranchSparkSubmitOperator.submit(
            task_id="transform_wide_table",
            script="jobs/basic_pipeline/transform_wide_table.py",
            inlets=[
                orders_ds,
                products_ds,
                users_ds,
            ],
            outlets=[wide_ds],
            # conf={
            #     "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
            #     "spark.openlineage.parentJobNamespace": "{{ dag.dag_id }}",
            #     "spark.openlineage.parentJobName": "{{ task.task_id }}",
            #     "spark.openlineage.parentRunId": "{{ run_id }}"
            # },
        )

        t_star = BranchSparkSubmitOperator.submit(
            task_id="transform_star_schema_table",
            script="jobs/basic_pipeline/transform_star_schema_table.py",
            inlets=[
                orders_ds,
                products_ds,
                users_ds,
            ],
            outlets=[wide_ds],
        )

        # 6️⃣ 集計タスク
        t_aggregate = BranchSparkSubmitOperator.submit(
            task_id="aggregate_user_sales",
            script="jobs/basic_pipeline/aggregate_user_sales.py",
            inlets=[wide_ds],
            outlets=[sales_ds],
        )
    
        # 7️⃣ クリーンアップタスク
        # t_cleanup = BranchSparkSubmitOperator.submit(
        #     task_id="cleanup",
        #     script="utils/teardown_cleanup.py",
        #     application_args=[
        #         # "--catalog", Variable.get("CATALOG", config.CATALOG_NAME),
        #         # "--tables", ",".join(config.TABLES),
        #     ],
        # )

        # タスク依存関係
        t_bootstrap_branch >> [t_orders, t_products, t_users]

        # 2. 抽出完了後にtransformタスクを並列実行
        [t_orders, t_products, t_users] >> t_wide
        [t_orders, t_products, t_users] >> t_star

        # 3. t_wideの完了後にaggregateを実行
        t_wide >> t_aggregate
        # >> t_cleanup


    return pipeline()

# 環境ごとに DAG を生成して登録

make_basic_pipeline()
