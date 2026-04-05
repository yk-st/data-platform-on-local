import os
import sys
from datetime import timedelta
import pendulum
from airflow.models import Variable, Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from pathlib import Path
cur = Path(__file__).resolve()
ENV = cur.parts[cur.parts.index("bundles") + 1]
SCRIPT_PATH = f"/opt/airflow/bundles/{ENV}/scripts"

if SCRIPT_PATH not in sys.path:
    sys.path.insert(0, SCRIPT_PATH)

from utils.BranchSparkSubmitOperator import BranchSparkSubmitOperator
from jobs.init.config import IntConfig
from utils.dag_extended import dag_extended
from shared.utils import iceberg_dataset, s3_dataset

branch_default = ENV if ENV != "prod" else "main"

# ── INIT DAG ──
@dag_extended(
    dag_id="init_pipeline",
    start_date=pendulum.datetime(2025, 5, 22, tz="Asia/Tokyo"),
    schedule=None,  # 年次スケジュール
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
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
    tags=["etl"],
)
def init_pipeline():
    config = IntConfig()

    zip_geocode_ds = iceberg_dataset(config.TABLE_ZIP_GEOCODE, env=ENV)
    age_group_ds = iceberg_dataset(config.TABLE_AGE_GROUP_REF, env=ENV)
    column_alias_map_ds = iceberg_dataset(config.TABLE_COLUMN_ALIAS_MAP, env=ENV)
    d_source_zip_geocode = s3_dataset(config.ZIP_GEOCODE_S3_SOURCE, env=ENV)
    d_source_age_group = s3_dataset(config.AGE_GROUP_REF_S3_SOURCE, env=ENV)

    # ブートストラップ: Namespace 作成
    # t_bootstrap_namespace = BranchSparkSubmitOperator.submit(
    #     task_id="bootstrap_namespace",
    #     script="utils/bootstrap_namespace.py",
    #     application_args=[
    #         "--catalog", Variable.get("CATALOG", config.CATALOG_NAME),
    #         "--namespaces", config.get_namespaces_string()
    #     ],
    # )

    create_ddl = TriggerDagRunOperator(
        task_id="create_ddl",
        trigger_dag_id="create_ddl",  # 呼び出したいDAGのID
        wait_for_completion=True  # True にすると A の完了は B の完了まで待つ
    )


    # 3️⃣ ブートストラップ: Snapshot/Branch 作成
    # t_bootstrap_branch = BranchSparkSubmitOperator.submit(
    #     task_id="bootstrap_branch",
    #     script="utils/bootstrap_branch.py",
    #     application_args=[
    #         # "--catalog", Variable.get("CATALOG", config.CATALOG_NAME),
    #         "--tables", config.get_iceberg_tables_string(),
    #     ],
    # )

    # タスク定義
    t_age_group = BranchSparkSubmitOperator.submit(
        task_id="create_age_group",
        script="jobs/init/age_group_ref.py",
        # inlets=[d_source_age_group],
        # outlets=[age_group_ds],
        application_args=[
            # "--catalog", Variable.get("CATALOG", config.CATALOG_NAME),
            # "--namespaces", config.get_namespaces_string()
            # "--tables", config.get_tables_string(),  # カンマ区切りのテーブル名
        ],
    )

    t_column_alias_map = BranchSparkSubmitOperator.submit(
        task_id="create_column_alias_map",
        script="jobs/init/column_alias_map.py",
        outlets=[column_alias_map_ds],
        application_args=[
            # "--catalog", Variable.get("CATALOG", config.CATALOG_NAME),
            # "--namespaces", config.get_namespaces_string()
            # "--tables", config.get_tables_string(),  # カンマ区切りのテーブル名
        ],
    )

    t_zip_geocode = BranchSparkSubmitOperator.submit(
        task_id="create_zip_geocode",
        script="jobs/init/zip_geocode.py",
        # inlets=[d_source_zip_geocode],
        # outlets=[zip_geocode_ds],
        application_args=[
            # "--catalog", Variable.get("CATALOG", config.CATALOG_NAME),
            # "--namespaces", config.get_namespaces_string()
            # "--tables", config.get_tables_string(),  # カンマ区切りのテーブル名
        ],
    )

    # # parquet形式なので、catalog等は不要
    # t_legacy_orders = BranchSparkSubmitOperator.submit(
    #     task_id="create_legacy_orders",
    #     script="jobs/init/legacy_orders.py"
    # )

    # parquet形式なので、catalog等は不要
    t_legacy_fund_master = BranchSparkSubmitOperator.submit(
        task_id="create_legacy_fund_master",
        script="jobs/init/legacy_fund_master.py"
    )

    # タスク依存関係
    create_ddl  >> [t_age_group , t_zip_geocode, t_column_alias_map] >> t_legacy_fund_master

# DAG オブジェクト生成
init = init_pipeline()
