import os
import sys
from datetime import timedelta
import pendulum
from airflow.decorators import dag

from airflow.operators.bash import BashOperator


# ── INIT DAG ──
@dag(
    dag_id="drop_ddl",
    start_date=pendulum.datetime(2025, 5, 22, tz="Asia/Tokyo"),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    tags=["etl"],
)

def ddl_pipeline():
    drop_namespace_task = BashOperator(
        task_id='drop_namespace',
        bash_command="""
            cd /opt/airflow/bundles/prod/scripts/jobs/ddl
            chmod +x drop_prod_namespace.sh
            ./drop_prod_namespace.sh
        """,
    )
    drop_namespace_task


# DAG オブジェクト生成
ddl = ddl_pipeline()
