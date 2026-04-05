import os
import sys
from datetime import timedelta
import pendulum
from airflow.decorators import dag

from airflow.operators.bash import BashOperator


# ── INIT DAG ──
@dag(
    dag_id="create_ddl",
    start_date=pendulum.datetime(2025, 5, 22, tz="Asia/Tokyo"),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    tags=["operation"],
)

def ddl_pipeline():
    create_namespace_task = BashOperator(
        task_id='create_namespace',
        bash_command="""
            cd /opt/airflow/bundles/prod/scripts/jobs/ddl
            chmod +x create_ddl.sh
            ./create_ddl.sh
        """,
    )
    create_namespace_task

# DAG オブジェクト生成
ddl = ddl_pipeline()
