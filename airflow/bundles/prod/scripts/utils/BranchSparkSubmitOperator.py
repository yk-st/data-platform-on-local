from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from pendulum import DateTime
import os, sys
from pathlib import Path
import inspect

SKIP_TASKS = ["bootstrap", "cleanup", "teardown"]

class BranchSparkSubmitOperator(SparkSubmitOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.env = None

    def get_ingest_date(self, context) -> str:
        """
        Asset-triggered Run でも一貫して YYYY-MM-DD を返す。
        1. 時間ベース Run なら logical_date
        2. Asset-triggered Run ならイベントの timestamp
        """
        # ① cron / timetable Run の場合
        logical_dt: DateTime | None = context.get("logical_date")
        if logical_dt:
            return logical_dt.format("YYYY-MM-DD")

        # ② asset-triggered Run の場合
        targ_events = context["triggering_asset_events"]
        # イベントを発行した Asset → AssetEvent の list が取れる
        for _, events in targ_events.items():
            if events: 
                ts: DateTime = events[0].timestamp
                # pendulumではない様子
                return ts.strftime("YYYY-MM-DD")


    def pre_execute(self, context):

        # 環境変数からブランチ名を取得
        print("params.branch--->", context.get('params', {}).get('branch', 'main'))

        # 親の pre_execute を呼ぶ
        super().pre_execute(context)
        # DAG params から branch を取得して一時保持
        print("pre_execute context->:", context)

        ### SPARK_CONF の設定 ###
        branch = None
        # 1) triggering_asset_events(airflow3系からtriggering_asset_eventsとなった。昔はtriggering_dataset_events) から優先的に取得
        events = context.get('triggering_asset_events', {})
        if events:  # triggering_asset_eventsが存在し、空でない場合
            for dataset, dataset_list in events.items():
                print("dataset_list[0]", dataset_list)
                branch = dataset_list[0].extra["branch"]
                break  # 最初のデータセットのbranchを取得したら終了

        # 2) 上記で取れなければ DAG params から取得
        if not branch:
            print("Fallback to params for branch extraction")
            branch = context.get('params', {}).get('branch', 'main')

        # 3) 取得した branch を保持 or エラー
        if not branch:
            raise ValueError("Branch parameter is not provided in params or triggering_asset_events.")

        self.env = branch

        # mainの場合は、データのブランチなどを切る必要がないのでSkipする
        if self.env == "main" and any(k in self.task_id for k in SKIP_TASKS):
            self.log.info("Skipping %s in main environment", self.task_id)
            raise AirflowSkipException(f"{self.task_id} skipped in main")

        #### APPLICATION ARGS の設定 ####
        base_args = list(self.application_args)

        # 2) コンテキストから動的に取り出す値
        logical_date     = self.get_ingest_date(context)
        run_id = context["run_id"]               # Run の ID
        dag_id = context["dag"].dag_id
        e2e_mode = context.get('params', {}).get('e2e_mode', 'BRANCH')
        extract_mode = context.get('params', {}).get('extract_mode', 'logical_date')
        master_data = context.get('params', {}).get('master_data', 'v1')

        # 3) 動的引数を作成して結合
        dyn_args = [
            "--ingest-date", logical_date,
            "--run-id",      run_id,
            "--env",         self.env,
            "--e2e-mode",   e2e_mode,
            "--extract-mode", extract_mode,
            "--master-data", master_data
        ]
        self.application_args = base_args + dyn_args

        conf = self.conf or {}
        # Base conf injection
        # if self.env != 'main' and not any(k in self.task_id for k in SKIP_TASKS):
        conf.update({
            f"spark.wap.enabled": "true",
            f"spark.wap.branch": f"{self.env }",
            #f"spark.sql.iceberg.snapshot-ref": self.env
        })
        print("ブランチはこれです:", self.env)

        ### Openlineage の設定 ###
        # mainのみ OpenLineage の設定を行う
        if self.env == "main":
            conf.update({
                "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
                "spark.openlineage.parentJobNamespace": f"{dag_id}",
                "spark.openlineage.parentJobName": f"{self.task_id}",
                "spark.openlineage.parentRunId": f"{run_id}"
            })
        else:
            conf.update({
                "spark.extraListeners": "",
                "spark.openlineage.transport.type": "console"
            })

        # sparkのconfを設定**spark_kwargsは展開済みなので、展開したものを再度設定する
        self.conf = conf

    def post_execute(self, context, result=None):
        super().post_execute(context, result)
        print("post_execute context->:", context)

        # 全てのoutletsに対してbranchを注入
        outlet_events = context.get("outlet_events", {})

        if self.outlets:
            for outlet in self.outlets:
                if outlet in outlet_events:
                    outlet_events[outlet].extra = {"branch": self.env}
                    print(f"inject ds name -> {outlet_events[outlet].extra}")

        print(f"branch_param injected into outlet events: {self.env}")

    @classmethod
    def submit(SubmitOperatorClass,
               task_id,
               script,
               inlets: list[Dataset] =[],
               outlets: list[Dataset] =[],
               application_args: list = [],
               **spark_kwargs
        ):
        print("task_id---->", task_id)

        env = SubmitOperatorClass._detect_env()              # ← prod / dev をその場で判定
        py_gz        = f"/opt/airflow/{env}_scripts.zip"
        script_path  = f"/opt/airflow/bundles/{env}/scripts"

        return SubmitOperatorClass(
            task_id=task_id,
            application=os.path.join(script_path, script),
            conn_id="spark_con",
            name=task_id,
            application_args=application_args,
            py_files=py_gz,
            inlets=inlets,                # ここに inlet 対象の Dataset
            outlets=outlets,            # ここに outlet 対象の Dataset
            trigger_rule=TriggerRule.NONE_FAILED,  # スキップされた場合は後続のタスクを実行する
            **spark_kwargs              # conf, total_executor_cores, executor_memory, etc.....
        )
    
    @staticmethod
    def _detect_env() -> str:
        """
        呼び出し元 DAG ファイルのパスから bundles/<env> を抽出して返す
        """
        for frame in inspect.stack():
            p = Path(frame.filename).resolve()
            if "bundles" in p.parts:
                return p.parts[p.parts.index("bundles") + 1]   # prod / dev
        return "dev"  # フォールバック