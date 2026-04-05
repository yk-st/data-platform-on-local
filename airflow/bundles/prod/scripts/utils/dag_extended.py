import inspect
import os
from pathlib import Path
from airflow.decorators import dag as _dag
from airflow.models.param import Param

def on_failure(context):
    # 失敗時の共通処理(Slack通知など)をここに実装
    # contextにはタスクの情報が含まれているため取り出して通知の内容を作成できる
    pass

def dag_extended(*dargs, **dkwargs):
    # ① デフォルトの on_failure_callback をセット
    dkwargs.setdefault("on_failure_callback", on_failure)

    # ② 呼び出し元ファイルのパスを取得
    caller_file = Path(inspect.stack()[1].filename).resolve()
    ENV         = caller_file.parents[1].name  

    # ④ dev 環境なら schedule と paused を強制設定（上書き）
    if ENV != "prod":
        # 明示的に dev は「手動実行のみ」「初期状態は Pause」
        # dkwargs["schedule"] = None  # dev 環境では手動実行のみ
        dkwargs["is_paused_upon_creation"] = True
        dkwargs["catchup"] = False

    # ────────────────────────────────
    # 4) TAGS に ENV を自動付与
    # ────────────────────────────────
    # 既存 tags が無ければ空リスト、tuple なら list に変換して拡張
    tags = list(dkwargs.get("tags", []))
    if ENV not in tags:
        tags.append(ENV)
    dkwargs["tags"] = tags

    # ────────────────────────────────
    # 3) dag_id を自動拡張
    # ────────────────────────────────
    if "dag_id" in dkwargs:
        base_id = dkwargs["dag_id"]
    elif dargs:
        base_id = dargs[0]
        dargs   = dargs[1:]
    else:
        raise ValueError("dag_id が指定されていません")

    # すでに _<ENV> が付いていなければ追加
    if not base_id.endswith(f"_{ENV}"):
        base_id = f"{base_id}_{ENV}"
    dkwargs["dag_id"] = base_id

    # ⑤ 最後に元の @dag を呼び出す
    return _dag(*dargs, **dkwargs)