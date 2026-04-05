# teardown_cleanup.py
"""
指定テーブルの一時ブランチを削除し、一定の安全バッファ後に
孤児ファイルを Iceberg remove_orphan_files でクリーンアップする。
"""

import re, sys, argparse
from datetime import datetime, timedelta
from spark_utils import get_spark
from pyspark.sql import SparkSession

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Iceberg ブランチ破棄 + orphan cleanup")
    p.add_argument("-c", "--catalog", required=True, help="Iceberg カタログ")
    p.add_argument("-b", "--branch",  required=True, help="削除するブランチ名")
    p.add_argument("-t", "--table",   action="append", metavar="NS.TBL",
                   help="対象テーブル (複数可)")
    p.add_argument("-T", "--tables",  metavar="LIST",
                   help="カンマ or 空白区切りのテーブル一覧")
    p.add_argument("--buffer-minutes", type=int, default=5,
                   help="ブランチ削除から孤児判定までの安全バッファ (分)")
    return p.parse_args()

# ---------- 主要処理 ----------
def teardown(
    spark: SparkSession,
    catalog: str,
    branch: str,
    tables: list[tuple[str, str]],
    buffer_minutes: int
) -> None:

    older_than_ts = (
        datetime.utcnow() - timedelta(minutes=buffer_minutes)
    ).strftime("%Y-%m-%d %H:%M:%S")

    for ns, tbl in tables:
        fqn = f"{catalog}.{ns}.{tbl}"

        # 1) ブランチを削除
        spark.sql(f"""
            ALTER TABLE {fqn}
              DROP BRANCH IF EXISTS {branch}
        """)
        print(f"✔ dropped branch {branch} on {fqn}")

        # 2) 安全バッファ後より古い孤児を削除
        spark.sql(f"""
            CALL {catalog}.system.remove_orphan_files(
              table      => '{ns}.{tbl}',
              older_than => TIMESTAMP '{older_than_ts}'
            )
        """)
        print(f"✔ orphan files removed on {fqn} (older_than = {older_than_ts} UTC)")

        # TODO 3種の神器は常に実行するように設定しておかなければならない
        # 3) TODO: 3種の神器を実行 (snapshot, metadata, manifest)の掃除プロシージャー
        
# ---------- エントリポイント ----------
if __name__ == "__main__":
    args = parse_args()
    spark = get_spark()

    # テーブル一覧を整理
    tables: list[tuple[str, str]] = []
    if args.tables:
        tables += [tuple(raw.split(".", 1)) for raw in re.split(r"[,\s]+", args.tables.strip()) if raw]
    if args.table:
        tables += [tuple(raw.split(".", 1)) for raw in args.table]

    if not tables:
        sys.exit("ERROR: 対象テーブルが指定されていません。")

    teardown(
        spark,
        catalog=args.catalog,
        branch=args.branch,
        tables=tables,
        buffer_minutes=args.buffer_minutes
    )
