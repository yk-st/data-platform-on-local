# -*- coding: utf-8 -*-
"""
Utilities for AVRO bytes → Spark Row conversion
・複数スキーマ（v3→v2→v1 …）を優先順位付きで試す。
・成功した行には schema_version を付与。
・すべて失敗した行は DLQ 用 DataFrame にまとめる。
 ABRiSを利用すれば、これらの処理を簡潔に実装することができますが依存が複雑になるので本書では見送りました。
"""

from pyspark.sql import functions as F
from typing import Iterable, List, Tuple
from pyspark.sql.avro.functions import from_avro

__all__ = ["decode_per_version"]


def decode_per_version(
    df,
    *,
    bytes_col: str,
    schemas: List[Tuple[int, str]],   # [(ver, schema_json), …]  ※降順
    key_field: str = "id",            # ← ここが not-null なら成功とみなす
    passthrough: Tuple[str, ...] = ("partition", "offset"),
    mode: str = "PERMISSIVE",
):
    """
    フォールバック付き Avro デコード。
    成功判定: payload.<key_field> が NOT NULL
    戻り値:
        decoded_dfs … List[DataFrame]  (成功したものだけ。順序＝schemas)
        dlq_df      … すべて失敗したレコード
    """

    remain = df.select(bytes_col, *passthrough)  # 必要列だけ残す
    decoded = []

    for ver, schema_json in schemas:
        with_payload = remain.withColumn(
            "payload",
            from_avro(F.col(bytes_col), schema_json, {"mode": mode})
        )

        ok = (
            with_payload
            .where(F.col(f"payload.{key_field}").isNotNull())
            .select("payload.*", bytes_col, *passthrough)
            .withColumn("schema_version", F.lit(ver))
        )
        decoded.append(ok)

        # まだ decode できていない行だけ次へ
        remain = (
            with_payload
            .where(F.col(f"payload.{key_field}").isNull())
            .select(bytes_col, *passthrough)
        )

    # --- どのバージョンでも解決できなかった行 = DLQ ---
    dlq_df = (
        remain.withColumn("schema_version", F.lit(None))
    )

    return decoded, dlq_df