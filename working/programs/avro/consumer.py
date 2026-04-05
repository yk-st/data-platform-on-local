#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Structured Streaming で Kafka-Avro を multi-schema decode するサンプル
  * Schema Registry から REST で v3→v2→v1 のスキーマ JSON を取得
  * schema_utils.decode_per_version() で段階的にデコード
  * 成功行は Union、失敗行は DLQ へ
"""

from pyspark.sql import functions as F
import json, urllib.request, urllib.error

from spark_utils   import get_spark
from schema_utils  import decode_per_version

# ──────────────── 設定 ───────────────────────────────────────
BOOTSTRAP  = "kafka2.local.data.platform:9093"
TOPIC      = "orders_topic_avro"
SUBJECT    = f"{TOPIC}-value"
REGISTRY   = "http://schema-registry.local.data.platform:8081"

# Kafka (SASL/PLAIN)
KAFKA_USER = "admin"
KAFKA_PASS = "admin"

# デコード対象バージョン（優先順に並べる）
#VERSIONS   = (3, 2, 1)
VERSIONS   = (2, 1)

# ──────────────── Helpers ────────────────────────────────────
def fetch_schema(subject: str, version: int, registry: str) -> str:
    """Schema Registry REST から schema JSON (str) を取得"""
    url = f"{registry.rstrip('/')}/subjects/{subject}/versions/{version}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["schema"]
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Failed to GET {url} – {e.code} {e.reason}") from e


def load_schemas():
    schemas = []
    for v in VERSIONS:
        print(f"Fetching {SUBJECT} v{v} …")
        schemas.append((v, fetch_schema(SUBJECT, v, REGISTRY)))
    return schemas


# ──────────────── SparkSession ───────────────────────────────
spark = get_spark(app_name="OrdersAvroMultiSchema")

# ──────────────── Kafka readStream ───────────────────────────
kafka_df = (
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP)
      .option("subscribe", TOPIC)
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("kafka.sasl.mechanism",    "PLAIN")
      .option(
          "kafka.sasl.jaas.config",
          f'org.apache.kafka.common.security.plain.PlainLoginModule '
          f'required username="{KAFKA_USER}" password="{KAFKA_PASS}";',
      )
      .load()
)

# Confluent Wire-format: 5-byte ヘッダーを除去
trimmed = kafka_df.select(
    F.expr("substring(value, 6, length(value) - 5)").alias("avro_bytes"),
    "partition", "offset",
)

# ──────────────── Decode per version ─────────────────────────
schemas = load_schemas()

decoded_dfs, dlq_df = decode_per_version(
    df        = trimmed,
    bytes_col = "avro_bytes",
    schemas   = schemas,      # [(3,json3), (2,json2), (1,json1)]
    key_field = "id",         # ← v1〜v3 共通で必ず入っている列
    mode      = "PERMISSIVE",
)

# 結合（列増減を自動調整）
from functools import reduce
good_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    decoded_dfs,
)

good_df = good_df.withColumn("ingest_ts", F.current_timestamp()).drop("avro_bytes")
dlq_df  = dlq_df .withColumn("ingest_ts", F.current_timestamp())

# --- sink はお好みで ---
good_q = (good_df.writeStream.format("console")
                     .option("truncate", False)
                     .start())
dlq_q  = (dlq_df .writeStream.format("console")
                     .option("truncate", False)
                     .start())

spark.streams.awaitAnyTermination()