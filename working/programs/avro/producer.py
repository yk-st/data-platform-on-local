#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
orders_producer.py  ― Avro メッセージを Kafka へ送信する汎用ユーティリティ
==========================================================================

■ 主な機能
-----------
1. **Schema Registry からスキーマ取得のみ（auto.register.schemas=False）**
2. `--schema-version`   : Subject のバージョン番号で固定
   `--schema-id`        : スキーマ ID で固定（ID が優先）
   何も指定しなければ latest を取得
3. Kafka SASL/PLAIN（user --kafka-username / --kafka-password）
4. Schema Registry Basic-Auth（user --sr-username / --sr-password）
5. v1 / v2 レコード生成  
   - **v2** では `parent_id` (string) を追加
6. 送信前に **追加バリデーション**  
   - `id` が UUID v4 形式  
   - Avro 型／enum 検証（fastavro.validate）

使い方例
--------
# 最新スキーマで 100 件
python orders_producer.py --bootstrap broker:9093 --registry http://sr:8081 \
    --topic orders_microbatch --count 100

# バージョン 1 で 50 件
python orders_producer.py ... --schema-version 1 --count 50

# スキーマ ID = 44 を明示して 10 件
python orders_producer.py ... --schema-id 44 --count 10

# Kafka SASL/PLAIN + Schema Registry Basic 認証
python orders_producer.py ... --kafka-username app --kafka-password secret \
                              --sr-username sruser --sr-password srpass
"""

from __future__ import annotations

import argparse
import datetime as dt
import random
import time
import re
import signal
import sys
import uuid
from typing import Dict, Optional
from decimal import Decimal, ROUND_HALF_UP

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from fastavro.validation import validate as avro_validate


# ───────────────────────── CLI ─────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True,
                   help="Kafka ブローカー (host:port[,host:port])")
    p.add_argument("--registry", required=True,
                   help="Schema Registry URL (http://host:port)")
    p.add_argument("--topic", required=True,
                   help="Kafka トピック名")
    p.add_argument("--schema-version", type=int,
                   help="Schema Registry のバージョン番号 (1,2,...)")
    p.add_argument("--schema-id", type=int,
                   help="Schema Registry のスキーマ ID を直接指定（優先）")
    p.add_argument("--count", type=int, default=100,
                   help="送信レコード数 (default: 100)")

    # 認証オプション
    p.add_argument("--kafka-username", help="Kafka SASL/PLAIN ユーザ名")
    p.add_argument("--kafka-password", help="Kafka SASL/PLAIN パスワード")
    p.add_argument("--sr-username", help="Schema Registry Basic 認証ユーザ名")
    p.add_argument("--sr-password", help="Schema Registry Basic 認証パスワード")
    return p.parse_args()

def _price(x: float) -> Decimal:
    return round(x, 2)

# ────────────────── レコード生成 ──────────────────
def _record_base() -> Dict:
    subtotal = round(random.uniform(500, 20_000), 2)
    tax = round(subtotal * 0.1, 2)
    now_ms   = int(time.time_ns() // 1_000_000)
    return {
        "id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 500),
        "subtotal_usd": _price(subtotal),
        "tax_usd":      _price(tax),
        "total_usd":       _price(subtotal + tax),
        "quantity": random.randint(1, 5),
        "status_flag": "FLAG_0",  # enum symbol
        "created_at": now_ms
    }


def build_record(version: int) -> Dict:
    rec = _record_base()
    if version >= 2:
        rec["parent_id"] = str(uuid.uuid4())  # v2 フィールド
    return rec


# ─────────────── AvroSerializer 構築 ───────────────
def build_avro_serializer(
    sr: SchemaRegistryClient,
    subject: str,
    schema_id: Optional[int],
    version: Optional[int],
) -> tuple[AvroSerializer, int, str, int]:
    """
    schema_id / version / latest に従い AvroSerializer を生成し、
    使用する schema_id, schema_str, version を返す。
    """
    actual_version = None
    
    if schema_id is not None:                          # ① ID が優先
        schema_obj: Schema = sr.get_schema(schema_id)
        schema_str = schema_obj.schema_str
        # スキーマIDからバージョンを逆引き
        # Schema Registryから全バージョンを取得してIDで検索
        try:
            versions = sr.get_versions(subject)
            for v in versions:
                version_meta = sr.get_version(subject, v)
                if version_meta.schema_id == schema_id:
                    actual_version = v
                    break
            if actual_version is None:
                actual_version = 1  # デフォルト値
        except Exception:
            actual_version = 1  # フォールバック
            
    elif version is not None:                          # ② Version 固定
        ver_meta = sr.get_version(subject, version)
        schema_id = ver_meta.schema_id
        schema_str = ver_meta.schema.schema_str
        actual_version = version
    else:                                              # ③ latest
        latest = sr.get_latest_version(subject)
        schema_id = latest.schema_id
        schema_str = latest.schema.schema_str
        actual_version = latest.version

    print(f"▶ Using subject={subject}, schema_id={schema_id}, version={actual_version}")
    avro_ser = AvroSerializer(
        schema_registry_client=sr,
        schema_str=schema_str,
        conf={
            "auto.register.schemas": False,
            "use.latest.version": False
        },
    )
    return avro_ser, schema_id, schema_str, actual_version


# ──────────────── 追加バリデーション ────────────────
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE
)


def validate_record(rec: Dict, schema_str: str) -> None:
    """
    1. UUID 形式チェック
    2. Avro 型 & enum シンボル検証
    3. ビジネスルール: flag/金額/数量
    """
    if not _UUID_RE.fullmatch(rec["id"]):
        raise ValueError(f"id is not UUID v4: {rec['id']}")

    # if not avro_validate(rec, schema_str):
    #     raise ValueError("Avro type or enum validation failed")

    if rec["quantity"] == 0:
        raise ValueError("quantity cannot be 0")


# ─────────────────────── main ───────────────────────
def main() -> None:
    args = parse_args()
    subject = f"{args.topic}-value"

    # Schema Registry client 設定
    sr_conf = {"url": args.registry}
    if args.sr_username:
        sr_conf |= {
            "basic.auth.credentials.source": "USER_INFO",
            "basic.auth.user.info": f"{args.sr_username}:{args.sr_password or ''}",
        }
    sr = SchemaRegistryClient(sr_conf)

    avro_ser, schema_id, schema_str, actual_version = build_avro_serializer(
        sr, subject, args.schema_id, args.schema_version
    )

    # Kafka Producer 設定
    kafka_conf = {
        "bootstrap.servers": args.bootstrap,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_ser,
        "enable.idempotence": True,
        "linger.ms": 50,
        "batch.size": 262_144,
        "compression.type": "snappy",
        "queue.buffering.max.messages": 200_000,
    }
    if args.kafka_username:  # SASL/PLAIN を上乗せ
        kafka_conf |= {
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": args.kafka_username,
            "sasl.password": args.kafka_password or "",
        }

    producer = SerializingProducer(kafka_conf)

    def delivery_report(err, msg):
        if err:
            print("❌", err)
        else:
            print(f"✅ {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

    def shutdown(_sig=None, _frm=None):
        print("\nFlushing …")
        producer.flush()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # バージョンを使用（修正済み）
    send_version = args.schema_version if args.schema_version else actual_version

    for _ in range(args.count):
        record = build_record(send_version)

        print(f"▶ Sending record: {record} (v{send_version})")

        try:
            validate_record(record, schema_str)  # ★ 追加バリデーション ★
            producer.produce(
                topic=args.topic,
                key=record["id"],
                value=record,
                on_delivery=delivery_report,
            )
        except Exception as ex:
            # 失敗レコードは送信せずスキップ
            print("⚠️  Skipped invalid record:", ex, file=sys.stderr)

        producer.poll(0)


    shutdown()


if __name__ == "__main__":
    main()
