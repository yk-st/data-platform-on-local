#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_user_orders_weather.py

Batch-mode weather enrichment for user_orders_wide_enriched → user_orders_wide_enriched_weather

* Spark 3.5.5 / Scala 2.13
* Iceberg + HiveMetastore (Postgres) backend
* spark_utils.get_spark() でセッション取得
* FastAPI /weather バッチエンドポイント呼び出し（POST）
* Keycloak からアクセストークンを自動取得
* Broadcast 変数でトークン／設定を全ワーカーに配布
* RDD mapPartitions + batched() で分散呼び出し
* 各 Executor プロセス内で Semaphore＋Timer によるローカル QPS 制御
* Spark Accumulators で呼び出し状況を集計
* 結果は Iceberg テーブルに partition overwrite
"""

import os
import sys
import time
import json
import requests

from threading import Semaphore, Timer
from datetime import datetime, timezone

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F

from spark_utils import get_spark
from jobs.enrich.config import EnrichConfig

# ───────────────────────────────────────────
# Config
# ───────────────────────────────────────────
KC_BASE       = os.environ.get("KC_BASE", "http://host.docker.internal:28080")
REALM         = os.environ.get("REALM", "openldap")
CLIENT_ID     = os.environ.get("CLIENT_ID", "api_token")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "OLklloONpt0lKiIDG6mJBR8FIdpqpyhX")
USERNAME      = os.environ.get("USERNAME", "pyspark@local.data.platform")
PASSWORD      = os.environ.get("PASSWORD", "pyspark")
API_URL       = os.environ.get("WEATHER_API_URL", "http://host.docker.internal:8000/weather")
# 1回のAPI呼び出しで取得する件数
CHUNK         = int(os.environ.get("WEATHER_CHUNK", "20"))
# APIの最大呼び出し数(秒間あたり)
MAX_QPS       = int(os.environ.get("WEATHER_MAX_QPS", "50"))
# RDDのパーティション数
N_PARTS       = int(os.environ.get("WEATHER_N_PARTS", "200"))
SNAPSHOT_DT   = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
# 安全係数（1秒あたりの呼び出し数をさらに減らす）
SAFETY = 0.5

# ───────────────────────────────────────────
# Broadcast config and token
# ───────────────────────────────────────────
def fetch_token():
    url = f"{KC_BASE}/realms/{REALM}/protocol/openid-connect/token"
    payload = {
        "grant_type":    "password",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username":      USERNAME,
        "password":      PASSWORD,
    }
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
        return r.json().get("access_token")
    except Exception as e:
        print(f"[ERROR] Keycloak token fetch failed: {e}", file=sys.stderr)
        sys.exit(1)

TOKEN=fetch_token()

class WideEnrichTableTransformer:
    def transform(self, target_df, weather_raw_df):

        # ───────────────────────────────────────────
        # 3) schema & rename
        # ───────────────────────────────────────────
        weather_df = (
            weather_raw_df
            .withColumn("weather_dt", F.to_timestamp("dt"))
            .withColumn("generated_at", F.to_timestamp("generated_at"))
            .withColumnRenamed("lat", "latitude")
            .withColumnRenamed("lon", "longitude")
            .withColumnRenamed("unique_id", "order_id")
            .drop("dt", "latitude", "longitude")
        )

        weather_df = weather_df \
            .withColumnRenamed("condition",     "weather") \
            .withColumnRenamed("generated_at",  "weather_fetched_at") \
            .withColumnRenamed("temperature_c", "temperature_celsius") \
            .withColumnRenamed("weather_dt",    "weather_datetime")

        # ───────────────────────────────────────────
        # 4) join & write
        # ───────────────────────────────────────────
        result_df = (
            target_df
                .join(weather_df, on=["order_id"], how="left")
                .withColumn("ingest_date", F.current_date())
        )

        # spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        # result_df.write \
        #     .format("iceberg") \
        #     .mode("overwrite") \
        #     .option("replaceWhere", f"ingest_date = '{datetime.now().date()}'") \
        #     .saveAsTable("local_data_platform.slv_analytics.user_orders_wide_enriched_weather")

        return result_df

def transform_weather_enrich_table(config, args):
    spark = get_spark()

    # ───────────────────────────────────────────
    # Spark setup
    # ───────────────────────────────────────────
    sc: SparkContext    = spark.sparkContext

    # ───────────────────────────────────────────
    # Compute per-task QPS so cluster-wide ≤ MAX_QPS
    # ───────────────────────────────────────────
    par = sc.defaultParallelism or 1
    rough_local_qps = max(1, MAX_QPS // par)              # まず均等割り
    local_qps = max(1, int(rough_local_qps * SAFETY))     # 半減（=安全係数2）

    # ついでに上振れ防止の軽い上限を入れるなら安全策として
    MAX_LOCAL_QPS = 10   # 好みで
    local_qps = min(local_qps, MAX_LOCAL_QPS)

    bc_token     = sc.broadcast(TOKEN)
    bc_api       = sc.broadcast(API_URL)
    bc_chunk     = sc.broadcast(CHUNK)
    bc_local_qps = sc.broadcast(local_qps)

    # ───────────────────────────────────────────
    # Accumulators
    # ───────────────────────────────────────────
    acc_calls   = sc.accumulator(0)
    acc_success = sc.accumulator(0)
    acc_fail    = sc.accumulator(0)

    # ───────────────────────────────────────────
    # Helper: split iterator into chunks
    # ───────────────────────────────────────────
    # n:chunk size
    def batched(iterator, n):
        it = iter(iterator)
        while True:
            chunk = list(itertools.islice(it, n))
            if not chunk:
                break
            yield chunk

    # ───────────────────────────────────────────
    # call_api: run per partition on Executor
    # ───────────────────────────────────────────
    import itertools

    def call_api(partition):
        if not hasattr(call_api, 'bucket'):
            limit = bc_local_qps.value
            # セマフォの初期化
            call_api.bucket = Semaphore(limit)
            def refill():
                missing = limit - call_api.bucket._value  # type: ignore[attr-defined]
                for _ in range(missing):
                    call_api.bucket.release()
                # 一秒経ったらチケットを補充する
                t = Timer(1, refill)
                t.daemon = True
                t.start()
            refill()
        token = bc_token.value
        endpoint = bc_api.value
        chunk_size = bc_chunk.value
        for batch in batched(partition, chunk_size):
            if batch:
                print(f"[DEBUG] Sending batch of size {len(batch)}, sample record: {batch[0]}", file=sys.stderr)
                body = json.dumps(batch)
                print(f"[DEBUG] HTTP REQUEST BODY: {body[:200]}{'...' if len(body)>200 else ''}", file=sys.stderr)
            # セマフォからチケットを取得(取得できなければブロック)
            call_api.bucket.acquire()
            acc_calls.add(1)
            for attempt in range(1):
                try:
                    resp = requests.post(
                        endpoint,
                        headers={"Authorization": f"Bearer {token}",
                                "Content-Type": "application/json"},
                        json=batch,
                        timeout=15
                    )
                    print(f"[DEBUG] RESPONSE status={resp.status_code}, body={resp.text}", file=sys.stderr)
                    if resp.status_code == 429:
                        time.sleep(int(resp.headers.get('Retry-After','1')))
                        continue
                    resp.raise_for_status()
                    acc_success.add(1)
                    # Yield JSON strings for parsing
                    for rec in resp.json():
                        yield json.dumps({**rec, '_dlq': False})
                    break
                except Exception as e:
                    print(f"[DEBUG] Exception on request: {e}", file=sys.stderr)
                    time.sleep(2 ** attempt)
            # for-else
            else:
                acc_fail.add(1)
                for rec in batch:
                    # フォーマットを合わせる
                    yield json.dumps({**rec, '_dlq': True, 'condition': "", 'temperature_c': -1, 'generated_at': datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()})

    # ───────────────────────────────────────────
    # 1) extract coords distinct
    # ───────────────────────────────────────────
    target_df = (
        spark.read
            .format("iceberg")
            #.option("branch", "wapuo")
            .table(config.get_dynamic_table_name(config.TABLE_USER_ORDERS_WIDE_ENRICHED, args))
            .filter(F.col("ingest_date") == F.lit(args.ingest_date))  # 本日分のみ
    )

    coords_df = (
        target_df
            .select(
                F.col("latitude").alias("lat"),
                F.col("longitude").alias("lon"),
                F.col("order_created_at").alias("dt"),
                F.col("order_id").alias("unique_id")
            )
            .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .distinct()
    )

    coords_count = coords_df.count()
    if coords_count == 0:
        print("[INFO] No valid coordinates found. Skipping weather enrichment.")
        spark.stop()
        return

    # ───────────────────────────────────────────
    # 2) RDD API + mapPartitions
    # ───────────────────────────────────────────
    weather_rdd = coords_df.toJSON().map(json.loads).mapPartitions(call_api)
    weather_rdd = weather_rdd.repartition(N_PARTS)
    weather_raw_df = spark.read.json(weather_rdd)
    print(f"[DEBUG] Weather raw DataFrame schema: {weather_raw_df.schema}", file=sys.stderr)

    df = WideEnrichTableTransformer().transform(target_df, weather_raw_df)

    # ───────────────────────────────────────────
    # summary
    # ───────────────────────────────────────────
    print("\n===== Weather Enrichment Summary =====")
    print(f"API calls issued     : {acc_calls.value}")
    print(f"Successful responses : {acc_success.value}")
    print(f"Failed (DLQ) chunks  : {acc_fail.value}")
    print("=======================================\n")

    # 書き込み
    df.writeTo(config.get_dynamic_table_name(config.TABLE_USER_ORDERS_WIDE_WEATHER_ENRICHED, args)).overwritePartitions()
    spark.stop()

if __name__ == "__main__":
    args = EnrichConfig.parse_args()
    transform_weather_enrich_table(EnrichConfig(), args)