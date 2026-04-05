# -*- coding: utf-8 -*-
"""
Avro(=契約)を読み込み、Bronzeテーブル `local_data_platform.brz_ingestion.products`
に適用して検証し、結果をコンソールに出す PySpark スクリプト。

使い方例:
spark-submit /home/pyspark/programs/spark/data_contract/data_contract.py \
  --avsc-path "/home/pyspark/programs/spark/data_contract/products_contract.avsc" \
  --src-table "local_data_platform.brz_ingestion.products" \
  --limit 30 \
  --strict-cols \
  --bad-max-rows 10 --bad-max-rate 0.05 \
  --strict-pk --bad-sample 50
"""
import argparse, json, re, sys
from typing import Tuple, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


# ---- JSON ユーティリティ（コメント/末尾カンマ除去） --------------------------
def strip_json_comments(s: str) -> str:
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.S)  # /* ... */
    s = re.sub(r"//.*", "", s)                   # // ...
    return s

def strip_trailing_commas(s: str) -> str:
    # 末尾カンマ:  ,  } / , ]
    return re.sub(r",(\s*[}\]])", r"\1", s)


# ---- Avro -> Spark DataType --------------------------------------------------
def to_spark_type(avro_type: Any) -> T.DataType:
    if isinstance(avro_type, list):                 # union: ["null", T]
        non_null = [t for t in avro_type if t != "null"][0]
        return to_spark_type(non_null)
    if isinstance(avro_type, str):                  # primitives
        mapping = {
            "string": T.StringType(),
            "boolean": T.BooleanType(),
            "int": T.IntegerType(),
            "long": T.LongType(),
            "float": T.FloatType(),
            "double": T.DoubleType(),
            "bytes": T.BinaryType(),
            "null": T.StringType(),
        }
        if avro_type not in mapping:
            raise ValueError(f"Unsupported avro primitive: {avro_type}")
        return mapping[avro_type]
    # dict (logicalTypeなど)
    base = avro_type.get("type")
    logical = avro_type.get("logicalType")
    if logical == "decimal":
        return T.DecimalType(int(avro_type["precision"]), int(avro_type["scale"]))
    if logical in ("timestamp-millis", "timestamp-micros"):
        return T.TimestampType()
    return to_spark_type(base)


# ---- 契約適用（キャスト＆列順整形＆余剰退避） --------------------------------
def apply_contract_cast(df, avro: Dict) -> Tuple[Any, Dict]:
    fields = avro["fields"]
    target_cols = []
    for f in fields:
        name = f["name"]
        st = to_spark_type(f["type"])
        default = f.get("default", None)
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast(st))
        else:
            df = df.withColumn(name, F.lit(default).cast(st))
        target_cols.append(name)

    extras_cols = [c for c in df.columns if c not in target_cols]
    if extras_cols:
        extras_map = F.map_from_arrays(
            F.array(*[F.lit(c) for c in extras_cols]),
            F.array(*[F.col(c).cast("string") for c in extras_cols])
        ).alias("_extras")
        df = df.select(*target_cols, extras_map)
    else:
        df = df.select(*target_cols)
    return df, avro


# ---- 検証（x_constraints / x_rules） -----------------------------------------
def _typed_lit(value, dtype):
    # 比較の型を列型に合わせる（特に Decimal で必須）
    return F.lit(value).cast(dtype)

def validate_by_contract(df, avro: dict):
    checks = []
    schema = {f.name: f.dataType for f in df.schema.fields}

    for f in avro["fields"]:
        name = f["name"]
        cons = f.get("x_constraints", {})
        dtype = schema.get(name)

        # pattern
        if "pattern" in cons:
            cond = (F.col(name).isNull() | F.col(name).rlike(cons["pattern"]))
            checks.append((cond, f"{name}_pattern"))

        # 数値レンジ（NULLの扱いを明示、リテラルを型合わせ）
        if "min" in cons or "max" in cons:
            is_null = F.col(name).isNull()
            null_ok = F.lit(not cons.get("null_fails", False))  # 既定: NULLはOK

            non_null_ok = F.lit(True)
            if "min" in cons:
                non_null_ok = non_null_ok & (F.col(name) >= _typed_lit(cons["min"], dtype))
            if "max" in cons:
                non_null_ok = non_null_ok & (F.col(name) <= _typed_lit(cons["max"], dtype))

            cond = (is_null & null_ok) | (~is_null & non_null_ok)
            checks.append((cond, f"{name}_range"))

        # 文字列長
        if "min_length" in cons:
            cond = (F.col(name).isNotNull()) & (F.length(F.col(name)) >= int(cons["min_length"]))
            checks.append((cond, f"{name}_min_length"))

    # 行ルール（NULL合格を防ぐため IS TRUE）
    for r in avro.get("x_rules", []):
        checks.append((F.expr(f"({r['sql']}) IS TRUE"), r.get("name","rule")))

    # 違反まとめ
    viol = [F.when(c, F.lit(None)).otherwise(F.lit(n)) for c,n in checks]
    if viol:
        df = df.withColumn("_viol_tmp", F.array(*viol)) \
               .withColumn("_violations", F.expr("filter(_viol_tmp, x -> x is not null)")) \
               .drop("_viol_tmp")
    else:
        df = df.withColumn("_violations", F.array())

    good = df.filter(F.size("_violations")==0)
    bad  = df.filter(F.size("_violations")>0)
    return good, bad


# ---- SLO: latency_sec の簡易評価 ---------------------------------------------
def slo_latency_check(df, avro: Dict):
    slo = avro.get("x-slo") or avro.get("x_slo") or {}
    thresh = slo.get("latency_sec")
    if not thresh:
        return {"slo_latency_sec": None}

    event_ts = F.coalesce(F.col("updated_at"), F.col("created_at"))
    df2 = df.withColumn("_event_ts", event_ts)
    df2 = df2.withColumn("_latency_sec", (F.current_timestamp().cast("long") - F.col("_event_ts").cast("long")))
    total = df2.count()
    if total == 0:
        return {"slo_latency_sec": thresh, "latency_ok_ratio": None, "p95_latency_sec": None}

    ok_ratio = df2.filter(F.col("_latency_sec") <= F.lit(int(thresh))).count() / total
    p95 = df2.approxQuantile("_latency_sec", [0.95], 0.01)[0]
    return {"slo_latency_sec": thresh, "latency_ok_ratio": ok_ratio, "p95_latency_sec": p95}


# ---- スキーマドリフト検出 -----------------------------------------------------
def detect_schema_drift(df_src, avro: dict) -> dict:
    expected = [f["name"] for f in avro["fields"]]
    src_cols = df_src.columns
    unexpected = sorted(set(src_cols) - set(expected))   # こっそり増えた
    missing    = sorted(set(expected) - set(src_cols))   # こっそり消えた
    # 型ミスマッチ（参考：元DFの推定型 vs 契約の要求型）
    req_types = {f["name"]: to_spark_type(f["type"]) for f in avro["fields"]}
    src_types = {f.name: f.dataType for f in df_src.schema.fields}
    type_mismatch = {
        c: {"src": src_types[c], "required": req_types[c]}
        for c in sorted(set(src_cols) & set(expected))
        if src_types.get(c) and req_types.get(c) and src_types[c] != req_types[c]
    }
    return {"unexpected": unexpected, "missing": missing, "type_mismatch": type_mismatch}


# ---- PK重複チェック -----------------------------------------------------------
def check_pk_duplicates(df_cast, avro: dict, sample: int = 20):
    pk = avro.get("x_sql", {}).get("primary_key", [])
    if not pk:
        return {"pk": [], "dup_count": 0, "dup_df": None}
    dup = (df_cast.groupBy(*[F.col(c) for c in pk]).count().filter("count > 1"))
    cnt = dup.count()
    return {"pk": pk, "dup_count": cnt, "dup_df": dup.limit(sample)}


# ---- main --------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--avsc-path", required=True, help="products の Avro(.avsc/.jsonc)。//コメント・末尾カンマ可")
    parser.add_argument("--src-table", default="local_data_platform.brz_ingestion.products",
                        help="読み込み元のBronzeテーブル")
    parser.add_argument("--limit", type=int, default=10, help="GOODの表示行数")
    parser.add_argument("--bad-sample", type=int, default=15, help="BADのサンプル表示行数")
    parser.add_argument("--strict-cols", action="store_true",
                        help="列の増減/型ズレを検知したら終了コード2で落とす")
    parser.add_argument("--bad-max-rows", type=int, default=0,
                        help="Badがこの件数を超えたら終了コード3（0=無効）")
    parser.add_argument("--bad-max-rate", type=float, default=0.0,
                        help="Bad率(0.0〜1.0)がこの値を超えたら終了コード3（0=無効）")
    parser.add_argument("--strict-pk", action="store_true",
                        help="PK重複があれば終了コード4で落とす（x_sql.primary_key が必要）")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("contract_check_products_from_bronze")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.storeAssignmentPolicy", "ANSI")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR") 

    # Avro(契約) 読み込み（まず厳密JSON、ダメならコメント/末尾カンマ除去）
    with open(args.avsc_path, "r", encoding="utf-8") as f:
        raw = f.read()
    try:
        avro = json.loads(raw)
    except json.JSONDecodeError:
        avro = json.loads(strip_trailing_commas(strip_json_comments(raw)))

    # Bronze 読み込み
    df_src = spark.table(args.src_table)

    # 0) スキーマドリフト検知（キャスト前に評価）
    drift = detect_schema_drift(df_src, avro)
    has_drift = bool(drift["unexpected"] or drift["missing"] or drift["type_mismatch"])
    if has_drift:
        print("\n===== SCHEMA DRIFT DETECTED =====")
        if drift["unexpected"]:
            print("Unexpected columns (source only):", drift["unexpected"])
        if drift["missing"]:
            print("Missing columns (in contract only):", drift["missing"])
        if drift["type_mismatch"]:
            print("Type mismatch (source vs required):")
            for c, t in drift["type_mismatch"].items():
                print(f"  - {c}: {t['src']} -> {t['required']}")
        if args.strict_cols:
            print("strict-cols enabled: exiting with code 2.")
            spark.stop(); sys.exit(2)
    else:
        print("\nNo schema drift: columns match the contract.")

    # 1) 型/論理型キャスト
    df_cast, _ = apply_contract_cast(df_src, avro)

    # 1.5) PK重複チェック（複合キー対応）
    pk_result = check_pk_duplicates(df_cast, avro, sample=20)
    if pk_result["pk"]:
        print(f"\n===== PRIMARY KEY CHECK ({'+'.join(pk_result['pk'])}) =====")
        print(f"duplicate groups : {pk_result['dup_count']}")
        if pk_result["dup_count"] > 0:
            pk_result["dup_df"].show(truncate=False)
            if args.strict_pk:
                print("strict-pk enabled: exiting with code 4.")
                spark.stop(); sys.exit(4)

    # 2) ルール検証
    df_good, df_bad = validate_by_contract(df_cast, avro)

    # 3) SLO（latency_sec）簡易評価
    slo_metrics = slo_latency_check(df_cast, avro)

    # 4) コンソール出力
    total_cnt = df_cast.count()
    good_cnt = df_good.count()
    bad_cnt  = df_bad.count()
    bad_rate = bad_cnt / max(total_cnt, 1)

    print("\n===== CONTRACT CHECK SUMMARY =====")
    print(f"table              : {avro.get('x_sql', {}).get('table', 'products')}")
    print(f"source table       : {args.src_table}")
    print(f"total rows         : {total_cnt}")
    print(f"good rows          : {good_cnt}")
    print(f"bad rows           : {bad_cnt} (rate={bad_rate:.4f})")
    if slo_metrics.get("slo_latency_sec") is not None:
        print("----- SLO (latency) -----")
        print(f"threshold_sec      : {slo_metrics['slo_latency_sec']}")
        print(f"ok_ratio           : {slo_metrics['latency_ok_ratio']}")
        print(f"p95_latency_sec    : {slo_metrics['p95_latency_sec']}")

    print("\n----- GOOD (first {}) -----".format(args.limit))
    df_good.show(args.limit, truncate=False)

    print("\n----- BAD (sample {}) -----".format(args.bad_sample))
    df_bad.show(args.bad_sample, truncate=False)

    # 違反内訳
    if bad_cnt > 0:
        viol_stats = (
            df_bad
            .select(F.explode("_violations").alias("violation"))
            .groupBy("violation").count()
            .orderBy(F.desc("count"))
        )
        print("\n----- VIOLATION BREAKDOWN -----")
        viol_stats.show(truncate=False)

    # 5) 失敗閾値ゲート（任意）
    if args.bad_max_rows and bad_cnt > args.bad_max_rows:
        print(f"\nBad rows exceeded threshold ({bad_cnt} > {args.bad_max_rows}). Exit 3.")
        spark.stop(); sys.exit(3)
    if args.bad_max_rate and bad_rate > args.bad_max_rate:
        print(f"\nBad rate exceeded threshold ({bad_rate:.4f} > {args.bad_max_rate}). Exit 3.")
        spark.stop(); sys.exit(3)

    print("\n===== DONE =====")
    spark.stop()


if __name__ == "__main__":
    main()



# ===== SCHEMA DRIFT DETECTED =====
# Unexpected columns (source only): ['category', 'ingest_date']
# Missing columns (in contract only): ['categorys']
# Type mismatch (source vs required):
#   - price_usd: DecimalType(10,0) -> DecimalType(12,2)
#   - rating: DecimalType(10,4) -> DecimalType(3,2)

# ===== PRIMARY KEY CHECK (product_id) =====
# duplicate groups : 0

# ===== CONTRACT CHECK SUMMARY =====
# table              : products
# source table       : local_data_platform.brz_ingestion.products
# total rows         : 100
# good rows          : 72
# bad rows           : 28 (rate=0.2800)
# ----- SLO (latency) -----
# threshold_sec      : 600
# ok_ratio           : 0.0
# p95_latency_sec    : 39398504.0

# ----- GOOD (first 10) -----
# +----------+-------------+---------------------------+---------+-----------+---------+------+-------------------+-------------------+----------+----------+--------------------------------------------+-----------+
# |product_id|ean_code     |product_title              |categorys|vendor_name|price_usd|rating|created_at         |updated_at         |is_deleted|deleted_at|_extras                                     |_violations|
# +----------+-------------+---------------------------+---------+-----------+---------+------+-------------------+-------------------+----------+----------+--------------------------------------------+-----------+
# |1         |2770735575101|ウィメンズジャケット S     |NULL     |三菱電機   |12534.00 |3.70  |2025-01-17 10:11:09|2025-02-09 10:11:09|false     |NULL      |{category -> C02, ingest_date -> 2025-09-05}|[]         |
# |3         |8505086125352|フェイスマスク 5枚         |NULL     |三菱電機   |40922.00 |4.40  |2025-03-31 10:11:09|2025-04-19 10:11:09|false     |NULL      |{category -> C04, ingest_date -> 2025-09-05}|[]         |
# |4         |7611974658271|台所用洗剤 500ml           |NULL     |ユニクロ   |13480.00 |2.50  |2025-04-08 10:11:09|2025-04-27 10:11:09|false     |NULL      |{category -> C04, ingest_date -> 2025-09-05}|[]         |
# |5         |0072121262762|国産米 5kg                 |NULL     |ユニクロ   |30966.00 |4.70  |2024-06-23 10:11:09|2024-07-02 10:11:09|false     |NULL      |{category -> C03, ingest_date -> 2025-09-05}|[]         |
# |6         |9225105731938|スマートテレビ 55型        |NULL     |花王       |7113.00  |3.10  |2025-02-21 10:11:09|2025-03-10 10:11:09|false     |NULL      |{category -> C01, ingest_date -> 2025-09-05}|[]         |
# |7         |1052268214521|全自動洗濯機 6kg           |NULL     |三菱電機   |43900.00 |4.30  |2024-09-30 10:11:09|2024-10-29 10:11:09|false     |NULL      |{category -> C01, ingest_date -> 2025-09-05}|[]         |
# |8         |5171379391933|スマートテレビ 55型        |NULL     |三菱電機   |28822.00 |4.60  |2025-01-17 10:11:09|2025-02-01 10:11:09|false     |NULL      |{category -> C01, ingest_date -> 2025-09-05}|[]         |
# |9         |8888537277383|トイレットペーパー 12ロール|NULL     |三菱電機   |9091.00  |2.40  |2025-01-07 10:11:09|2025-01-11 10:11:09|false     |NULL      |{category -> C04, ingest_date -> 2025-09-05}|[]         |
# |10        |8217145354239|歯ブラシ 3本セット         |NULL     |味の素     |13990.00 |4.60  |2025-01-11 10:11:09|2025-01-30 10:11:09|false     |NULL      |{category -> C04, ingest_date -> 2025-09-05}|[]         |
# |12        |4129024915541|歯ブラシ 3本セット         |NULL     |三菱電機   |30769.00 |4.60  |2024-06-04 10:11:09|2024-06-08 10:11:09|false     |NULL      |{category -> C04, ingest_date -> 2025-09-05}|[]         |
# +----------+-------------+---------------------------+---------+-----------+---------+------+-------------------+-------------------+----------+----------+--------------------------------------------+-----------+
# only showing top 10 rows


# ----- BAD (sample 15) -----
# +----------+-------------+----------------------+---------+-----------+---------+------+-------------------+-------------------+----------+----------+--------------------------------------------+--------------+
# |product_id|ean_code     |product_title         |categorys|vendor_name|price_usd|rating|created_at         |updated_at         |is_deleted|deleted_at|_extras                                     |_violations   |
# +----------+-------------+----------------------+---------+-----------+---------+------+-------------------+-------------------+----------+----------+--------------------------------------------+--------------+
# |2         |1724396229455|メンズTシャツ Lサイズ |NULL     |花王       |19765.00 |1.80  |2024-09-18 10:11:09|2024-10-10 10:11:09|false     |NULL      |{category -> C02, ingest_date -> 2025-09-05}|[rating_range]|
# |11        |6111451107611|デニムパンツ 32inch   |NULL     |三菱電機   |44560.00 |1.70  |2024-08-28 10:11:09|2024-09-05 10:11:09|false     |NULL      |{category -> C02, ingest_date -> 2025-09-05}|[rating_range]|
# |13        |6925682785103|デニムパンツ 32inch   |NULL     |味の素     |6947.00  |1.30  |2024-12-20 10:11:09|2025-01-10 10:11:09|false     |NULL      |{category -> C02, ingest_date -> 2025-09-05}|[rating_range]|
# |17        |3109615311655|フェイスマスク 5枚    |NULL     |味の素     |41630.00 |1.10  |2024-09-25 10:11:09|2024-10-13 10:11:09|false     |NULL      |{category -> C04, ingest_date -> 2025-09-05}|[rating_range]|
# |19        |4851326808445|冷蔵庫 300L           |NULL     |味の素     |7581.00  |1.70  |2024-11-20 10:11:09|2024-12-06 10:11:09|false     |NULL      |{category -> C01, ingest_date -> 2025-09-05}|[rating_range]|
# |24        |8926128737988|台所用洗剤 500ml      |NULL     |味の素     |21436.00 |1.70  |2025-01-13 10:11:09|2025-02-02 10:11:09|false     |NULL      |{category -> C04, ingest_date -> 2025-09-05}|[rating_range]|
# |26        |9212274837320|スマートテレビ 55型   |NULL     |花王       |37587.00 |1.50  |2025-03-06 10:11:09|2025-03-21 10:11:09|false     |NULL      |{category -> C01, ingest_date -> 2025-09-05}|[rating_range]|
# |31        |4625182819460|チョコレートギフト    |NULL     |ユニクロ   |31145.00 |1.40  |2024-12-31 10:11:09|2025-01-17 10:11:09|false     |NULL      |{category -> C03, ingest_date -> 2025-09-05}|[rating_range]|
# |38        |9460627391745|ウィメンズジャケット S|NULL     |味の素     |26630.00 |1.60  |2024-06-16 10:11:09|2024-06-26 10:11:09|false     |NULL      |{category -> C02, ingest_date -> 2025-09-05}|[rating_range]|
# |39        |0287378091040|有機りんご 1kg        |NULL     |花王       |42942.00 |1.60  |2025-03-04 10:11:09|2025-03-24 10:11:09|false     |NULL      |{category -> C03, ingest_date -> 2025-09-05}|[rating_range]|
# |42        |1695093937202|スマートテレビ 55型   |NULL     |花王       |44523.00 |1.50  |2025-02-27 10:11:09|2025-03-12 10:11:09|false     |NULL      |{category -> C01, ingest_date -> 2025-09-05}|[rating_range]|
# |47        |7072086135739|スマートテレビ 55型   |NULL     |三菱電機   |1424.00  |1.40  |2024-08-24 10:11:09|2024-08-26 10:11:09|false     |NULL      |{category -> C01, ingest_date -> 2025-09-05}|[rating_range]|
# |49        |6311136474739|即席ラーメン 12食     |NULL     |三菱電機   |33914.00 |1.00  |2025-03-24 10:11:09|2025-04-21 10:11:09|false     |NULL      |{category -> C03, ingest_date -> 2025-09-05}|[rating_range]|
# |53        |2130745491291|フェイスマスク 5枚    |NULL     |花王       |8404.00  |1.10  |2024-06-06 10:11:09|2024-06-08 10:11:09|false     |NULL      |{category -> C04, ingest_date -> 2025-09-05}|[rating_range]|
# |59        |6297961043051|フェイスマスク 5枚    |NULL     |花王       |39418.00 |1.00  |2024-10-06 10:11:09|2024-10-26 10:11:09|false     |NULL      |{category -> C04, ingest_date -> 2025-09-05}|[rating_range]|
# +----------+-------------+----------------------+---------+-----------+---------+------+-------------------+-------------------+----------+----------+--------------------------------------------+--------------+
# only showing top 15 rows


# ----- VIOLATION BREAKDOWN -----
# +------------+-----+
# |violation   |count|
# +------------+-----+
# |rating_range|28   |
# +------------+-----+


# ===== DONE =====
# pyspark@work:~$ 