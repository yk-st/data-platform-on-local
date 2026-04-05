import json
from pyspark.sql import functions as F, types as T

# ---- Avro -> SparkType 変換
def _to_spark_type(avro_type):
    # union: ["null", {...}] or ["null","string"] など
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"][0]
        return _to_spark_type(non_null)
    if isinstance(avro_type, str):
        return {
            "string": T.StringType(),
            "boolean": T.BooleanType(),
            "int": T.IntegerType(),
            "long": T.LongType(),
            "double": T.DoubleType(),
            "float": T.FloatType(),
            "bytes": T.BinaryType(),
        }[avro_type]
    # dict（logicalType など）
    lt = avro_type.get("logicalType")
    if lt == "decimal":
        return T.DecimalType(int(avro_type["precision"]), int(avro_type["scale"]))
    if lt in ("timestamp-millis", "timestamp-micros"):
        return T.TimestampType()
    # fallback（必要なら他logicalTypeを追加）
    base = avro_type.get("type")
    return _to_spark_type(base)

# ---- 契約適用（キャスト & デフォルト補填 & 列順整形 & extras退避）
def apply_contract_cast(df, avro_json: str):
    avro = json.loads(avro_json)
    fields = avro["fields"]
    target_cols = []
    for f in fields:
        name = f["name"]
        # Avroの型（union対応）
        st = _to_spark_type(f["type"])
        # default対応
        default = f.get("default", None)
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast(st))
        else:
            df = df.withColumn(name, F.lit(default).cast(st))
        target_cols.append(name)

    # 余剰列は map で退避（捨てない）
    extras_cols = [c for c in df.columns if c not in target_cols]
    if extras_cols:
        extras = F.map_from_arrays(
            F.array(*[F.lit(c) for c in extras_cols]),
            F.array(*[F.col(c).cast("string") for c in extras_cols])
        ).alias("_extras")
        df = df.select(*target_cols, extras)
    else:
        df = df.select(*target_cols)
    return df, avro

# ---- ルール検証（x_constraints / x_rules をDataFrame評価）
def validate_by_contract(df, avro):
    checks = []
    for f in avro["fields"]:
        name = f["name"]
        cons = f.get("x_constraints", {})
        if "pattern" in cons:
            checks.append((
                F.col(name).isNull() | F.col(name).rlike(cons["pattern"]),
                f"{name}_pattern"
            ))
        rng_cond = F.lit(True)
        if "min" in cons:
            rng_cond = rng_cond & (F.col(name).isNull() | (F.col(name) >= F.lit(cons["min"])))
        if "max" in cons:
            rng_cond = rng_cond & (F.col(name).isNull() | (F.col(name) <= F.lit(cons["max"])))
        if rng_cond != F.lit(True):
            checks.append((rng_cond, f"{name}_range"))

    for r in avro.get("x_rules", []):      # 例: ソフトデリート整合
        checks.append((F.expr(r["sql"]), r["name"]))

    # 失敗理由を配列で持たせる
    fails = [
        F.when(~cond, F.lit(name)) for cond, name in checks
    ]
    violations = F.array(*[f for f in fails if f is not None])
    df_chk = df.withColumn("_violations", F.expr("filter({}, x -> x IS NOT NULL)".format(violations._jc.toString())))
    good = df_chk.filter(F.size("_violations") == 0)
    bad  = df_chk.filter(F.size("_violations") > 0)
    return good, bad
