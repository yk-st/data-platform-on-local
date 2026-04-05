CREATE TABLE IF NOT EXISTS spark_catalog.legacy.legacy_fund_master (
    `fund_id`            STRING          COMMENT '外部 ID（旧システム NFD…）',
    `投資信託_分類`       STRING          COMMENT 'ACTIVE / INDEX など',
    `信託報酬_率`         DECIMAL(4,2)    COMMENT '例: 1.20 = 1.20 %',
    `有効レコード`        BOOLEAN         COMMENT 'True=現行 / False=失効',
    `ファンド名`          STRING,
    `愛称`               STRING,
    `運用会社`            STRING,
    `隠れコスト`          DECIMAL(4,2),
    `内部戦略コード`      STRING,
    `ingest_date`        DATE            COMMENT '取り込み日（YYYY-MM-DD）'
)
USING PARQUET
PARTITIONED BY (ingest_date)
LOCATION 's3a://misc-data-platform/warehouse/legacy.db/legacy_fund_master'
TBLPROPERTIES (
  'parquet.compression' = 'snappy',
  'engine.hive.enabled' = 'true',
  'openlineage.dataset.namespace'='local_data_platform.legacy',
  'openlineage.dataset.name'='legacy_fund_master'
);