CREATE EXTERNAL TABLE IF NOT EXISTS spark_catalog.legacy.legacy_orders (
  `注文ID`       STRING            COMMENT 'ID',
  `ユーザーID`    INT               COMMENT 'ユーザーID',
  `商品ID`       INT               COMMENT '製品ID',
  `小計_円`      DECIMAL(10,2)     COMMENT '小計-金額',
  `税額_円`      DECIMAL(10,2)     COMMENT '税金額',
  `合計_円`      DECIMAL(10,2)     COMMENT '合計ー円',
  `数量_個`      INT               COMMENT '数量（個）',
  `フラグ`       INT               COMMENT 'フラグ',
  `作成日時`     TIMESTAMP         COMMENT '作成日時',
  ingest_date    DATE
)
USING PARQUET
PARTITIONED BY (ingest_date)
LOCATION 's3a://misc-data-platform/warehouse/legacy.db/legacy_orders'
TBLPROPERTIES (
  'parquet.compression' = 'snappy',
  'engine.hive.enabled' = 'true',
  'openlineage.dataset.namespace'='local_data_platform.legacy',
  'openlineage.dataset.name'='legacy_order'
);