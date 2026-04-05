
CREATE TABLE IF NOT EXISTS local_data_platform.brz_ingestion.products (
  `product_id`        STRING COMMENT '商品ID、本書ではSKUと同義として扱う',
  `ean_code`          CHAR(13) COMMENT 'EANコード',
  `product_title`     VARCHAR(255) COMMENT '商品タイトル',
  `category`          VARCHAR(255) COMMENT 'カテゴリ',
  `vendor_name`       VARCHAR(255) COMMENT 'ベンダー名称',
  `price_usd`         NUMERIC COMMENT '価格_ドル',
  `rating`            NUMERIC(10,4) COMMENT '評価（RATING）',
  `created_at`        TIMESTAMP COMMENT '作成日時',
  `updated_at`        TIMESTAMP COMMENT '更新日時',
  `is_deleted`        BOOLEAN COMMENT '論理削除フラグ',
  `deleted_at`        TIMESTAMP COMMENT '論理削除日時',
  `ingest_date`       DATE COMMENT 'パーティション用追加列'
)
USING iceberg
PARTITIONED BY (days(ingest_date))
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.brz_ingestion',
    'openlineage.dataset.name'='products',
    'write.format.default' = 'parquet',
    'format-version'       = '2',
    'write.distribution-mode'='hash',
    'write.target-file-size-bytes' = '268435456',
    'history.expire.max-snapshot-age-ms' = '2592000000',
    'history.expire.min-snapshots-to-keep' = '20'
);

ALTER TABLE local_data_platform.brz_ingestion.products SET TBLPROPERTIES (
'openlineage.dataset.namespace'='local_data_platform.brz_ingestion',
'openlineage.dataset.name'='products'
);