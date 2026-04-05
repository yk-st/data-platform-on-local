CREATE TABLE IF NOT EXISTS local_data_platform.brz_ingestion.orders (
  `order_id`        STRING COMMENT '注文ID',
  `user_id`         INT COMMENT 'ユーザーID',
  `product_id`      INT COMMENT '商品ID',
  `subtotal_usd`    NUMERIC COMMENT '小計_ドル',
  `tax_usd`         NUMERIC COMMENT '税額_ドル',
  `total_usd`       NUMERIC COMMENT '合計_ドル',
  -- 6章向け
  -- `subtotal_usd`           NUMERIC(10,2),
  -- `tax_usd`                NUMERIC(10,2),
  -- `total_usd`              NUMERIC(10,2),
  -- 6章向けここまでコメント
  `quantity`        INT COMMENT '数量_個',
  `status_flag`            INT COMMENT '1 は個人注文、2 は団体注文。',
  `created_at`      TIMESTAMP COMMENT '作成日時',
  `parent_id`       STRING COMMENT '親ID',
  `ingest_date`     DATE COMMENT 'パーティション用追加列',
  `user_id_hash`    BIGINT COMMENT 'パーティション用追加列'
)
USING iceberg
PARTITIONED BY (
  days(ingest_date),
  bucket(4, user_id_hash) -- 指定するカラムは数値型やdate timestamp型である必要があります
)
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.brz_ingestion',
    'openlineage.dataset.name'='orders',
    'write.format.default' = 'parquet',
    'format-version'       = '2',
    'write.distribution-mode'='hash',
    'write.target-file-size-bytes' = '268435456',
    'history.expire.max-snapshot-age-ms' = '2592000000',
    'history.expire.min-snapshots-to-keep' = '20'
);

ALTER TABLE local_data_platform.brz_ingestion.orders SET TBLPROPERTIES (
'openlineage.dataset.namespace'='local_data_platform.brz_ingestion',
'openlineage.dataset.name'='orders'
);
