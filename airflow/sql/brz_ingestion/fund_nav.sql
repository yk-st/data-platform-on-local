
CREATE TABLE IF NOT EXISTS local_data_platform.brz_ingestion.fund_nav (
    `base_date` DATE COMMENT '基準日',
    `fund_id` STRING COMMENT 'ファンドID',
    `nav_price` DECIMAL(12,2) COMMENT '基準価格'
)
USING ICEBERG
PARTITIONED BY (months(`base_date`))
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.brz_ingestion',
    'openlineage.dataset.name'='fund_nav',
    'write.format.default' = 'parquet',
    'format-version'       = '2',
    'write.distribution-mode'='hash',
    'write.target-file-size-bytes' = '268435456',
    'history.expire.max-snapshot-age-ms' = '2592000000',
    'history.expire.min-snapshots-to-keep' = '20'
);