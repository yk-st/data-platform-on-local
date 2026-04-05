
CREATE TABLE IF NOT EXISTS local_data_platform.brz_ingestion.fund_master  (
    `fund_id`           STRING,
    `fund_category`     STRING,
    `trust_fee_rate`       DECIMAL(5,2),
    `is_active`       BOOLEAN,
    `fund_name`          STRING,
    `nickname`               STRING,
    `management_company`            STRING,
    `manager_name`         STRING,
    `manager_email`       STRING,
    ingest_date DATE
)
USING ICEBERG
PARTITIONED BY (days(ingest_date))
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.brz_ingestion',
    'openlineage.dataset.name'='fund_master',
    'write.format.default' = 'parquet',
    'format-version'       = '2',
    'write.distribution-mode'='hash',
    'write.target-file-size-bytes' = '268435456',
    'history.expire.max-snapshot-age-ms' = '2592000000',
    'history.expire.min-snapshots-to-keep' = '20'
);
