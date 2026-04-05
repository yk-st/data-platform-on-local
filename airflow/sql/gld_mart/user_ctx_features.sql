
CREATE TABLE IF NOT EXISTS local_data_platform.gld_mart.user_ctx_feature (
    `user_id`           STRING,
    `total_purchase_amount`         DOUBLE,
    `order_count`             INT,
    `rainy_day_coupon_eligible`     BOOLEAN,
    `temperature_celsius`            INT,
    `weather_datetime`         TIMESTAMP,
    ingest_date           DATE
)
USING iceberg
PARTITIONED BY (`ingest_date`)
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.gld_mart',
    'openlineage.dataset.name'='user_ctx_feature',
    'write.format.default'            = 'parquet',
    'format-version'                  = '2',
    'write.distribution-mode'         = 'hash',
    'write.target-file-size-bytes'    = '268435456',
    'history.expire.max-snapshot-age-ms'   = '2592000000',
    'history.expire.min-snapshots-to-keep' = '20'
);