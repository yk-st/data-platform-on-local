CREATE TABLE IF NOT EXISTS local_data_platform.ref.zip_taxonomy_pt (
  `prefecture`   STRING,
  `municipality` STRING,
  `chome`        STRING,
  `zip_code`     STRING,
  `latitude`     DOUBLE,
  `longitude`    DOUBLE
)
USING iceberg
PARTITIONED BY (prefecture)
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet',
  'openlineage.dataset.namespace'='local_data_platform.ref',
  'openlineage.dataset.name'='zip_taxonomy_pt'
);