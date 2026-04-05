
CREATE TABLE IF NOT EXISTS local_data_platform.ref.zip_geocode (
  `zip_code` STRING        COMMENT 'ハイフン無し7桁郵便番号（先頭ゼロ保持）',
  `address`     STRING  COMMENT '都道府県名市区町村＋丁目まで',
  `latitude`      DOUBLE       COMMENT '緯度',
  `longitude`      DOUBLE       COMMENT '経度'
)
USING iceberg
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet',
  'openlineage.dataset.namespace'='local_data_platform.ref',
  'openlineage.dataset.name'='zip_geocode'
);