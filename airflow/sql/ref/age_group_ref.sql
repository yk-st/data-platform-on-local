-- ② age_group_ref テーブル

CREATE TABLE IF NOT EXISTS local_data_platform.ref.age_group_ref (
  `age_code`  CHAR(2)    COMMENT '年代グループコード (例: A1=10代)',
  `age_label` VARCHAR(10) COMMENT '年代ラベル (例: 10代)'
)
USING iceberg
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet',
  'openlineage.dataset.namespace'='local_data_platform.ref',
  'openlineage.dataset.name'='age_group_ref'
);