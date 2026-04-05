CREATE TABLE IF NOT EXISTS local_data_platform.ref.column_alias_map (
    regex           STRING          COMMENT '列名にマッチさせる正規表現',
    canonical_name  STRING          COMMENT '統一後のカラム名',
    priority        INT             COMMENT '競合時に小さい方を優先',
    updated_at      TIMESTAMP       COMMENT '最終更新時刻'
)
USING ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'openlineage.dataset.namespace'='local_data_platform.ref',
    'openlineage.dataset.name'='age_group_ref'
);
