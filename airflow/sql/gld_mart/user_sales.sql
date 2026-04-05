
CREATE TABLE IF NOT EXISTS local_data_platform.gld_mart.user_sales (
    `user_id`   INT COMMENT 'ユーザーのフルネーム',
    `user_name`   STRING COMMENT 'ユーザーのフルネーム',
    `order_count`     BIGINT COMMENT '集計: 注文ID のカウント',
    `total_sales`   DECIMAL(18,2) COMMENT '集計: 合計(ドル) の合計'
)
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.gld_mart',
    'openlineage.dataset.name'='user_sales',
    'write.format.default' = 'parquet',
    'format-version'       = '2',
    'write.distribution-mode'='hash',
    'write.target-file-size-bytes' = '268435456',
    'history.expire.max-snapshot-age-ms' = '2592000000',
    'history.expire.min-snapshots-to-keep' = '20',
    'comment' = 'ユーザ日次売上 (税込) テーブル'
);

ALTER TABLE local_data_platform.gld_mart.user_sales SET TBLPROPERTIES (
'openlineage.dataset.namespace' = 'local_data_platform.gld_mart',
'openlineage.dataset.name' = 'user_sales',
'comment' = 'ユーザ日次売上 (税込) テーブル',
'openlineage.dataset.facets.update_schedule' = '{
    "cron": "0 2 * * *",
    "schedule": "daily"
}'
);

ALTER TABLE local_data_platform.gld_mart.user_sales
  CHANGE COLUMN `ユーザー名` `ユーザー名`  STRING COMMENT 'ユーザーのフルネーム';