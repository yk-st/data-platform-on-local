CREATE TABLE IF NOT EXISTS local_data_platform.brz_ingestion.users (
  `user_id`               STRING COMMENT 'ユーザーID',
  `address`               VARCHAR(255) COMMENT '住所',
  `email`                 VARCHAR(255) COMMENT 'メールアドレス',
  `password`              VARCHAR(255) COMMENT 'パスワード',
  `user_name`             VARCHAR(255) COMMENT 'ユーザー名',
  `acquisition_channel`   VARCHAR(255) COMMENT 'チャネル_取得元',
  `birth_date`            DATE COMMENT '生年月日',
  `zip_code`              CHAR(5) COMMENT '郵便番号',
  `created_at`            TIMESTAMP COMMENT '作成日時',
  `updated_at`            TIMESTAMP COMMENT '更新日時',
  `is_deleted`            BOOLEAN COMMENT '論理削除フラグ',
  `deleted_at`            TIMESTAMP COMMENT '論理削除日時',
  `ingest_date`           DATE COMMENT 'パーティション用追加列',
  `user_id_hash`          BIGINT COMMENT 'パーティション用追加列'
)
USING iceberg
PARTITIONED BY (
  days(ingest_date),
  bucket(4, user_id_hash) -- 指定するカラムは数値型やdate timestamp型である必要があります
)
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.brz_ingestion',
    'openlineage.dataset.name'='users',
    'write.format.default' = 'parquet',
    'format-version'       = '2',
    'write.distribution-mode'='hash',
    'write.target-file-size-bytes' = '268435456',
    'history.expire.max-snapshot-age-ms' = '2592000000',
    'history.expire.min-snapshots-to-keep' = '20'
);

ALTER TABLE local_data_platform.brz_ingestion.users SET TBLPROPERTIES (
'openlineage.dataset.namespace'='local_data_platform.brz_ingestion',
'openlineage.dataset.name'='users'
);