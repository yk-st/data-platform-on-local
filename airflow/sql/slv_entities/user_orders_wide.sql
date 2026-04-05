
CREATE TABLE IF NOT EXISTS local_data_platform.slv_entities.user_orders_wide (
    `order_id`                   STRING COMMENT '注文ID',                      -- from orders
    `user_id`                    INT COMMENT 'ユーザーID',                     -- from orders
    `product_id`                 INT COMMENT '商品ID',                         -- from orders
    `subtotal_usd`               NUMERIC(10,2) COMMENT '小計_ドル',                     -- from orders
    `tax_usd`                    NUMERIC(10,2) COMMENT '税額_ドル',                     -- from orders
    `total_usd`                  NUMERIC(10,2) COMMENT '合計_ドル',                     -- from orders
    `quantity`                   INT COMMENT '数量_個',                        -- from orders
    `status_flag`                       INT COMMENT 'フラグ',                         -- from orders
    `order_created_at`           TIMESTAMP COMMENT '注文_作成日時',              -- from orders
    `parent_id`                  STRING COMMENT '親ID',                        -- from orders

    `ean_code`                   CHAR(13) COMMENT 'EANコード',                 -- from products
    `product_title`              VARCHAR(255) COMMENT '商品_タイトル',           -- from products
    `category`                   VARCHAR(255) COMMENT 'カテゴリ',               -- from products
    `vendor_name`                VARCHAR(255) COMMENT 'ベンダー_名称',           -- from products
    `price_usd`                  NUMERIC(10,2) COMMENT '価格_ドル',                     -- from products
    `rating`                     NUMERIC(10,4) COMMENT '評価_RATING',                -- from products
    `product_created_at`         TIMESTAMP COMMENT '商品_作成日時',              -- from products
    `product_updated_at`         TIMESTAMP COMMENT '商品_更新日時',              -- from products
    `product_is_deleted`         BOOLEAN COMMENT '商品_論理削除フラグ',          -- from products
    `product_deleted_at`         TIMESTAMP COMMENT '商品_論理削除日時',          -- from products

    `address`                    VARCHAR(255) COMMENT '住所',                   -- from people
    `email`                      VARCHAR(255) COMMENT 'メールアドレス',          -- from people
    `password`                   VARCHAR(255) COMMENT 'パスワード',              -- from people
    `user_name`                  VARCHAR(255) COMMENT 'ユーザー名',              -- from people
    `acquisition_channel`        VARCHAR(255) COMMENT 'チャネル_取得元',         -- from people
    `birth_date`                 DATE COMMENT '生年月日',                       -- from people
    `zip_code`                   CHAR(5) COMMENT '郵便番号',                    -- from people
    `user_created_at`            TIMESTAMP COMMENT 'ユーザー_作成日時',          -- from people
    `user_updated_at`            TIMESTAMP COMMENT 'ユーザー_更新日時',          -- from people
    `user_is_deleted`            BOOLEAN COMMENT 'ユーザー_論理削除フラグ',      -- from people
    `user_deleted_at`            TIMESTAMP COMMENT 'ユーザー_論理削除日時',      -- from people

    `ingest_date`                DATE COMMENT 'パーティションキー',             -- partition key
    `user_id_hash`               BIGINT COMMENT 'パーティションキー'        -- partition key
)
USING iceberg
PARTITIONED BY (ingest_date)
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.slv_entities',
    'openlineage.dataset.name'='user_orders_wide',
    'write.format.default'            = 'parquet',
    'format-version'                  = '2',
    'write.distribution-mode'         = 'hash',
    'write.target-file-size-bytes'    = '268435456',
    'history.expire.max-snapshot-age-ms'   = '2592000000',
    'history.expire.min-snapshots-to-keep' = '20'
);

ALTER TABLE local_data_platform.slv_entities.user_orders_wide SET TBLPROPERTIES (
'openlineage.dataset.namespace'='local_data_platform.slv_entities',
'openlineage.dataset.name'='user_orders_wide'
);
