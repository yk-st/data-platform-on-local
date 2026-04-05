-- 顧客ディメンション
CREATE TABLE IF NOT EXISTS local_data_platform.slv_entities.dim_customer (
    customer_sk         BIGINT      COMMENT 'サロゲートキー',
    `user_id`           STRING      COMMENT 'ユーザーID',
    `user_name`         STRING      COMMENT 'ユーザー名',
    `email`             STRING      COMMENT 'メールアドレス',
    `password`          STRING      COMMENT 'パスワード',
    `address`           STRING      COMMENT '住所',
    `acquisition_channel` STRING    COMMENT 'チャネル_取得元',
    `birth_date`        DATE        COMMENT '生年月日',
    `zip_code`          STRING      COMMENT '郵便番号',
    `created_at`        TIMESTAMP   COMMENT '作成日時',
    `updated_at`        TIMESTAMP   COMMENT '更新日時',
    `is_deleted`        BOOLEAN     COMMENT '論理削除フラグ',
    `deleted_at`        TIMESTAMP   COMMENT '論理削除日時',
    ingest_date         DATE        COMMENT 'ロード日'
)
USING iceberg
PARTITIONED BY (days(ingest_date))
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.slv_entities',
    'openlineage.dataset.name'='dim_customer'
);

-- 商品ディメンション
CREATE TABLE IF NOT EXISTS local_data_platform.slv_entities.dim_product (
    product_sk          BIGINT      COMMENT 'サロゲートキー',
    `product_id`        STRING      COMMENT '商品ID',
    `ean_code`          STRING      COMMENT 'EANコード',
    `product_title`     STRING      COMMENT '商品_タイトル',
    `category`          STRING      COMMENT 'カテゴリ',
    `vendor_name`       STRING      COMMENT 'ベンダー_名称',
    `price_usd`         NUMERIC(10,2) COMMENT '価格_ドル',
    `rating`            NUMERIC(10,4) COMMENT '評価_RATING',
    `created_at`        TIMESTAMP   COMMENT '作成日時',
    `updated_at`        TIMESTAMP   COMMENT '更新日時',
    `is_deleted`        BOOLEAN     COMMENT '論理削除フラグ',
    `deleted_at`        TIMESTAMP   COMMENT '論理削除日時',
    ingest_date         DATE        COMMENT 'ロード日'
)
USING iceberg
PARTITIONED BY (days(ingest_date))
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.slv_entities',
    'openlineage.dataset.name'='dim_product'
);

-- 日付ディメンション
CREATE TABLE IF NOT EXISTS local_data_platform.slv_entities.dim_date (
    date_sk             DATE        COMMENT 'YYYY-MM-DD',
    `year`              INT         COMMENT '年',
    `month`             INT         COMMENT '月',
    `day`               INT         COMMENT '日',
    yyyymm              INT         COMMENT '年月',
    `day_of_week`       INT         COMMENT '曜日',
    `is_weekend`        BOOLEAN     COMMENT '週末フラグ'
)
USING iceberg
PARTITIONED BY (years(date_sk))
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.slv_entities',
    'openlineage.dataset.name'='dim_date'
);

-- 注文ファクトテーブル
CREATE TABLE IF NOT EXISTS local_data_platform.slv_entities.fact_orders (
    `order_id`          STRING      COMMENT '注文ID',
    customer_sk         BIGINT      COMMENT '顧客サロゲートキー',
    product_sk          BIGINT      COMMENT '商品サロゲートキー',
    date_sk             DATE        COMMENT '日付サロゲートキー',
    `quantity`          INT         COMMENT '数量_個',
    `subtotal_usd`      NUMERIC(10,2) COMMENT '小計_ドル',
    `tax_usd`           NUMERIC(10,2) COMMENT '税額_ドル',
    `total_usd`         NUMERIC(10,2) COMMENT '合計_ドル',
    `status_flag`              INT         COMMENT 'フラグ',
    `parent_id`         STRING      COMMENT '親ID',
    ingest_date         DATE        COMMENT 'ロード日',
    load_ts             TIMESTAMP   COMMENT 'ロードタイムスタンプ'
)
USING iceberg
PARTITIONED BY (
    days(date_sk),
    bucket(4, customer_sk)
)
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.slv_entities',
    'openlineage.dataset.name'='fact_orders'
);