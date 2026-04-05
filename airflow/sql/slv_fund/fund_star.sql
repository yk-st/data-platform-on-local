/* ファンド次元（SCD2） */
CREATE TABLE IF NOT EXISTS local_data_platform.slv_fund.dim_fund (
    `fund_sk`           BIGINT COMMENT 'ファンドサロゲートキー',
    `fund_id`           STRING NOT NULL COMMENT 'ファンドID',
    `fund_category`     STRING COMMENT '投資信託_分類',
    `trust_fee_rate` DECIMAL(5,2) COMMENT '信託報酬_率',
    `is_active`         BOOLEAN COMMENT '有効フラグ',
    `effective_start_date` DATE COMMENT '効力開始日',
    `effective_end_date`   DATE COMMENT '効力終了日',
    `is_current`        BOOLEAN COMMENT '現在フラグ'
)
USING ICEBERG
TBLPROPERTIES (
    'format-version'    = '2',
    'identifier-fields' = 'fund_id',
    'write.format.default' = 'parquet',
    'openlineage.dataset.namespace'='local_data_platform.slv_fund',
    'openlineage.dataset.name'='dim_fund'
)
PARTITIONED BY (
    bucket(8, `fund_id`)
);

/* 日付ディメンション */
CREATE TABLE IF NOT EXISTS local_data_platform.slv_fund.dim_date (
    `date_sk`           INT COMMENT '日付サロゲートキー',
    `date_value`        DATE COMMENT '日付',
    `year`              SMALLINT COMMENT '年',
    `month`             TINYINT COMMENT '月',
    `day`               TINYINT COMMENT '日',
    `day_of_week`       SMALLINT COMMENT '曜日',
    `is_weekend`        BOOLEAN COMMENT '週末フラグ',
    `is_month_end`      BOOLEAN COMMENT '月末フラグ',
    `year_month`        CHAR(6) COMMENT '年月',
    `fiscal_year`       SMALLINT COMMENT '会計年度'
)
USING ICEBERG
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.slv_fund',
    'openlineage.dataset.name'='dim_date',
    'format-version'    = '2',
    'write.format.default' = 'parquet'
)
PARTITIONED BY (`year`);

/* KPI ファクト */
CREATE TABLE IF NOT EXISTS local_data_platform.slv_fund.fct_fund_performance (
    `date_sk`           INT COMMENT '日付サロゲートキー',
    `fund_sk`           BIGINT COMMENT 'ファンドサロゲートキー',
    `base_date`         DATE COMMENT '基準日',
    `nav_price_usd`     DECIMAL(18,4) COMMENT '基準価額_米ドル',
    `price_change_usd`  DECIMAL(18,4) COMMENT '騰落額_米ドル',
    `return_rate_pct`   DECIMAL(10,6) COMMENT '騰落率_%'
)
USING ICEBERG
TBLPROPERTIES (
    'openlineage.dataset.namespace'='local_data_platform.slv_fund',
    'openlineage.dataset.name'='fct_fund_performance',
    'format-version'    = '2',
    'write.format.default' = 'parquet'
)
PARTITIONED BY (months(`base_date`));