-- 1) 既存があれば消して作り直し（上書きしたくない場合は DROP を外す）
DROP TABLE IF EXISTS iceberg_prod.brz_ingestion.orders_monthly_base;

-- 2) CTAS：baseで定義されたCTEをCTASを利用して物理テーブル化
CREATE TABLE iceberg_prod.brz_ingestion.orders_monthly_base
WITH (
    format = 'PARQUET',
    format_version = 2,
    partitioning = ARRAY['month(month_start)']
) AS
SELECT
    month_start,
    user_id,
    user_revenue_usd,
    ROW_NUMBER() OVER (
        PARTITION BY month_start
        ORDER BY user_revenue_usd DESC
        -- もし同額時に user_id 昇順で決め打ちしたい場合は以下を追加:
        , user_id ASC
    ) AS rn
FROM (
    -- 集計を先にまとめて、上のウィンドウ関数はここに対して実行
    SELECT
        date_trunc('month', created_at) AS month_start,
        user_id,
        SUM(total_usd)                 AS user_revenue_usd
    FROM iceberg_prod.brz_ingestion.orders
    WHERE ingest_date > DATE '2023-12-31'
    GROUP BY 1, 2
);

-- CREATE TABLE: 101 rows

-- Query 20250920_025517_00063_7c5wf, FINISHED, 1 node
-- https://reverse-proxy.local.data.platform/ui/query.html?20250920_025517_00063_7c5wf
-- Splits: 103 total, 103 done (100.00%)
-- CPU Time: 0.2s total,   429 rows/s, 76.6KiB/s, 33% active
-- Per Node: 0.2 parallelism,    89 rows/s, 16.1KiB/s
-- Parallelism: 0.2
-- Peak Memory: 14.1KiB
-- 1.18 [106 rows, 18.9KiB] [89 rows/s, 16.1KiB/s]


WITH top_user_by_month AS (
    SELECT
        year(month_start)          AS year,                -- 年
        month(month_start)         AS month,               -- 月
        user_id                    AS top_user_id,         -- トップユーザーID
        ROUND(user_revenue_usd, 2) AS top_user_revenue_usd -- トップユーザーの月次売上
    FROM iceberg_prod.brz_ingestion.orders_monthly_base
    WHERE rn = 1
    AND month_start <  TIMESTAMP '2028-10-01 00:00:00'
)
SELECT *
FROM top_user_by_month
ORDER BY year, month;
