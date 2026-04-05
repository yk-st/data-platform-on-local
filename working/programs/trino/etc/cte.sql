-- ============================================================
-- 2各月のトップユーザー（Trino / Iceberg）
-- （CTE: base, top_user_by_month）
-- ============================================================
WITH base AS (
    SELECT
            date_trunc('month', created_at)   AS month_start,
            user_id,
            SUM(total_usd)                    AS user_revenue_usd,
            ROW_NUMBER() OVER (
                PARTITION BY date_trunc('month', created_at)
                ORDER BY     SUM(total_usd) DESC
                -- もし同額時にuser_id昇順で決め打ちしたい場合は以下を追加:
                -- , user_id ASC
            ) AS rn
        FROM iceberg_prod.brz_ingestion.orders
        WHERE ingest_date > DATE '2023-12-31'
        GROUP BY date_trunc('month', created_at), user_id
),
top_user_by_month AS (
    SELECT
        year(month_start)                     AS year,                   -- 年
        month(month_start)                    AS month,                  -- 月
        user_id                               AS top_user_id,            -- トップユーザーID
        ROUND(user_revenue_usd, 2)            AS top_user_revenue_usd    -- トップユーザーの月次売上
    FROM (
        SELECT * FROM base
    )
    WHERE rn = 1
)
SELECT *
FROM top_user_by_month
ORDER BY year, month;