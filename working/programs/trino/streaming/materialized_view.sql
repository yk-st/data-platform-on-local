CREATE MATERIALIZED VIEW iceberg_prod.gld_mart.orders_user_daily_mv AS
WITH agg AS (
    SELECT
        DATE(created_at)                    AS as_of_date,
        user_id,
        COUNT(*)                            AS order_count,
        SUM(total_usd)                      AS total_purchase_amount,
        MAX(total_usd)                      AS max_order_amount
    FROM iceberg_prod.brz_ingestion.orders_microbatch_window_raw
    WHERE processing_time >= (CURRENT_TIMESTAMP - INTERVAL '1' DAY) and created_at >= (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
    GROUP BY
        DATE(created_at),
        user_id
)
SELECT
    as_of_date,
    user_id,
    order_count,
    total_purchase_amount,
    max_order_amount,
    ROW_NUMBER() OVER (
        PARTITION BY as_of_date
        ORDER BY total_purchase_amount DESC
    ) AS rank
FROM agg;


-- マテリアライズドビューは明示的に更新しないと新たなデータが反映されないため、定期的に更新する
REFRESH MATERIALIZED VIEW iceberg_prod.gld_mart.orders_user_daily_mv