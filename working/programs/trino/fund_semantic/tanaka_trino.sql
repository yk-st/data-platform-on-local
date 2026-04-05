WITH month_end AS (
  SELECT
      fct."fund_sk",
      dim_f."fund_id"             AS fund_id,
      d."date_value"                      AS month_date,
      fct."nav_price_usd"              AS price
  FROM iceberg_prod.slv_fund.fct_fund_performance fct
  JOIN iceberg_prod.slv_fund.dim_date d
        ON fct."date_sk" = d."date_sk"
  JOIN iceberg_prod.slv_fund.dim_fund dim_f
        ON fct."fund_sk" = dim_f."fund_sk"
  WHERE d."year" = 2025
    AND fct."base_date" >= DATE '2025-04-01'
    AND dim_f."fund_id" IN ('FND001','FND002','FND003')
),
month_diff AS (
  SELECT
      fund_id,
      DATE_TRUNC('month', month_date)                        AS month_start,
      LAST_VALUE(price)
        OVER (PARTITION BY fund_id, DATE_TRUNC('month',month_date)
              ORDER BY month_date
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS price_end,
      FIRST_VALUE(price)
        OVER (PARTITION BY fund_id, DATE_TRUNC('month',month_date)
              ORDER BY month_date
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS price_start
  FROM month_end
)
SELECT
    fund_id,
    DATE_ADD('day', -1, DATE_ADD('month', 1, month_start))  AS month_end_date,
    price_end - price_start                                 AS "gross_return_Tanaka"
FROM month_diff
GROUP BY fund_id, month_start, price_end, price_start
ORDER BY fund_id, month_end_date;


--  fund_id | month_end_date | gross_return_Tanaka 
-- ---------+----------------+-------------------
--  FND001  | 2025-04-30     |         -180.5300 
--  FND001  | 2025-05-31     |         -207.7500 
--  FND001  | 2025-06-30     |          336.2000 
--  FND002  | 2025-04-30     |           -8.3000 
--  FND002  | 2025-05-31     |          -99.6600 
--  FND002  | 2025-06-30     |           65.3800 
-- (6 rows)
