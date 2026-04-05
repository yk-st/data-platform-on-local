/* ──────────────── 佐藤さん（リサーチ部） ────────────────
   月末基準価格差 ÷ 30 － 信託報酬（月割り）
   NULL（前月なし）は 0 に置換
*/
WITH daily AS (
  SELECT
      dim_f."fund_id"                         AS fund_id,
      CAST(d."date_value" AS TIMESTAMP)               AS trade_ts,
      /* 1) 前日との差額（円） */
      fct."nav_price_usd"
        - LAG(fct."nav_price_usd")
            OVER (PARTITION BY fct."fund_sk" ORDER BY d."date_value")
          AS diff_price,
      /* 2) 信託報酬（日割り）── % を率に変換するため /100 */
      LAG(fct."nav_price_usd")
        OVER (PARTITION BY fct."fund_sk" ORDER BY d."date_value")
        * (dim_f."trust_fee_rate" / 100) / 365     AS fee_daily
  FROM iceberg_prod.slv_fund.fct_fund_performance fct
  JOIN iceberg_prod.slv_fund.dim_date            d
       ON fct."date_sk" = d."date_sk"
  JOIN iceberg_prod.slv_fund.dim_fund            dim_f
       ON fct."fund_sk" = dim_f."fund_sk"
  WHERE d."year" = 2025
    AND fct."base_date" >= DATE '2025-04-01'
    AND dim_f."fund_id" IN ('FND001','FND002','FND003')
),
net_daily AS (
  SELECT
      fund_id,
      trade_ts,
      COALESCE(diff_price, 0) - COALESCE(fee_daily, 0) AS net_return_daily
  FROM daily
  WHERE diff_price IS NOT NULL
)
SELECT
    fund_id,
    DATE_ADD('day', -1,
             DATE_ADD('month', 1, DATE_TRUNC('month', trade_ts))
    )                                   AS month_end_date,
    SUM(net_return_daily)               AS net_return_sato_month
FROM net_daily
GROUP BY fund_id,
         DATE_ADD('day', -1,
                  DATE_ADD('month', 1, DATE_TRUNC('month', trade_ts)))
ORDER BY fund_id, month_end_date;


--  fund_id |     month_end_date      | net_return_sato_month 
-- ---------+-------------------------+-----------------------
--  FND001  | 2025-04-30 00:00:00.000 |           -189.330484 
--  FND001  | 2025-05-31 00:00:00.000 |            -36.615130 
--  FND001  | 2025-06-30 00:00:00.000 |            446.642946 
--  FND002  | 2025-04-30 00:00:00.000 |             -8.300000 
--  FND002  | 2025-05-31 00:00:00.000 |              1.400000 
--  FND002  | 2025-06-30 00:00:00.000 |             68.480000 

--v2時点

--  fund_id |     month_end_date      | net_return_sato_month 
-- ---------+-------------------------+-----------------------
--  FND001  | 2025-04-30 00:00:00.000 |           -189.330484 
--  FND001  | 2025-05-31 00:00:00.000 |            -36.615130 
--  FND001  | 2025-06-30 00:00:00.000 |            446.642946 
--  FND001  | 2025-07-31 00:00:00.000 |            632.805484 
--  FND002  | 2025-04-30 00:00:00.000 |             -8.300000 
--  FND002  | 2025-05-31 00:00:00.000 |              1.400000 
--  FND002  | 2025-06-30 00:00:00.000 |             68.480000 
--  FND002  | 2025-07-31 00:00:00.000 |            755.890000 
--  FND003  | 2025-07-31 00:00:00.000 |            900.000000 
-- (9 rows)

-- trino role up

--  fund_id | month_start | net_return_common_month 
-- ---------+-------------+-------------------------
--  FND001  | 2025-04-01  |             -189.330484 
--  FND001  | 2025-05-01  |              -36.615130 
--  FND001  | 2025-06-01  |              446.642946 
--  FND001  | 2025-07-01  |              632.805484 
--  FND002  | 2025-04-01  |               -8.300000 
--  FND002  | 2025-05-01  |                1.400000 
--  FND002  | 2025-06-01  |               68.480000 
--  FND002  | 2025-07-01  |              755.890000 
--  FND003  | 2025-07-01  |              900.000000 