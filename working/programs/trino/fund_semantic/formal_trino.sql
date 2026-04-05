
-- Anatic Base Viewとしてslvへ
CREATE OR REPLACE VIEW iceberg_prod.slv_fund.analytic_vw_fund_daily_return AS
WITH base AS (
  SELECT
    fct."fund_sk",
    dim_f."fund_category"            AS fund_type,
    dim_f."fund_id"               AS fund_id,
    CAST(d."date_value" AS TIMESTAMP)      AS trade_ts,

    /* 単価（1万口あたり）と前日値：非加算 */
    fct."nav_price_usd"                AS nav_per_10k,
    LAG(fct."nav_price_usd") OVER (
      PARTITION BY fct."fund_sk" ORDER BY d."date_value"
    )                                AS nav_prev_per_10k
  FROM iceberg_prod.slv_fund.fct_fund_performance fct
  JOIN iceberg_prod.slv_fund.dim_date d  ON fct."date_sk" = d."date_sk"
  JOIN iceberg_prod.slv_fund.dim_fund dim_f ON fct."fund_sk" = dim_f."fund_sk"
  WHERE d."year" = 2025
    AND fct."base_date" >= DATE '2025-04-01'
    AND dim_f."fund_id" IN ('FND001','FND002','FND003')
),
daily AS (
  SELECT
    fund_id,
    fund_type,
    trade_ts,
    nav_per_10k,
    nav_prev_per_10k,

    /* 半加算（時間のみSUM可）：前日差 */
    (nav_per_10k - nav_prev_per_10k) AS nav_diff_per_10k,

    /* emit：その期間の“最初の行”だけ期首を出す（分母用）*/
    ROW_NUMBER() OVER (PARTITION BY fund_id, date_trunc('month', trade_ts) ORDER BY trade_ts) AS rn_in_month,
    ROW_NUMBER() OVER (PARTITION BY fund_id, date_trunc('year',  trade_ts) ORDER BY trade_ts) AS rn_in_year
  FROM base
  WHERE nav_prev_per_10k IS NOT NULL      -- 初日除外
)
SELECT
  fund_id,
  fund_type,
  trade_ts,

  /* 分子（半加算：時間SUMで期末-期首に一致）*/
  nav_diff_per_10k,

  /* 日次の分母（横断ratio-of-sums用）*/
  nav_prev_per_10k,

  /* 月次の分母：月内の最初の行だけ値をemit（それ以外は0）*/
  CASE WHEN rn_in_month = 1 THEN nav_prev_per_10k ELSE 0 END AS nav_beg_month_emit_per_10k,

  /* 年次の分母：年内の最初の行だけemit */
  CASE WHEN rn_in_year  = 1 THEN nav_prev_per_10k ELSE 0 END AS nav_beg_year_emit_per_10k

FROM daily;

-- Aggregated_viewとしてgldへ
CREATE OR REPLACE VIEW iceberg_prod.gld_fund.agg_vw_fund_return_unified AS
-- Day
SELECT
  fund_id,
  fund_type,
  date_trunc('day', trade_ts)   AS period_start,
  'day'                                       AS period_grain,
  SUM(nav_diff_per_10k)                       AS num_sum,    -- 分子
  SUM(nav_prev_per_10k)                       AS denom_sum   -- 分母（日は prev のSUM）
FROM iceberg_prod.slv_fund.analytic_vw_fund_daily_return
GROUP BY fund_id, fund_type, date_trunc('day', trade_ts)

UNION ALL
-- Month
SELECT
  fund_id,
  fund_type,
  date_trunc('month', trade_ts) AS period_start,
  'month'                                     AS period_grain,
  SUM(nav_diff_per_10k)                       AS num_sum,    -- 分子
  SUM(nav_beg_month_emit_per_10k)             AS denom_sum   -- 分母（月は期首emitのSUM）
FROM iceberg_prod.slv_fund.analytic_vw_fund_daily_return
GROUP BY fund_id, fund_type, date_trunc('month', trade_ts)

UNION ALL
-- Year
SELECT
  fund_id,
  fund_type,
  date_trunc('year', trade_ts) AS period_start,
  'year'                                      AS period_grain,
  SUM(nav_diff_per_10k)                       AS num_sum,    -- 分子
  SUM(nav_beg_year_emit_per_10k)              AS denom_sum   -- 分母（年は年初emitのSUM）
FROM iceberg_prod.slv_fund.analytic_vw_fund_daily_return
GROUP BY fund_id, fund_type, date_trunc('year', trade_ts);



-- 月次ネットリターン（簡易：ratio-of-sums）— 統合AGGを使用
SELECT
  a.fund_id,
  a.period_start AS month_start,
  100 * a.num_sum / NULLIF(a.denom_sum, 0) AS net_return_month_simple
FROM iceberg_prod.gld_fund.agg_vw_fund_return_unified a
WHERE a.period_grain = 'month'
ORDER BY a.fund_id, month_start;