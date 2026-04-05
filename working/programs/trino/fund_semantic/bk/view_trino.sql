CREATE OR REPLACE VIEW iceberg_prod.gld_fund.vw_fund_daily_return AS
WITH base AS (
  SELECT
    fct."ファンドSK",
    dim_f."投資信託_分類"                AS fund_type,
    dim_f."ファンドID"                         AS fund_id,
    CAST(d."日付" AS TIMESTAMP)               AS trade_ts,
    /* 1) 前日との差額（円） */
    fct."基準価額_円"
      - LAG(fct."基準価額_円")
          OVER (PARTITION BY fct."ファンドSK" ORDER BY d."日付") AS diff_price,
    /* 2) 信託報酬（日割り）：前日価額×(率/100)/365 */
    LAG(fct."基準価額_円")
        OVER (PARTITION BY fct."ファンドSK" ORDER BY d."日付")
      * (dim_f."信託報酬_率" / 100) / 365                      AS fee_daily
  FROM iceberg_prod.gld_fund.fct_fund_performance  fct
  JOIN iceberg_prod.gld_fund.dim_date              d
       ON fct."日付SK" = d."日付SK"
  JOIN iceberg_prod.gld_fund.dim_fund              dim_f
       ON fct."ファンドSK" = dim_f."ファンドSK"
  WHERE d."年" = 2025
    AND fct."基準日" >= DATE '2025-04-01'
    AND dim_f."ファンドID" IN ('FND001','FND002','FND003')
)
SELECT
  fund_id,
  trade_ts,
  fund_type,
  /* diff_price NULL（シリーズ初日）は 0 とみなす */
  COALESCE(diff_price, 0) - COALESCE(fee_daily, 0) AS net_return_common_daily
FROM base
WHERE diff_price IS NOT NULL;   -- 初日の行は除外（3/31 データが無い想定）


-- 月次集計のロールアップ
SELECT
  fund_id,
  CAST(date_trunc('month', trade_ts) AS DATE)   AS month_start,
  SUM(net_return_common_daily)                   AS net_return_common_month
FROM iceberg_prod.gld_fund.vw_fund_daily_return
GROUP BY fund_id, date_trunc('month', trade_ts)
ORDER BY fund_id, month_start;