WITH params AS (
  -- 0.000000001
  SELECT 1e-9 AS eps
),
b_raw AS (
  SELECT COALESCE(acquisition_channel, 'UNKNOWN') AS k, COUNT(*) AS cnt
  FROM iceberg_prod.brz_ingestion.users
  WHERE ingest_date < DATE '2029-01-01'
  GROUP BY COALESCE(acquisition_channel, 'UNKNOWN')
),
b AS (  -- 観測分布
  SELECT k, cnt, cnt * 1.0 / SUM(cnt) OVER () AS b_p
  FROM b_raw
),
ref AS (  -- 基準分布（必要に応じて調整しても、直近の実態から判別するようにしても良い。また参照テーブル化もGood）
  SELECT * FROM (VALUES
    ('Twitter',   0.26),
    ('Facebook',  0.20),
    ('Google',    0.30),
    ('Instagram', 0.24),
    ('UNKNOWN',   0.00)   -- ← 0でもOK（計算時にクランプ）
    -- ('Twitter',   0.16),
    -- ('Facebook',  0.20),
    -- ('Google',    0.40),
    -- ('Instagram', 0.24),
    -- ('UNKNOWN',   0.10)   -- ← 0でもOK（計算時にクランプ）
  ) AS t(k, r_p)
),
joined AS (
  SELECT
    COALESCE(b.k, ref.k)    AS k,
    COALESCE(b.b_p, 0.0)    AS b_p_raw,
    COALESCE(ref.r_p, 0.0)  AS r_p_raw
  FROM b FULL OUTER JOIN ref ON b.k = ref.k
),
clamped AS (
  SELECT
    j.k,
    -- [eps, 1-eps] にクランプ(特定の範囲に収めること)
    -- Greatest(下限を0.000000001に)、Least(上限を1-0.000000001に)
    LEAST(GREATEST(j.b_p_raw, p.eps), 1 - p.eps) AS b_p,
    LEAST(GREATEST(j.r_p_raw, p.eps), 1 - p.eps) AS r_p
  FROM joined j CROSS JOIN params p
),
terms AS (
  SELECT
    k, b_p, r_p,
    -- psiの各要素の計算(内訳を知りたい時)
    (b_p - r_p) * ln(b_p / r_p) AS psi_term
  FROM clamped
)
SELECT
  k,
  ROUND(b_p, 6) AS b_p,
  ROUND(r_p, 6) AS r_p,
  ROUND(psi_term, 6) AS psi_term,
  -- psiの合計(1つの指標として判断したい時)
  ROUND(SUM(psi_term) OVER (), 6) AS psi_total
FROM terms
ORDER BY psi_term DESC;

-- 期待している分布に近い
--      k     | b_p | r_p  | psi_term | psi_total 
-- -----------+-----+------+----------+-----------
--  Google    | 0.2 |  0.3 | 0.040547 |   0.09411 
--  Facebook  | 0.3 |  0.2 | 0.040547 |   0.09411 
--  Instagram | 0.2 | 0.24 | 0.007293 |   0.09411 
--  Twitter   | 0.3 | 0.26 | 0.005724 |   0.09411 
--  UNKNOWN   | 0.0 |  0.0 |      0.0 |   0.09411 


-- 期待している分布から乖離している(基準部分布を結構ずらすとこうなる)
--      k     | b_p | r_p  | psi_term | psi_total 
-- -----------+-----+------+----------+-----------
--  UNKNOWN   | 0.0 |  0.1 | 1.842068 |  2.116542 
--  Google    | 0.2 |  0.4 | 0.138629 |  2.116542 
--  Twitter   | 0.3 | 0.16 | 0.088005 |  2.116542 
--  Facebook  | 0.3 |  0.2 | 0.040547 |  2.116542 
--  Instagram | 0.2 | 0.24 | 0.007293 |  2.116542 