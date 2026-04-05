-- Trino での Iceberg ↔ PostgreSQL フェデレーション JOIN

EXPLAIN ANALYZE
SELECT
    os."order_id"          AS order_id,
    os."user_id"      AS user_id,
    p."user_name"             AS user_name,
    os."product_id"          AS product_id,
    os."subtotal_usd"          AS subtotal_usd,
    os."tax_usd"          AS tax_usd,
    os."total_usd"          AS total_usd,
    os."quantity"         AS quantity,
    os."status_flag"           AS status_flag,
    os."created_at"        AS ordered_at_utc,
    os.ingest_date,
    p."address"     AS address
FROM iceberg_prod.brz_ingestion.orders   AS os   -- Iceberg テーブル
JOIN postgresql.public.users           AS p    -- PostgreSQL テーブル
  ON CAST(os."user_id" AS VARCHAR) = p."id"
WHERE os.ingest_date >= DATE '2025-06-01'  -- Iceberg 側だけを絞り込み
ORDER BY ordered_at_utc DESC
LIMIT 50;


```
 order_id | user_id | user_name | product_id | subtotal_usd | tax_usd | total_usd | quantity | status_flag |         ordered_at_utc         | ingest_date |               address               
----------+---------+-----------+------------+--------------+---------+-----------+----------+-------------+--------------------------------+-------------+-------------------------------------
 106      |      77 | User 77   |         72 |           11 |       1 |        12 |        1 |           2 | 2025-05-02 06:00:00.000000 UTC | 2025-09-05  | 大阪府大阪市港区3丁目12番6号        
 20       |       5 | User 5    |         55 |           52 |       5 |        58 |        1 |           1 | 2025-05-01 07:03:21.000000 UTC | 2025-09-05  | 東京都中野区4丁目10番27号           
 11       |      71 | User 71   |          2 |           14 |       1 |        15 |        1 |           1 | 2025-04-30 07:03:21.000000 UTC | 2025-09-05  | 大阪府大阪市東住吉区2丁目13番47号   
 3        |      70 | User 70   |         67 |          186 |      19 |       204 |        2 |           1 | 2025-04-29 07:03:21.000000 UTC | 2025-09-05  | 大阪府大阪市東成区2丁目7番39号      
 103      |      70 | User 70   |         67 |         NULL |      19 |       204 |        2 |           2 | 2025-04-29 07:03:21.000000 UTC | 2025-09-05  | 大阪府大阪市東成区2丁目7番39号      
 72       |       8 | User 8    |         84 |           39 |       4 |        43 |        2 |           1 | 2025-04-26 07:03:21.000000 UTC | 2025-09-05  | 大阪府大阪市阿倍野区4丁目6番31号    
 10       |      75 | User 75   |         54 |           71 |       7 |        78 |        4 |           1 | 2025-04-26 07:03:21.000000 UTC | 2025-09-05  | 東京都足立区1丁目10番48号           
 24       |      46 | User 46   |         60 |           96 |      10 |       105 |        2 |           1 | 2025-04-25 07:03:21.000000 UTC | 2025-09-05  | 神奈川県横浜市旭区3丁目3番34号      
 41       |      44 | User 44   |         99 |          391 |      39 |       430 |        5 |           1 | 2025-04-25 07:03:21.000000 UTC | 2025-09-05  | 東京都江戸川区4丁目11番33号          
```

SET SESSION postgresql.join_pushdown_strategy = 'EAGER';