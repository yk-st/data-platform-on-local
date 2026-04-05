
trino> select * from iceberg_prod.gld_fund.fct_fund_performance where "基準日" > DATE '2024-04-01';
    -> 

  日付sk  |     ファンドsk      |   基準日   | 基準価額_円 | 騰落額_円 | 騰落率_%  
----------+---------------------+------------+-------------+-----------+-----------
 20250601 |  727761285675220583 | 2025-06-01 |  10944.6500 |  119.5800 |  1.104658 
 20250602 |  727761285675220583 | 2025-06-02 |  10884.6600 |  -59.9900 | -0.548122 
 20250603 |  727761285675220583 | 2025-06-03 |  10934.3300 |   49.6700 |  0.456330 
 20250604 |  727761285675220583 | 2025-06-04 |  11013.9400 |   79.6100 |  0.728074 
略

マスターV1時点
trino> select * from iceberg_prod.gld_fund.dim_fund where "ファンドID" IN ('FND001','FND002','FND003');
     ファンドsk      | ファンドid | 投資信託_分類 | 信託報酬_率 | 有効フラグ | 効力開始日 | 効力終了日 | 現在フラグ 
---------------------+------------+---------------+-------------+------------+------------+------------+------------
  727761285675220583 | FND001     | ACTIVE        |        1.20 | true       | 2025-07-10 | 9999-12-31 | true       
 7273158674654827435 | FND002     | INDEX         |        0.30 | true       | 2025-07-10 | 9999-12-31 | true       

trino> select * from iceberg_prod.gld_fund.dim_fund where "ファンドID" IN ('FND001','FND002','FND003');
      ファンドsk      | ファンドid | 投資信託_分類 | 信託報酬_率 | 有効フラグ | 効力開始日 | 効力終了日 | 現在フラグ 
----------------------+------------+---------------+-------------+------------+------------+------------+------------
   727761285675220583 | FND001     | ACTIVE        |        1.20 | true       | 2025-07-10 | 9999-12-31 | true       
  7273158674654827435 | FND002     | INDEX         |        0.30 | true       | 2025-07-10 | 2025-07-10 | false      
 -8739698191845768069 | FND002     | INDEX         |        0.25 | true       | 2025-07-10 | 9999-12-31 | true       
  5139098686053517633 | FND003     | INDEX         |        0.15 | true       | 2025-07-10 | 9999-12-31 | true    

select * from iceberg_prod.gld_fund.dim_date where "年" = 2025;

trino> select * from iceberg_prod.gld_fund.dim_date where "年" = 2025;
  日付sk  |    日付    |  年  | 月 | 日 | 曜日 | 週末フラグ | 月末フラグ |  年月  | 会計年度 
----------+------------+------+----+----+------+------------+------------+--------+----------
 20250520 | 2025-05-20 | 2025 |  5 | 20 |    3 | false      | false      | 202505 |     2025 
 20250605 | 2025-06-05 | 2025 |  6 |  5 |    5 | false      | false      | 202506 |     2025 
 20250613 | 2025-06-13 | 2025 |  6 | 13 |    6 | false      | false      | 202506 |     2025 
 20250404 | 2025-04-04 | 2025 |  4 |  4 |    6 | false      | false      | 202504 |     2025 
 20250406 | 2025-04-06 | 2025 |  4 |  6 |    1 | true       | false      | 202504 |     2025 
 略



 # RLS/CLSの確認

./trino --server https://reverse-proxy.local.data.platform --debug --user pyspark@local.data.platform --password
SELECT "ユーザーID", "パスワード" FROM iceberg_prod.slv_analytics.user_orders_wide
WHERE ingest_date > DATE '2025-07-01' and "ユーザーID" BETWEEN 10 AND 15 LIMIT 3;  

 ユーザーID | パスワード 
------------+------------
         12 | pass12     
         15 | pass15     
         10 | pass10     
(3 rows)

./trino --server https://reverse-proxy.local.data.platform --debug --user user1@local.data.platform --password

SELECT COUNT(*) FROM iceberg_prod.slv_analytics.user_orders_wide
WHERE ingest_date > DATE '2025-07-01' and "ユーザーID" BETWEEN 10 AND 15 LIMIT 3;  

 _col0 
-------
     0 
(1 row)

select * from iceberg_prod.gld_fund.dim_date where "年" = 2025;

Query 20250711_080935_00008_4as9q failed: Access Denied: Cannot select from table gld_fund.dim_date
io.trino.spi.security.AccessDeniedException: Access Denied: Cannot select from table gld_fund.dim_date
        at io.trino.spi.security.AccessDeniedException.denySelectTable(AccessDeniedException.java:361)


SELECT "ユーザーID", "パスワード"
FROM iceberg_prod.slv_analytics.user_orders_wide
WHERE ingest_date > DATE '2025-07-01' and "ユーザーID" = 5
LIMIT 1;

 ユーザーID | パスワード 
------------+------------
          5 | *****      
(1 row)