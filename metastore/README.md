<p align="center"><b>以下の書籍に関するリポジトリです。</b></p>

<p align="center">
    <a href="https://www.amazon.co.jp/dp/4297145634/ref=sspa_dk_detail_0?psc=1&pd_rd_i=4297145634&pd_rd_w=BXEhW&content-id=amzn1.sym.f293be60-50b7-49bc-95e8-931faf86ed1e&pf_rd_p=f293be60-50b7-49bc-95e8-931faf86ed1e&pf_rd_r=VZ7P7XN3YX1NAMJAPZEB&pd_rd_wg=CuOVv&pd_rd_r=31953068-34be-40e1-978d-b417f6b20227&s=books&sp_csd=d2lkZ2V0TmFtZT1zcF9kZXRhaWw">
        <img alt="エンジニアのためのデータ分析基盤 基本編" src="../書影.png" width="150px" style="margin-right: 100px;">
    </a>
    <a href>
        <img alt="エンジニアのためのデータ分析基盤 実践編" src="../title.jpg" width="150px">
    </a>
</p>

# Metastore

Metastoreは、以下のようなメタデータを管理するためのサービスです：

テーブル定義（スキーマ、列名、データ型など）
データベース情報
パーティション情報
ストレージの場所（HDFSやS3などのURI）
アクセス権限や認証情報
これらのメタデータは、HiveQLやその他のクエリ言語を実行する際に必要です。

今回はメタストアとしてPostgresSQLを利用します。

## Metastore thrift

Metastore Thriftは、Apache Hiveや関連するビッグデータエコシステムで利用されるMetastoreサービスのプロトコルです。このサービスは、Hadoopやその他のデータ処理システムで使用されるデータカタログ（メタデータの管理システム）を操作するための中心的な役割を果たします。

Metastore Thriftサービスは、Hiveや他のデータエンジン（Spark、Presto、Impalaなど）がMetastoreにアクセスするためのインターフェースを提供します。クライアントは、このサービスを通じて以下の操作を行えます：

テーブルやデータベースの作成、変更、削除
パーティション情報の取得と操作
ストレージ情報の管理
メタデータのクエリや更新

例えば、Thrift経由でSparkから接続したり、Openmetadataから接続したり、Trinoから接続したりと統一的にメタデータへのアクセス
を行えるようになります。

## 注意事項
今回はメタストアのバージョンとして、Hive 4.0.0を利用しています。
本来、Spark3.5.5ではHive3.13までのサポートですが、説明の都合上は影響がないのでHive4.0.0を利用しています。
Hive4.0.1となるとget-tableなどのAPIが変更されているため、Spark3.5.5ではHive4.0.1は利用できません。

# 接続情報

## ワーキングコンテナからのアクセス
各種設定に必要な場合は以下を利用してください。

| 項目                   | アクセス                                                              |
|------------------------|-----------------------------------------------------------------|
| Metastore Thrift URL   | thrift://metastore-thrift.local.data.platform:9083             |
| Metastore DB URL   |  psql -h metastore-db -U hive -d metastore -p 9001      |

パスワードは`hive`です。

## 認証/認可
認証認可は設定していません。

kerberosを設定することで、認証をONにすることが可能です。
認可はRangerと連携することで設定が可能です。

## SSL
SSL化なし

# 初期設定
なし

# オペレーション

HMSに保存されたテーブル定義の確認を行うことができます。

非Iceberg管理テーブルの確認

```
SELECT 
  d."NAME" as DATABASE_NAME,
  t."TBL_NAME" as TABLE_NAME,
  c."COLUMN_NAME",
  c."TYPE_NAME",
  c."COMMENT" as COLUMN_COMMENT,
  p."PART_NAME" as PARTITION_VALUES,
  ps."LOCATION" as PARTITION_LOCATION
FROM "TBLS" t
  JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
  JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
  JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
  LEFT JOIN "PARTITION_KEYS" pk ON t."TBL_ID" = pk."TBL_ID"
  LEFT JOIN "PARTITIONS" p ON t."TBL_ID" = p."TBL_ID"
  LEFT JOIN "SDS" ps ON p."SD_ID" = ps."SD_ID"
WHERE t."TBL_NAME" = 'legacy_fund_master'
ORDER BY c."INTEGER_IDX", pk."INTEGER_IDX", p."PART_NAME";
```

Icebergテーブルの識別とmetadata.jsonロケーション確認

```
SELECT 
  d."NAME" as DATABASE_NAME,
  t."TBL_NAME" as TABLE_NAME,
  t."TBL_TYPE",
  tp."PARAM_KEY" as PROPERTY_KEY,
  tp."PARAM_VALUE" as PROPERTY_VALUE
FROM "TBLS" t
  JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
  JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
  LEFT JOIN "TABLE_PARAMS" tp ON t."TBL_ID" = tp."TBL_ID"
WHERE t."TBL_NAME" = 'fund_master'
  AND (tp."PARAM_KEY" LIKE '%iceberg%' 
       OR tp."PARAM_KEY" = 'table_type'
       OR tp."PARAM_KEY" = 'metadata_location'
       OR tp."PARAM_KEY" = 'current-snapshot-id'
       OR tp."PARAM_KEY" = 'uuid')
ORDER BY tp."PARAM_KEY";

```