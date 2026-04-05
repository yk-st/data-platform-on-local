README.mdのテンプレイメージ

# はじめに

以下の書籍に関するリポジトリです。

<div style="text-align: center;">
    <figure style="display: inline-block; margin: 0 20px; text-align: center;">
        <a href="https://www.amazon.co.jp/dp/4297145634/ref=sspa_dk_detail_0?psc=1&pd_rd_i=4297145634&pd_rd_w=BXEhW&content-id=amzn1.sym.f293be60-50b7-49bc-95e8-931faf86ed1e&pf_rd_p=f293be60-50b7-49bc-95e8-931faf86ed1e&pf_rd_r=VZ7P7XN3YX1NAMJAPZEB&pd_rd_wg=CuOVv&pd_rd_r=31953068-34be-40e1-978d-b417f6b20227&s=books&sp_csd=d2lkZ2V0TmFtZT1zcF9kZXRhaWw">
            <img alt="エンジニアのためのデータ分析基盤 基本編" src="../書影.png" width="150px" style="margin-right: 10px;">
        </a>
         <figcaption>基本編</figcaption>
    </figure>
    <figure style="display: inline-block; margin: 0 20px; text-align: center;">
        <a href>
            <img alt="エンジニアのためのデータ分析基盤 実践編" src="../title.jpg" width="150px">
        </a>
         <figcaption>実践編</figcaption>
    </figure>
</div>

# Trino
Trinoは、複数のデータソースに対してSQLで横断的に問い合わせできる分散SQLクエリエンジンです。
データを1箇所へ集約しなくても、Iceberg、Hive、PostgreSQL、Kafkaなどの異なるシステムに対して同じSQLインターフェースでクエリできる点が大きな特徴です。

TrinoはOLAPや対話的な分析を得意としており、大量データに対してもスケールアウトしながら高速にクエリを実行できます。
ETL のようなデータ加工処理にも利用できますが、特にBIツールや分析用途のクエリエンジンとして広く使われています。

構成としては、SQLの受け付けや実行計画の生成を行うCoordinatorと実際の処理を分担するWorkerで役割が分かれています。
CoordinatorはWorker 群を管理しながらクエリ全体を制御し、Workerはsplitと呼ばれる処理単位ごとに並列実行を担当します。

本リポジトリではTrinoをローカル分析基盤の共通クエリエンジンとして利用しています。
Icebergテーブルへのアクセスを中心に、必要に応じて異なるデータソースを横断しながらSQLベースでデータを参照できるようにしています。


## カタログ、スキーマ、テーブル
Trinoは、カタログ（コネクタと考えても良い）、スキーマ(いわゆるデータベースと考えても良い)、テーブル(テーブル)のように分かれている。

つまり

select * from catalog.schema.tableという形でのSQLとなるが、フェデーレーションが得意であることから
select * from hive.hoge.aaaa unionall select * from postgres.hoge.bbbbのような複数のサービスに跨ったSQLを実行することが可能。

ETL用ツールとしても利用可能だが、主にBIツールなどからの分析用途での利用に人気がある。

## 類似サービス
OLAP分析が得意という点に関しては、Clickhouseやpinotなども有名である。

# 接続情報

## ホストからのアクセス

| 項目               | アクセス                                                                | ユーザー | パスワード |
|--------------------|-----------------------------------------------------------------------|----------|------------|
| Trino Cluster OverView| https://localhost/ui/     | LDAPユーザ       | LDAPパスワード          |


自己証明書のためブラウザで警告が出ますが無視で問題ありません。

## ワーキングコンテナからのアクセス

プログラム的に利用するエンドポイント

| 項目               | アクセス                                                                |
|--------------------|-----------------------------------------------------------------------|
| Trino Serverへの接続     |  https://reverse-proxy.local.data.platform  |
| Trino JDBC    |  jdbc:trino://reverse-proxy.local.data.platform:443  |


接続コマンド例

```
./trino --server https://reverse-proxy.local.data.platform --debug --user pyspark@local.data.platform --password

```

Trinoへの接続はリバースプロキシ経由で行います。

## 認証/認可
認証は、[LDAP](../openldap/README.md)と連携し、LDAPに登録されたユーザでの認証を行います。
認可は、詳細は本書内で説明しますが、LDAPユーザーのグループ(ロール)ごとのアクセス制御を実施しています。

## SSL
TrinoはSSL通信を前提として起動しています。
SSL通信はリバースプロキシとして設定したnginx(reverse-proxyのREADME.md参照)で終端しTrinoクラスターへ接続する形です。
そのため、Trinoクラスター自体はHttp通信です。

# 初期設定

大きく以下の設定をしています。

1. HSMを経由したIceberg Tableへの接続(iceberg_prod)
2. データソース側のpostgresへの接続(postgresql)
3. Ldapによる認証・認可設定

# コマンドスニペット

## CLI経由での接続

作業はworkコンテナから行います。

workコンテナに配置されたtrino ClIツールから以下のようなコマンドを発行し接続します。
--userはOpen Ldapのユーザを指定し、パスワードも同様です。

```
./trino --server https://reverse-proxy.local.data.platform --debug --user pyspark@local.platform --password
```

また、Rangerによる認可を設定しているため認可を設定しなければログインできてもクエリを実行することはできません。
本書や[Apache RangerのREADME.md](../ranger/README.md)を確認しながら設定を行なってください。

## クエリの実行

認証/認可の設定が完了後
以下のようにcatalog.schema.tableの形でSQLを実行することが可能です。

```

# hiveのhogeoスキーマのmy_table2からデータを取得するクエリ
trino> select * from hive.hogeo.my_table2;
 id |  value   
----+----------
  1 | example1 
  2 | example2 
(2 rows)

## システム情報を取得するクエリ
trino> select * from system.runtime.nodes;
   node_id    |        http_uri         | node_version | coordinator | state  
--------------+-------------------------+--------------+-------------+--------
 9b93ea114d82 | http://172.28.0.14:8080 | 465          | true        | active 
(1 row)

```

# コンテナの起動とログイン
対象のディレクトリへ移動し以下のコマンド

コンテナの起動

```
docker compose up -d
```

コンテナのログイン

ターミナルより以下のコマンド。
docker compose に指定しているホスト名を指定する。

例:
```
docker exec -it trino.local.data.platform /bin/bash
```


# クエリスニペット


```

trino> show schemas in iceberg_prod;
       Schema       
--------------------
 brz_ingestion      
 default            
 gld_fund           
 gld_mart           
 information_schema 
 legacy             
 ref                
 slv_entities       
 slv_fund           
 system             
(10 rows)

```

```

trino> show tables in iceberg_prod.ref;
      Table       
------------------
 age_group_ref    
 column_alias_map 
 zip_geocode      
 zip_taxonomy_pt  
(4 rows)


```


```


trino> select * from iceberg_prod.ref.age_group_ref;
 age_code | age_label 
----------+-----------
 A1       | 10代      
 A2       | 20代      
 A3       | 30代      
 A4       | 40代      
 A5       | 50代      
 A6       | 60代      
 A7       | 70代      
 A8       | 80代      
 A9       | 90代      
 A10      | 100超     
(10 rows)

```