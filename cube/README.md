# Cube

CubeはTrinoの上にセマンティックレイヤーを構築するためのコンポーネントです。
本リポジトリでは、Iceberg上のファンド収益率データに対してメジャーやディメンションを定義し、BIツールやAPIから再利用しやすい形で公開する用途で利用します。

ローカル環境では、Cube自体はデータを保持する主役ではなく、Trinoをクエリエンジンとして利用しながら、分析用の指標定義を集約する役割を担います。

# 接続情報

## ワーキングコンテナからのアクセス
各種設定に必要な場合は以下を利用してください。

| 項目 | アクセス |
|------|----------|
| Cube GUI | http://localhost:4000 |

## Cube の接続先
Cube コンテナは [docker-compose.yml](./docker-compose.yml) で Trino 接続を設定しています。
接続先の概要は以下の通りです。

| 項目 | 値 |
|------|----|
| DB Type | trino |
| Host | reverse-proxy.local.data.platform |
| Port | 443 |
| Catalog | iceberg_prod |
| Schema | gld_fund |

この構成により、Cube は reverse-proxy 経由で Trino に接続し、Iceberg テーブルやビューに対してセマンティックモデルを提供します。

## 認証/認可
学習用のローカル環境を簡潔に保つため、Cube 側の認証認可は設定していません。

## SSL
Cube GUI 自体は SSL 化していません。
一方で Trino への接続は `CUBEJS_DB_SSL=true` で SSL を有効化しています。

# 初期設定

初期状態で、ファンド収益率を扱うセマンティックモデルを配置済みです。

## 定義済みモデル

### FundReturnABV
- 参照先: `iceberg_prod.slv_fund.analytic_vw_fund_daily_return`
- 用途: 日次の収益率計算に必要な値をベースに、日次・月次・年次の単純収益率を定義
- 特徴: 分子と分母を個別に集計したうえで ratio-of-sums として収益率を算出

### FundReturnAgg
- 参照先: `iceberg_prod.gld_fund.agg_vw_fund_return_unified`
- 用途: 集約済みデータをもとに、粒度をまたいで再利用しやすい収益率指標を定義
- 特徴: day、month、year の粒度をセグメントとして切り替え可能

## モデル配置場所
モデル定義は以下に配置しています。

- [cube/conf/model/FundReturnAvb.js](./cube/conf/model/FundReturnAvb.js)
- [cube/conf/model/FundReturnAgg.js](./cube/conf/model/FundReturnAgg.js)

# 起動前提
Cube を起動する前に、少なくとも以下が利用可能である必要があります。

- reverse-proxy
- trino
- Iceberg カタログに対する参照先データ

起動例:

```bash
cd ./data-platform-on-local/cube
docker compose up -d
```

起動後は Cube GUI からスキーマの確認やクエリの動作確認が可能です。