# Marquez

Marquezは、データリネージとメタデータ管理のためのオープンソースツールです。
どのジョブが、どのデータセットを入力として受け取り、どのテーブルやファイルを出力したのかを可視化できるため、データパイプライン全体の流れを把握しやすくなります。

特に、データの生成元や依存関係、更新の流れを追跡したい場面で有効であり、障害調査や影響範囲の確認、データガバナンスの観点でも役立ちます。

本リポジトリでは、ローカル分析基盤上で動くパイプラインのメタデータを確認し、ジョブとデータセットの関係を可視化する用途で Marquez を利用します。

参考: https://github.com/MarquezProject/marquez/tree/0.50.0

# 起動方法/停止方法

## 起動
git clone https://github.com/MarquezProject/marquez.git
cd marquez
git switch --detach 0.50.0　
./docker/up.sh --build --tag 0.50.0 --detach -a 8661

## 停止
./docker/down.sh -v 

# 接続情報

## ホストからのアクセス

| 項目               | アクセス               | ユーザー | パスワード |
|--------------------|-----------------------------------------------------------------------|----------|------------|
| Marquez UI         | http://localhost:3000/    | -       | -          |


## ワーキングコンテナからのアクセス

プログラム的に利用するエンドポイント

| 項目               | アクセス                                                                |
|--------------------|-----------------------------------------------------------------------|
| APIエンドポイント     |  http://host.docker.internal:5000/api/v1/ |

## 認証/認可
認証認可は設定していません。

## SSL
SSL化なし

# 初期設定
なし

# コマンドスニペット

API経由でメタデータを取得する

curl -s http://host.docker.internal:5000/api/v1/namespaces
curl -s  http://host.docker.internal:5000/api/v1/namespaces/local_data_platform.gld_presentation/datasets?limit=50 | jq .

# (筆者確認)　ARM対応か否か

docker image inspect --format '{{.Architecture}}' e28c2366ca01  
arm64

uname -a
Linux 8bfc29b18f94 6.10.14-linuxkit #1 SMP Tue Apr 15 16:00:54 UTC 2025 aarch64 GNU/Linux
