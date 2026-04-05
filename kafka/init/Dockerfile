FROM curlimages/curl:latest

# 一時的にルート権限を使用
USER root

# jq をインストール
RUN apk add --no-cache jq

# スクリプトをコピー（ホストからコンテナ内へ）
COPY initializer.sh /scripts/initializer.sh

# 実行権限を付与
RUN chmod +x /scripts/initializer.sh

# 初期化スクリプトをデフォルトのコマンドとして設定
CMD ["/scripts/initializer.sh"]
