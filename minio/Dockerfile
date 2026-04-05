FROM minio/mc:RELEASE.2025-07-16T15-35-03Z

# 環境変数を指定する（必要であれば変更）
ENV MINIO_ROOT_USER=your-root-user
ENV MINIO_ROOT_PASSWORD=your-root-password

# 必要なスクリプトをコンテナにコピー
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# エントリーポイントスクリプトを実行可能にする
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 固定データセットをコンテナにコピーする（例: fixed_dataset ディレクトリ）
COPY fixed_dataset/ /root/fixed_dataset/

# エントリーポイントを設定
ENTRYPOINT ["docker-entrypoint.sh"]
