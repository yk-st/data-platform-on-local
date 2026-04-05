#!/bin/sh

# MinIOホストの設定
echo "Configuring MinIO..."
until /usr/bin/mc alias set myminio http://minio.local.data.platform:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
  echo "...waiting for MinIO to be available..."
  sleep 1
done

# バケットの作成と設定
echo "Creating and configuring buckets..."
/usr/bin/mc mb myminio/local-data-platform
/usr/bin/mc anonymous set none myminio/local-data-platform

/usr/bin/mc mb myminio/dev-local-data-platform
/usr/bin/mc anonymous set none myminio/dev-local-data-platform

/usr/bin/mc mb myminio/data-source
/usr/bin/mc anonymous set none myminio/data-source

/usr/bin/mc mb myminio/misc-data-platform
/usr/bin/mc anonymous set none myminio/misc-data-platform

# 監査ログ用のバケット作成
# /usr/bin/mc mb myminio/audit-logs
# /usr/bin/mc retention set myminio/audit-logs --retention-mode governance --retention-duration 7d
# /usr/bin/mc mb myminio/audit-logs.trino
# /usr/bin/mc retention set myminio/audit-logs.trino --retention-mode governance --retention-duration 7d

/usr/bin/mc mb myminio/process-bucket

# # ブロンズ
# /usr/bin/mc mb myminio/data-platform-prod-bronze
# /usr/bin/mc mb myminio/data-platform-test-bronze

# # シルバー
# /usr/bin/mc mb myminio/data-platform-prod-silver
# /usr/bin/mc mb myminio/data-platform-test-silver

# # ゴールド
# /usr/bin/mc mb myminio/data-platform-prod-gold
# /usr/bin/mc mb myminio/data-platform-test-gold

# /usr/bin/mc mb myminio/data-platform-sql

# ダミーファイルを作成して配置（Sparkのイベント保存用のディレクトリ）
echo "Placing dummy files..."
echo "KEEP" > /tmp/keep
/usr/bin/mc cp /tmp/keep myminio/process-bucket/spark-events/keep
/usr/bin/mc cp /tmp/keep myminio/process-bucket/spark-history/keep

# データセットのアップロード
echo "Uploading dataset..."
/usr/bin/mc cp /root/fixed_dataset/ myminio/data-source/ --recursive

# アクセスキーの作成
echo "Creating additional access credentials..."
/usr/bin/mc admin accesskey create myminio/ $MINIO_ROOT_USER --name initcred --access-key 5nCJP6jHFJd7PDsLlT3a --secret-key FXn6MFKDbNamyMzxiMBGIpgTDFu2r1IfymESfRJd

cat > /tmp/initcred-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::dev-local-data-platform",
        "arn:aws:s3:::misc-data-platform",
        "arn:aws:s3:::process-bucket"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListMultipartUploadParts",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::dev-local-data-platform/*",
        "arn:aws:s3:::misc-data-platform/*",
        "arn:aws:s3:::process-bucket/*"
      ]
    }
  ]
}
EOF

# ② access-key を root ユーザー ($MINIO_ROOT_USER) に追加
/usr/bin/mc admin accesskey create myminio/ $MINIO_ROOT_USER \
  --name        initcred \
  --access-key  6nCJP6jHFJd7PDsLlT3a \
  --secret-key  GXn6MFKDbNamyMzxiMBGIpgTDFu2r1IfymESfRJd \
  --policy      /tmp/initcred-policy.json

echo "MinIO setup completed."

# ベースイメージのデフォルトコマンドを実行
exec "$@"
