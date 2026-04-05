#!/bin/sh

# 環境変数の設定
KEYSTORE_PATH="/opt/java/openjdk/lib/security/cacerts"
CERT_FILE="/root/server.crt"
STORE_PASS="changeit"
ALIAS="reverse-proxy"

# 証明書がマウントされていることを確認
if [ ! -f "$CERT_FILE" ]; then
  echo "証明書ファイルが見つかりません: $CERT_FILE"
  exit 1
fi

# 証明書をキーストアにインポート
echo "証明書をインポートしています..."
yes | keytool -importcert -trustcacerts \
  -keystore "$KEYSTORE_PATH" \
  -storepass "$STORE_PASS" \
  -file "$CERT_FILE" \
  -alias "$ALIAS"

echo "証明書のインポートが完了しました！"

# Metabaseを起動
java -jar metabase.jar &


# 管理者情報の設定
ADMIN_EMAIL=${MB_ADMIN_EMAIL:-admin@metabase.local}
ADMIN_PASSWORD=${MB_ADMIN_PASSWORD:-Metapass123}
METABASE_HOST=${MB_HOSTNAME:-localhost}
METABASE_PORT=${MB_PORT:-13000}

echo "⌚︎ Waiting for Metabase to start"
# Metabaseが起動するまで待機
while (! curl -s -m 5 http://${METABASE_HOST}:${METABASE_PORT}/api/session/properties -o /dev/null); do sleep 5; done

echo "😎 Creating admin user"

SETUP_TOKEN=$(curl -s -m 5 -X GET \
    -H "Content-Type: application/json" \
    http://${METABASE_HOST}:${METABASE_PORT}/api/session/properties \
    | jq -r '.["setup-token"]'
)

MB_TOKEN=$(curl -s -X POST \
    -H "Content-type: application/json" \
    http://${METABASE_HOST}:${METABASE_PORT}/api/setup \
    -d '{
    "token": "'${SETUP_TOKEN}'",
    "user": {
        "email": "'${ADMIN_EMAIL}'",
        "first_name": "Metabase",
        "last_name": "Admin",
        "password": "'${ADMIN_PASSWORD}'"
    },
    "prefs": {
        "allow_tracking": false,
        "site_name": "Metawhat"
    }
}' | jq -r '.id')

# 管理者ユーザーでログインし、セッションIDを取得
SESSION_ID=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d '{
        "username": "'${ADMIN_EMAIL}'",
        "password": "'${ADMIN_PASSWORD}'"
    }' \
    http://${METABASE_HOST}:${METABASE_PORT}/api/session | jq -r '.id')

echo "🔑 Retrieved session ID: ${SESSION_ID}"

# データソース(Spark)
curl -s -X POST \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: ${SESSION_ID}" \
  http://${METABASE_HOST}:${METABASE_PORT}/api/database \
  -d '{
        "engine": "sparksql",
        "name": "SparkSQL Data Source",
        "details": {
            "host": "spark-thrift-server.local.data.platform",
            "port": 10000,
            "db": "default",
            "user": "pyspark@local.data.platform",
            "password": "pyspark",
            "additional-options": ""
        }
      }'

echo "🔑 CREATED DATASOURCE(SPARK SQL)"

# データソース(trino)
curl -s -X POST \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: ${SESSION_ID}" \
  http://${METABASE_HOST}:${METABASE_PORT}/api/database \
  -d '{
        "engine": "starburst",
        "name": "Trino Connector",
        "details": {
            "host": "reverse-proxy.local.data.platform",
            "port": 443,
            "catalog": "iceberg_prod",
            "schema": "gld_mart",
            "user": "pyspark@local.data.platform",
            "password": "pyspark",
            "use-ssl": true,
            "additional-options": ""
        }
      }' 

echo "🔑 CREATED DATASOURCE(STAR BURST)"

# フォアグラウンドでMetabaseを実行
wait
