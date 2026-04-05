#!/bin/bash

set -a; . /run/secrets/my_secret; set +a;

# 環境変数の設
#!/bin/bash

# 現在のアーキテクチャを取得
ARCH=$(uname -m)

# JAVA_HOMEのパスを設定
if [[ "$ARCH" == "x86_64" ]]; then
    JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
elif [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
    JAVA_HOME="/usr/lib/jvm/java-17-openjdk-arm64"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

# KEYSTORE_PATHを設定
KEYSTORE_PATH="$JAVA_HOME/lib/security/cacerts"
CERT_FILE="/home/pyspark/server.crt"
STORE_PASS="changeit"
ALIAS="reverse-proxy"

# 証明書がマウントされていることを確認
if [ ! -f "$CERT_FILE" ]; then
  echo "証明書ファイルが見つかりません: $CERT_FILE"
  exit 1
fi

# 証明書をキーストアにインポート
echo "証明書をインポートしています..."
yes | sudo keytool -importcert -trustcacerts \
  -keystore "$KEYSTORE_PATH" \
  -storepass "$STORE_PASS" \
  -file "$CERT_FILE" \
  -alias "$ALIAS"

echo "証明書のインポートが完了しました！"

# actのインストール
# gh extension install https://github.com/nektos/gh-act

# 実行後に保持するための対策（オプション）
exec bash
