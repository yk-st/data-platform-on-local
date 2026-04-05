#!/bin/sh
set -e

REST_URL="http://rest-proxy:8082"

# ─────────────────────────────────────────
# 1) REST Proxy 起動を待機
# ─────────────────────────────────────────
echo "[wait] REST Proxy ..."
for i in $(seq 1 30); do
  curl -fs "$REST_URL/v3/clusters" >/dev/null 2>&1 && break
  printf '.'
  sleep 2
done
echo

# ─────────────────────────────────────────
# 2) Cluster ID 取得
# ─────────────────────────────────────────
CLUSTER_ID=$(curl -fs "$REST_URL/v3/clusters" | jq -r '.data[0].cluster_id')
if [ -z "$CLUSTER_ID" ] || [ "$CLUSTER_ID" = "null" ]; then
  echo "❌  Cluster ID を取得できませんでした" >&2
  exit 1
fi
echo "✅  Cluster ID = $CLUSTER_ID"

# ─────────────────────────────────────────
# 3) トピック作成（存在しても OK）
# ─────────────────────────────────────────
echo "[create] topic: orders_topic"
curl -fs -u admin:admin -X POST \
  "$REST_URL/v3/clusters/$CLUSTER_ID/topics" \
  -H 'Content-Type: application/json' \
  -d '{
        "topic_name": "orders_topic",
        "partitions_count": 1,
        "replication_factor": 1,
        "configs": [
          {"name": "cleanup.policy", "value": "delete"}
        ]
      }' \
  || echo "ℹ️  既に存在するか作成に失敗しました (無視して続行)"

echo "[create] topic: orders_topic_avro"
curl -fs -u admin:admin -X POST \
  "$REST_URL/v3/clusters/$CLUSTER_ID/topics" \
  -H 'Content-Type: application/json' \
  -d '{
        "topic_name": "orders_topic_avro",
        "partitions_count": 1,
        "replication_factor": 1,
        "configs": [
          {"name": "cleanup.policy", "value": "delete"}
        ]
      }' \
  || echo "ℹ️  既に存在するか作成に失敗しました (無視して続行)"


echo "[create] topic: orders_topic_acl"

curl -fs -u admin:admin -X POST \
  "$REST_URL/v3/clusters/$CLUSTER_ID/topics" \
  -H 'Content-Type: application/json' \
  -d '{
        "topic_name": "orders_topic_acl",
        "partitions_count": 1,
        "replication_factor": 1,
        "configs": [
          {"name": "cleanup.policy", "value": "delete"}
        ]
      }' \
  || echo "ℹ️  既に存在するか作成に失敗しました (無視して続行)"


# ─────────────────────────────────────────
# 4) ACL 付与関数
# ─────────────────────────────────────────
add_acl () {         # $1=resource_type  $2=resource_name  $3=operation
  curl -fs -u admin:admin -X POST \
    "$REST_URL/v3/clusters/$CLUSTER_ID/acls" \
    -H 'Content-Type: application/json' \
    -d "{
          \"resource_type\": \"$1\",
          \"resource_name\": \"$2\",
          \"pattern_type\": \"LITERAL\",
          \"principal\": \"User:kafka\",
          \"host\": \"*\",
          \"operation\": \"$3\",
          \"permission\": \"ALLOW\"
        }" \
    || echo "ℹ️  ACL ($1 $2 $3) は既に存在するか作成に失敗しました"
}

# ─────────────────────────────────────────
# 5) ACL を投入
# ─────────────────────────────────────────
echo "[acl] orders_topic ← User:kafka (Read / Write / Describe)"
for op in READ WRITE DESCRIBE; do
  add_acl TOPIC orders_topic $op
done

echo "[acl] orders-group ← User:kafka (Read)"
add_acl GROUP orders-group READ

echo "🎉  Topic と ACL の初期化が完了しました"

# Schema Registry の初期化

## 後方互換として設定
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data '{"compatibility":"BACKWARD"}' \
     http://schema-registry.local.data.platform:8081/config/orders_topic_avro-value

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data "$(jq -Rs '{schema: .}' /opt/kafka/init/order_event_v1.avsc)" \
     http://schema-registry.local.data.platform:8081/subjects/orders_topic_avro-value/versions

echo "🎉  Regist Schema V1"

# V2スキーマの互換性チェック
echo "[check] V2 スキーマの互換性を確認中..."
COMPATIBILITY_RESPONSE=$(curl -s -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data "$(jq -Rs '{schema: .}' /opt/kafka/init/order_event_v2.avsc)" \
     "http://schema-registry.local.data.platform:8081/compatibility/subjects/orders_topic_avro-value/versions/latest")

# 互換性チェックの結果を取得
IS_COMPATIBLE=$(echo "$COMPATIBILITY_RESPONSE" | jq -r '.is_compatible')

echo "📋 互換性チェック結果: $IS_COMPATIBLE"
echo "📋 レスポンス詳細: $COMPATIBILITY_RESPONSE"

# 互換性がTrueの場合のみV2スキーマを登録
if [ "$IS_COMPATIBLE" = "true" ]; then
    echo "✅ V2スキーマは互換性あり - 登録を実行します"
    
    REGISTER_RESPONSE=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
         --data "$(jq -Rs '{schema: .}' /opt/kafka/init/order_event_v2.avsc)" \
         http://schema-registry.local.data.platform:8081/subjects/orders_topic_avro-value/versions)
    
    SCHEMA_ID=$(echo "$REGISTER_RESPONSE" | jq -r '.id')
    
    if [ "$SCHEMA_ID" != "null" ] && [ -n "$SCHEMA_ID" ]; then
        echo "🎉 V2スキーマ登録成功 - Schema ID: $SCHEMA_ID"
    else
        echo "❌ V2スキーマ登録失敗: $REGISTER_RESPONSE"
        exit 1
    fi
else
    echo "❌ V2スキーマは互換性なし - 登録をスキップします"
    echo "💡 互換性エラーの詳細:"
    echo "$COMPATIBILITY_RESPONSE" | jq '.'
    
    # 互換性がない場合でも処理を続行するか、エラーで停止するかを選択
    # exit 1  # エラーで停止する場合
    echo "⚠️  V2スキーマの登録をスキップして続行します"
fi

# products_contract

curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data '{"compatibility":"BACKWARD"}' \
     http://schema-registry.local.data.platform:8081/config/products_contract_avro-value

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data "$(jq -Rs '{schema: .}' /opt/kafka/init/products_contract.avsc)" \
     http://schema-registry.local.data.platform:8081/subjects/products_contract_avro-value/versions

echo "🎉 Schema Registry の初期化が完了しました"