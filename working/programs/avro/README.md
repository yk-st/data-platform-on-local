
# Avro を利用した Kafka Producer/Consumer

コマンドスニペットおよび確認の仕方をまとめます。

## スキーマレジストリの操作

### ID指定で取得
curl -s "http://schema-registry.local.data.platform:8081/schemas/ids/1"

```
pyspark@work:~$ curl -s "http://schema-registry.local.data.platform:8081/schemas/ids/1" | jq .
{
  "schema": "{\"type\":\"record\",\"name\":\"OrderEvent\",\"namespace\":\"local_data_platform\",\"doc\":\"受注ストリーム (orders_microbatch)\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"doc\":\"ID\"},{\"name\":\"user_id\",\"type\":\"int\",\"doc\":\"ユーザーID\"},{\"name\":\"product_id\",\"type\":\"int\",\"doc\":\"製品ID\"},{\"name\":\"subtotal_amount\",\"type\":\"double\",\"doc\":\"小計-金額\"},{\"name\":\"tax_amount\",\"type\":\"double\",\"doc\":\"税金額\"},{\"name\":\"total_jpy\",\"type\":\"double\",\"doc\":\"合計ー円\"},{\"name\":\"quantity\",\"type\":\"int\",\"doc\":\"数量（個）\"},{\"name\":\"flag\",\"type\":\"int\",\"doc\":\"フラグ\"},{\"name\":\"created_at\",\"type\":\"string\",\"doc\":\"作成日時 (ISO-8601)\"}]}"
}
pyspark@work:~$ 
```

### Version 指定で取得

curl -s "http://schema-registry.local.data.platform:8081/subjects/orders_topic_avro-value/versions/1"

```
pyspark@work:~$ curl -s "http://schema-registry.local.data.platform:8081/subjects/orders_topic_avro-value/versions/1" | jq .
{
  "subject": "orders_topic_avro-value",
  "version": 1,
  "id": 1,
  "schema": "{\"type\":\"record\",\"name\":\"OrderEvent\",\"namespace\":\"local_data_platform\",\"doc\":\"受注ストリーム (orders_microbatch)\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"doc\":\"ID\"},{\"name\":\"user_id\",\"type\":\"int\",\"doc\":\"ユーザーID\"},{\"name\":\"product_id\",\"type\":\"int\",\"doc\":\"製品ID\"},{\"name\":\"subtotal_amount\",\"type\":\"double\",\"doc\":\"小計-金額\"},{\"name\":\"tax_amount\",\"type\":\"double\",\"doc\":\"税金額\"},{\"name\":\"total_jpy\",\"type\":\"double\",\"doc\":\"合計ー円\"},{\"name\":\"quantity\",\"type\":\"int\",\"doc\":\"数量（個）\"},{\"name\":\"flag\",\"type\":\"int\",\"doc\":\"フラグ\"},{\"name\":\"created_at\",\"type\":\"string\",\"doc\":\"作成日時 (ISO-8601)\"}]}"
}
pyspark@work:~$ 

```

### Latest

curl -s "http://schema-registry.local.data.platform:8081/subjects/orders_topic_avro-value/versions/latest" | jq .

```

pyspark@work:~$ curl -s "http://schema-registry.local.data.platform:8081/subjects/orders_topic_avro-value/versions/latest" | jq .
{
  "subject": "orders_topic_avro-value",
  "version": 1,
  "id": 1,
  "schema": "{\"type\":\"record\",\"name\":\"OrderEvent\",\"namespace\":\"local_data_platform\",\"doc\":\"受注ストリーム (orders_microbatch)\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"doc\":\"ID\"},{\"name\":\"user_id\",\"type\":\"int\",\"doc\":\"ユーザーID\"},{\"name\":\"product_id\",\"type\":\"int\",\"doc\":\"製品ID\"},{\"name\":\"subtotal_amount\",\"type\":\"double\",\"doc\":\"小計-金額\"},{\"name\":\"tax_amount\",\"type\":\"double\",\"doc\":\"税金額\"},{\"name\":\"total_jpy\",\"type\":\"double\",\"doc\":\"合計ー円\"},{\"name\":\"quantity\",\"type\":\"int\",\"doc\":\"数量（個）\"},{\"name\":\"flag\",\"type\":\"int\",\"doc\":\"フラグ\"},{\"name\":\"created_at\",\"type\":\"string\",\"doc\":\"作成日時 (ISO-8601)\"}]}"
}
pyspark@work:~$ 

```

### V2の設定

後方互換のチェック(今回はBackwordをちゃんと満たしているか)を行う。
当たらなバージョンを設定し直す際の事前チェックとして有効。

```
curl -s -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data "$(jq -Rs '{schema: .}' /home/pyspark/programs/avro/order_event_v2.avsc)" \
     "http://schema-registry.local.data.platform:8081/compatibility/subjects/orders_topic_avro-value/versions/latest" | \
  jq -e '.is_compatible'
```

True/Falseが返却されるので、それをみて登録予定のAVSCを適宜変更する

```
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data "$(jq -Rs '{schema: .}' /home/pyspark/programs/avro/order_event_v2.avsc)" \
     http://schema-registry.local.data.platform:8081/subjects/orders_topic_avro-value/versions
```

## Sparkと連携したAvro Producer/Consumerの利用

### 確認用プログラムの起動(Consumer)

spark-submit /home/pyspark/programs/avro/consumer.py

### データの送信

以下のコマンドで Avro Producer を起動し、Kafka にデータを送信します。
送信後Consumerが順次データを受信していることを確認します。

#### v1 スキーマで 2 レコード

```
python /home/pyspark/programs/avro/producer.py \
  --bootstrap kafka2.local.data.platform:9093 \
  --registry  http://schema-registry.local.data.platform:8081 \
  --topic     orders_topic_avro \
  --schema-version 1 \
  --count 2 \
  --kafka-username admin \
  --kafka-password admin
```

#### v2 スキーマで 2 レコード

```
python /home/pyspark/programs/avro/producer.py \
  --bootstrap kafka2.local.data.platform:9093 \
  --registry  http://schema-registry.local.data.platform:8081 \
  --topic     orders_topic_avro \
  --schema-version 2 \
  --count 2 \
  --kafka-username admin \
  --kafka-password admin
```

# 既登録 ID = 44 を強制使用（v2 でも v3 でも OK）
python /home/pyspark/programs/avro/producer.py \
  --bootstrap kafka2.local.data.platform:9093 \
  --registry  http://schema-registry.local.data.platform:8081 \
  --topic     orders_topic_avro \
  --schema-id 44 \
  --count 10 \
  --kafka-username admin \
  --kafka-password admin

# 参考情報

スキーマレジストリへの登録は事前に行なっています。
手動での登録は以下のようなコマンドを発行します。

## ❶ subject 互換性を BACKWARD に
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data '{"compatibility":"BACKWARD"}' \
     http://schema-registry.local.data.platform:8081/config/orders_topic_avro-value

## ❷ v1 を登録
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data "$(jq -Rs '{schema: .}' /home/pyspark/programs/avro/order_event_v1.avsc)" \
     http://schema-registry.local.data.platform:8081/subjects/orders_topic_avro-value/versions
