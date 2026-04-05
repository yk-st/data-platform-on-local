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

# Apache Kafka

分散メッセージングシステムとして今回はKafkaを利用します。

Apache Kafkaは、分散型のストリーミングプラットフォームであり、リアルタイムのデータストリームを処理するための強力なツールです。Kafkaは、高い耐障害性とスケーラビリティを持ち、大量のデータを迅速に処理することができます。

schema registry
https://hub.docker.com/r/confluentinc/cp-schema-registry/tags

https://kafka.apache.org/


# 接続情報

## ワーキングコンテナからのアクセス

| 項目               | アクセス                                                                | ユーザー | パスワード |
|--------------------|-----------------------------------------------------------------------|----------|------------|
| KAFKA SSL　ENDPOINT| CLIENT_SASL_SSL://kafka1.local.data.platform:9092,kafka2.local.data.platform:9092     | admin もしくは　kafka      | admin もしくはpassword          |
| KAFKA WITHOUT SSL　ENDPOINT| CLIENT_SASL_PLAINTEXT://kafka1.local.data.platform:9093,kafka2.local.data.platform:9093     | admin もしくは　kafka      | admin もしくはpassword          |
| KAFKA REST API| http://rest-proxy:8082/v3/     | admin     | admin         |
| KAFKA SCHEMA REGISTRY| http://schema-registry.local.data.platform:8081     | admin     | admin         |

## 認可

kafkaユーザーに対して、orders_topicへのRead権限を付与しており、他のトピックへのアクセスや書き込みは禁止されています。

## SSL
CLIENT_SASL_SSL://kafka1.local.data.platform:9092,kafka2.local.data.platform:9092のエンドポイントに対して
自己証明書によるSSLを設定済みです。
証明書の失効運用(CRL/OSCP)は実施していません。

# 初期設定

initilizer.shにて以下の初期設定を行なっています。

1. orders_topic、orders_topic_avro、orders_topic_aclの3つのトピックを作成
2. kafkaユーザーに対して、orders_topicへのRead権限を付与
3. スキーマエボリューション用のavroファイルの登録

# 設定周りの解説

トピックなど基本用語は既刊を参照してください。

## セキュリティ設定

Kafkaにはいくつかのセキュリティ設定の組み合わせがあります。
SASLという認証の方式と通信の方式(SSLかPLAINTEXT)の組み合わせです。

```
SASL
　　メカニズム　　パスワード(KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN)
　　メカニズム　　SCRAM(KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-256)
　　メカニズム　　OAUTHBEARER(KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: OAUTHBEARER)
```

例えば、KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAINを設定した場合の、SASL_SSLはSASLのパスワードかスクラムで通信はSSLということを指します。
Scrum/BEARRE/AWSであればIAMであればクラスターを再起動せずともユーザーの追加を行うことができる。

## アンサンブル構成
Zookeeperはローカル確認用に1台となっており。アンサンブル構成は取っていません。


REST_URL="http://rest-proxy:8082"
CLUSTER_ID=$(curl -fs "$REST_URL/v3/clusters" | jq -r '.data[0].cluster_id')


curl -s -u admin:admin \
  "$REST_URL/v3/clusters/$CLUSTER_ID/acls?resource_type=TOPIC&resource_name=orders_topic&pattern_type=LITERAL&principal=User:kafka" \
| jq -r '.data[].operation' | sort -u


```

pyspark@work:~$ curl -s -u admin:admin   "$REST_URL/v3/clusters/$CLUSTER_ID/acls?resource_type=TOPIC&resource_name=orders_topic&pattern_type=LITERAL&principal=User:kafka" | jq .
{
  "kind": "KafkaAclList",
  "metadata": {
    "self": "http://rest-proxy:8082/v3/clusters/X883q3E5QyKCOrnDK6mGVw/acls?resource_type=TOPIC&resource_name=orders_topic&pattern_type=LITERAL&principal=User%3Akafka&host=&operation=ANY&permission=ANY",
    "next": null
  },
  "data": [
    {
      "kind": "KafkaAcl",
      "metadata": {
        "self": "http://rest-proxy:8082/v3/clusters/X883q3E5QyKCOrnDK6mGVw/acls?resource_type=TOPIC&resource_name=orders_topic&pattern_type=LITERAL&principal=User%3Akafka&host=*&operation=READ&permission=ALLOW"
      },
      "cluster_id": "X883q3E5QyKCOrnDK6mGVw",
      "resource_type": "TOPIC",
      "resource_name": "orders_topic",
      "pattern_type": "LITERAL",
      "principal": "User:kafka",
      "host": "*",
      "operation": "READ",
      "permission": "ALLOW"
    },
    {
      "kind": "KafkaAcl",
      "metadata": {
        "self": "http://rest-proxy:8082/v3/clusters/X883q3E5QyKCOrnDK6mGVw/acls?resource_type=TOPIC&resource_name=orders_topic&pattern_type=LITERAL&principal=User%3Akafka&host=*&operation=WRITE&permission=ALLOW"
      },
      "cluster_id": "X883q3E5QyKCOrnDK6mGVw",
      "resource_type": "TOPIC",
      "resource_name": "orders_topic",
      "pattern_type": "LITERAL",
      "principal": "User:kafka",
      "host": "*",
      "operation": "WRITE",
      "permission": "ALLOW"
    },
    {
      "kind": "KafkaAcl",
      "metadata": {
        "self": "http://rest-proxy:8082/v3/clusters/X883q3E5QyKCOrnDK6mGVw/acls?resource_type=TOPIC&resource_name=orders_topic&pattern_type=LITERAL&principal=User%3Akafka&host=*&operation=DESCRIBE&permission=ALLOW"
      },
      "cluster_id": "X883q3E5QyKCOrnDK6mGVw",
      "resource_type": "TOPIC",
      "resource_name": "orders_topic",
      "pattern_type": "LITERAL",
      "principal": "User:kafka",
      "host": "*",
      "operation": "DESCRIBE",
      "permission": "ALLOW"
    }
  ]
}

```


#　運用

echo "123456" > kafka_keystore_creds
echo "123456" > kafka_truststore_creds
echo "123456" > kafka_sslkey_creds