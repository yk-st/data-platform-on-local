
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

# ワーキングコンテナ

# 接続情報

本書で紹介しているすべてのコマンド操作は断りのない限りこのワーキングコンテナから実施します。
各プロダクトへの接続は各README.mdを参照してください。

# orders_simulator.pyによるデータ生成

ordersテーブル相当のデータを生成するPythonスクリプトです。

## Kafkaへ指定件数だけテスト送信

```
python /home/pyspark/generator/orders_simulator.py \
  --n 1 \
  --output kafka \
  --kafka_bootstrap "kafka1.local.data.platform:9093" \
  --kafka_topic "orders_topic" \
  --kafka_user "admin" \
  --kafka_password "admin"
```

## Kafkaへ継続送信モード（5秒間隔で1件ずつ送信）

```
python /home/pyspark/generator/orders_simulator.py \
  --output kafka \
  --kafka_bootstrap "kafka1.local.data.platform:9093" \
  --kafka_topic "orders_topic" \
  --kafka_user "admin" \
  --kafka_password "admin" \
  --bad_data_rate 0.2 \
  --continuous \
  --n 1 \
  --interval 5
```

## PostgreSQL継続挿入

```
python /home/pyspark/generator/orders_simulator.py --output postgres --n 2 --bad_data_rate 0.0 --continuous --interval 5.0
```

## Postgresへ指定件数だけテスト送信

```
python /home/pyspark/generator/orders_simulator.py --output postgres --n 2 --bad_data_rate 0.0
```

### 不正データの例
小計が抜けてしまっている。

```

                  ID                  | ユーザーID | 製品ID | 小計-金額 | 税金額 | 合計ー円 | 数量（個） | フラグ |      作成日時       | 親ID 
--------------------------------------+------------+--------+-----------+--------+----------+------------+--------+---------------------+------
 19009557-defa-4276-8baa-71b6a21f409a |         82 |     17 |           |  100.0 |   1100.0 |          1 |      2 | 2025-07-09 06:36:22 |   

```

# コマンドスニペット

## データソースDBへの接続

```
psql -h host.docker.internal -p 5435 -U domain -d domain_database

```

## Trinoへの接続

```
./trino --server https://reverse-proxy.local.data.platform --debug --user pyspark@local.data.platform --password
```

パスワードは@の前と同一

## Sparkクラスタへの接続(REPL)

pyspark

### Sparkプログラムの実行

単純なストリーミング処理

```
spark-submit /home/pyspark/programs/spark/streaming/spark_streaming.py
```

Window関数を利用したストリーミング処理

```
spark-submit /home/pyspark/programs/spark/streaming/spark_s_window.py
```

#### ストリーミングの処理を初期化したくなったら
minioに配置されているcheckpointディレクトリを削除してください。
kafkaのコンテナをdocker compose downしてから再度起動してください。


## Avro Producer/Consumerの利用
以下を参照してください
[Avro](programs/avro/README.md)

## Kafkaの動作確認

### SASL_PLAINTEXT設定のあるエンドポイント

トピックの確認

```

./kafka_cli/bin/kafka-topics.sh \
  --bootstrap-server kafka1.local.data.platform:9093 \
  --command-config ./security/client-sasl.properties \
  --list

```

### SASL_SSL設定のあるエンドポイント

トピックの確認

```

./kafka_cli/bin/kafka-topics.sh \
  --bootstrap-server kafka1.local.data.platform:9092 \
  --command-config ./security/client-sasl-ssl.properties \
  --list

```


producerの起動

```

./kafka_cli/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1.local.data.platform:9092 \
  --producer.config ./security/client-sasl-ssl.properties \
  --topic orders_topic

```

consumerの起動

```

./kafka_cli/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1.local.data.platform:9092 \
  --consumer.config ./security/client-sasl-ssl.properties \
  --topic orders_topic \
  --group ssl-check-group \
  --from-beginning

```