FROM python:3.12.11-bookworm

RUN apt-get update 

RUN apt-get install -y sudo

# user pysparkを作りsudoを可能にする
RUN echo pyspark ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/pyspark\
    && chmod 0440 /etc/sudoers.d/pyspark

# ディレクトリの作成
RUN sudo mkdir -p /home/pyspark
RUN sudo chmod 777 -R /home/pyspark

# pyspark グループの作成
RUN groupadd pyspark

# pyspark ユーザを pysparkグループへ追加
RUN useradd -s /bin/bash pyspark -g pyspark

# 便利なパッケージをいくつか追加
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    bash \
    jq \
    wget \
    git \
    openjdk-17-jdk \
    maven

# Pythonで利用するパッケージをインストール
RUN sudo pip install findspark
RUN sudo pip install pandas==2.2.3
RUN sudo pip install pyarrow==18.0.0
RUN sudo pip install boto3==1.38.19
RUN sudo pip install botocore==1.38.19
RUN sudo pip install requests
RUN sudo pip install cuallee==0.15.2
RUN sudo pip install setuptools==80.9.0
# RUN sudo pip install mlflow==3.1.1
# RUN sudo pip install spark-nlp==6.1.0

# pysparkユーザのディレクトリへ移動
WORKDIR /home/pyspark 

# バージョンを環境変数として定義
ARG SPARK_VERSION=3.5.5
ARG SPARK_MAJOR_VERSION=3.5
ARG HADOOP_VERSION=3
ARG SCALA_VERSION=2.13
ARG ICEBERG_VERSION=1.9.2
ARG AWS_VERSION=3.3.4
ARG CP_KAFKA_VERSION=7.9.0
ARG MONGO_VERSION=10.5.0
ARG OPENLINEAGE_VERSION=1.33.0
# ARG ABASA_VERSION=6.4.1
# ARG AVRO_VERSION=1.12.0

# Spark Cliのインストール
# ダウンロードとインストール

# Scala2.13の場合
#RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz
# 開発用(何度もとると遅いので)
COPY src/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz /home/pyspark/
RUN tar -xzvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz
# シンボリックリンクの作成pyspark
RUN ln -s spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION} spark
RUN rm -rf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz

# Scala2.12の場合
# RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
# RUN tar -xzvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
# RUN ln -s spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark
# RUN rm -rf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz


RUN sudo mkdir -p /tmp/spark-events 
RUN sudo chmod 777 -R /tmp/spark-events
RUN sudo mkdir /var/log/spark/
RUN sudo chmod 777 -R /var/log/spark

# 環境変数の設定
RUN sudo echo 'export SPARK_HOME=/home/pyspark/spark' >> ~/.bashrc
RUN sudo echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc

# mavenで必要なjarをインストール
# 3.5系に依存したjarをインストール
# awsはpomよりバージョン判断(https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.13/3.5.5/spark-core_2.13-3.5.5.pom)
# Mongo Spark ConnectorはSpark3.5系をサポートしているものを利用します。https://www.mongodb.com/ja-jp/docs/spark-connector/current/
RUN mvn dependency:get -Dartifact=org.apache.hadoop:hadoop-aws:${AWS_VERSION}
RUN mvn dependency:get -Dartifact=org.apache.iceberg:iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}:${ICEBERG_VERSION}
# RUN mvn dependency:get -Dartifact=org.apache.iceberg:iceberg-core:${ICEBERG_VERSION}
# RUN mvn dependency:get -Dartifact=org.apache.iceberg:iceberg-aws:${ICEBERG_VERSION}
RUN mvn dependency:get -Dartifact=org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}
RUN mvn dependency:get -Dartifact=org.postgresql:postgresql:42.7.3
    #mvn dependency:get -Dartifact=com.datastax.spark:spark-cassandra-connector_${SCALA_VERSION}:3.5.1
# RUN mvn dependency:get -Dartifact=org.apache.spark:spark-avro_${SCALA_VERSION}:${SPARK_VERSION}
RUN mvn dependency:get -Dartifact=org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION}
RUN mvn dependency:get -Dartifact=org.mongodb.spark:mongo-spark-connector_${SCALA_VERSION}:${MONGO_VERSION}
RUN mvn dependency:get -Dartifact=io.openlineage:openlineage-spark_${SCALA_VERSION}:${OPENLINEAGE_VERSION}
RUN mvn dependency:get -Dartifact=org.apache.spark:spark-avro_${SCALA_VERSION}:${SPARK_VERSION}
# RUN mvn dependency:get -Dartifact=org.apache.avro:avro:${AVRO_VERSION}

# jarの依存をシンプルにするため以下いくつかコメントアウト
# RUN mvn dependency:get -Dartifact=za.co.absa:abris_${SCALA_VERSION}:${ABASA_VERSION}

# Centralの外
# RUN mvn dependency:get -Dartifact=io.confluent:kafka-avro-serializer:${CP_KAFKA_VERSION} -DrepoUrl=https://packages.confluent.io/maven/
# RUN mvn dependency:get -Dartifact=io.confluent:kafka-schema-registry-client:${CP_KAFKA_VERSION} -DrepoUrl=https://packages.confluent.io/maven/

# マルチステージビルド用に一旦jarを退避しておく
RUN mkdir -p /root/depend_jars/
RUN find /root/.m2/ -name \*.jar -exec mv {} /root/depend_jars/ \; && rm -rf /root/.m2/
# Sparkのjarディレクトリへ
# RUN cp /home/pyspark/depend_jars/*.jar /home/pyspark/spark/jars/

# 余計なデータを削除
RUN find /root/depend_jars/ -type f ! \( \
    -name "aws-java-sdk-bundle-1.12.262.jar" -o \
    -name "hadoop-aws-${AWS_VERSION}.jar" -o \
    -name "wildfly-openssl-1.0.7.Final.jar" -o \
    #-name "spark-cassandra-connector*.jar" -o \
    # -name "*kryo*" -o \
    -name "iceberg*.jar" -o \
    -name "avro*.jar" -o \
    #   -name "*kafka-avro-serializer*${CP_KAFKA_VERSION}*.jar" -o \
    #   -name "*kafka-schema-registry-client*${CP_KAFKA_VERSION}*.jar" -o \
    #   -name "*kafka-serde-common-${CP_KAFKA_VERSION}*.jar" -o \
    #   -name "*abris*.jar" -o \
    #   -name "commons_2.13-1.0.0.jar" -o \
    -name "*spark-avro*${SCALA_VERSION}-${SPARK_VERSION}.jar" -o \
    -name "commons-pool2-2.11.1.jar" -o \
    -name "kafka-clients-3.4.1.jar" -o \
    -name "*spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" -o \
    -name "*spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" -o \
    -name "*mongo*.jar" -o \
    -name "*bson*.jar" -o \
    -name "*openlineage*.jar" -o \
    -name "postgresql-42.7.3.jar" \
\) -exec rm -f {} +

ARG COPY_TO=/home/pyspark/spark/jars/

RUN cp /root/depend_jars/aws-java-sdk-bundle-1.12.262.jar ${COPY_TO}
RUN cp /root/depend_jars/hadoop-aws-${AWS_VERSION}.jar ${COPY_TO}
RUN cp /root/depend_jars/wildfly-openssl-1.0.7.Final.jar ${COPY_TO}
#RUN cp /root/depend_jars/spark-cassandra-connector*.jar  ${COPY_TO}
# RUN cp /root/depend_jars/kryo-shaded-4.0.2.jar  ${COPY_TO}
RUN cp /root/depend_jars/iceberg*.jar  ${COPY_TO}
RUN cp /root/depend_jars/avro*.jar  ${COPY_TO}
RUN cp /root/depend_jars/postgresql-42.7.3.jar  ${COPY_TO}
# RUN cp /root/depend_jars/*kafka-avro-serializer*${CP_KAFKA_VERSION}*.jar  ${COPY_TO}
# RUN cp /root/depend_jars/*kafka-schema-registry-client*${CP_KAFKA_VERSION}*.jar  ${COPY_TO}
# RUN cp /root/depend_jars/*kafka-serde-common-${CP_KAFKA_VERSION}*.jar  ${COPY_TO}
# # RUN cp /root/depend_jars/*abris*.jar  ${COPY_TO}
# RUN cp /root/depend_jars/commons_2.13-1.0.0.jar  ${COPY_TO}
RUN cp /root/depend_jars/*spark-avro*${SCALA_VERSION}-${SPARK_VERSION}.jar  ${COPY_TO}

# ── 4 本の JAR を COPY_TO へコピー ─────────────────────────
RUN cp /root/depend_jars/commons-pool2-2.11.1.jar  ${COPY_TO}
RUN cp /root/depend_jars/kafka-clients-3.4.1.jar  ${COPY_TO}
RUN cp /root/depend_jars/*spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar  ${COPY_TO}
RUN cp /root/depend_jars/*spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar  ${COPY_TO}


RUN cp /root/depend_jars/*mongo*.jar  ${COPY_TO}
RUN cp /root/depend_jars/*bson*.jar  ${COPY_TO}
RUN cp /root/depend_jars/*openlineage*.jar  ${COPY_TO}

# avroのバージョンを1.12.0に合わせる
# RUN rm -f /home/pyspark/spark/jars/avro*1.11.4*.jar
# RUN rm -f /home/pyspark/spark/jars/avro*-1.7.7*.jar

# Utilの設定
COPY src/ /home/pyspark/spark/utils/
ENV PYTHONPATH=/home/pyspark/spark/utils:$PYTHONPATH

USER pyspark
# 環境変数の設定
RUN echo "export SPARK_HOME=/home/pyspark/spark/" >> ~/.bashrc
RUN echo "export PATH=${PATH}:/home/pyspark/spark/bin" >> ~/.bashrc

ENV SPARK_HOME=/home/pyspark/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV SPARK_VERSION=${SPARK_VERSION}

# TODO nativeライブラリの設定
# export HADOOP_HOME=/home/pyspark/spark/hadoop-3.3.6
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
