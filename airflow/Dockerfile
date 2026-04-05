# 必要なライブラリをマルチステージビルドでインストール
FROM yukisaito/spark:3.5.5 AS base_image

# PYTHON_VERSION=3.12.9
FROM apache/airflow:3.0.3-python3.12 

USER root

RUN apt-get update && apt-get install -y \
    git \
    procps \
    openjdk-17-jdk \
    zip

USER airflow

RUN pip install --upgrade pip

# 必要なライブラリをインストール
COPY requirements.txt .
RUN pip install -r requirements.txt

# # coreをインストール
# RUN git clone --branch v1.8.9 --depth 1 https://github.com/dbt-labs/dbt-core.git && cd dbt-core && python -m pip install -r requirements.txt
# # Adapterをインストール
# RUN git clone --branch v1.8.0 --depth 1 https://github.com/dbt-labs/dbt-spark && cd dbt-spark && python -m pip install -r requirements.txt

# yarn-site.xmlの場所を読み込ませる必要がある
# RUN mkdir /opt/airflow/conf
# ENV YARN_CONF_DIR=/opt/airflow/conf

USER root

# yarn.jarsにしたいけどstand alone モードもあり得るので一応記載しておく
# ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/
# ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/
# ADD https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/

# RUN chmod 655 /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.262.jar 
# RUN chmod 655 /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar
# RUN chmod 655 /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/wildfly-openssl-1.0.7.Final.jar 

# ARG COPY_TO=/home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/

# COPY --from=base_image /root/depend_jars/*.jar ${COPY_TO}
#RUN chmod 655 -R /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/

# 実行バイナリをコピー
COPY --from=base_image /home/pyspark/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=/opt/spark/utils:$PYTHONPATH

USER airflow
