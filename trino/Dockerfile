# 必要なライブラリをマルチステージビルドでインストール
FROM yukisaito/spark:3.5.5 AS base_image

FROM trinodb/trino:476

# /lib/trino/plugin
# sh-5.1$ ls 
# bigquery    druid                exchange-hdfs  http-event-listener         jmx                   mariadb  mysql-event-listener  password-authenticators  redis                      snowflake            tpcds
# blackhole   elasticsearch        faker          http-server-event-listener  kafka                 memory   opa                   phoenix5                 redshift                   spooling-filesystem  tpch
# cassandra   example-http         geospatial     hudi                        kafka-event-listener  ml       openlineage           pinot                    resource-group-managers    sqlserver            vertica
# clickhouse  exasol               google-sheets  iceberg                     kinesis               mongodb  opensearch            postgresql               session-property-managers  teradata-functions
# delta-lake  exchange-filesystem  hive           ignite                      kudu                  mysql    oracle                prometheus               singlestore                thrift

# 各プラグインは各コネクターに

USER root

#RUN apt install gzip -y

# s3(Minio)アクセス用のライブラリ群の配置
# ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /lib/trino/plugin/hive
# ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /lib/trino/plugin/hive
# ADD https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar /lib/trino/plugin/hive

# RUN chmod 655 /lib/trino/plugin/hive/aws-java-sdk-bundle-1.12.262.jar 
# RUN chmod 655 /lib/trino/plugin/hive/hadoop-aws-3.3.4.jar
# RUN chmod 655 /lib/trino/plugin/hive/wildfly-openssl-1.0.7.Final.jar 

# Icebergは同梱されているので不要
# # 依存のJarを設定
# ARG COPY_TO=/lib/trino/plugin/hive/
# COPY --from=base_image /root/depend_jars/*.jar ${COPY_TO}

# 古いAWS SDKを削除(Sparkのバージョンが古いのでTrino用に入れ替え)
# RUN rm -f /lib/trino/plugin/hive/aws-java-sdk-bundle-*.jar
# RUN rm -f /lib/trino/plugin/hive/hadoop-aws-*.jar

# # 新しいAWS SDK v2 または最新のv1を使用
# ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.770/aws-java-sdk-bundle-1.12.788.jar /lib/trino/plugin/hive/
# ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar /lib/trino/plugin/hive/

# RUN chmod 655 /lib/trino/plugin/hive/aws-java-sdk-bundle-1.12.788.jar
# RUN chmod 655 /lib/trino/plugin/hive/hadoop-aws-3.3.6.jar

# LDAP Group Provider を password-authenticators プラグインに追加
ADD https://repo1.maven.org/maven2/io/trino/trino-ldap-group-provider/476/trino-ldap-group-provider-476.jar /usr/lib/trino/plugin/ldap-group-provider/
# 権限設定
RUN chmod 655 /usr/lib/trino/plugin/ldap-group-provider/trino-ldap-group-provider-476.jar


USER trino
