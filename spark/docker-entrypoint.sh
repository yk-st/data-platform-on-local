#!/bin/bash

# 環境変数によってモードを変更
if [ "$SPARK_MODE" == "master" ]; then
  exec /home/pyspark/spark/bin/spark-class org.apache.spark.deploy.master.Master --host $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT
elif [ "$SPARK_MODE" == "worker" ]; then
  exec /home/pyspark/spark/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL
elif [ "$SPARK_MODE" == "history" ]; then
  exec /home/pyspark/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer
elif [ "$SPARK_MODE" == "thrift" ]; then
  exec /home/pyspark/spark/sbin/start-thriftserver.sh \
    --master $SPARK_MASTER_URL \
    --deploy-mode client \
    --name "Application Through Spark Thrift Server" \
    --hiveconf hive.server2.thrift.port=10000 \
    --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
    --executor-memory 2G \
    --total-executor-cores 2 \
    --conf spark.sql.hive.thriftServer.singleSession=false \
    --conf spark.submit.deployMode=client \
    --conf spark.hadoop.hive.metastore.uris=thrift://metastore-thrift.local.data.platform:9083 \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio.local.data.platform:9000 \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.access.key=5nCJP6jHFJd7PDsLlT3a \
    --conf spark.hadoop.fs.s3a.secret.key=FXn6MFKDbNamyMzxiMBGIpgTDFu2r1IfymESfRJd \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --hiveconf hive.server2.authentication=LDAP \
    --hiveconf hive.server2.authentication.ldap.url=ldap://ldap.local.data.platform:389 \
    --hiveconf hive.server2.authentication.ldap.baseDN=ou=people,dc=local,dc=data,dc=platform \
    --hiveconf hive.server2.authentication.ldap.Domain=local.data.platform \
    --hiveconf hive.server2.authentication.ldap.binddn=cn=admin,dc=local,dc=data,dc=platform \
    --hiveconf hive.server2.authentication.ldap.bindpw=admin \
    --hiveconf hive.server2.authentication.ldap.groupFilter=cn=admin,ou=groups,dc=local,dc=data,dc=platform & \
    tail -f /dev/null
    # 本来はSSSD経由の方が好ましいが、今回は本質的ではないので直接LDAPに繋ぐものとする
    #--conf spark.sql.thriftServer.incrementalCollect=falseの利用は要検討
else
  echo "Invalid SPARK_MODE specified: $SPARK_MODE"
  exit 1
fi

# HADOOP_VERSION=3.3.6
# curl -O https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
# tar -xzf hadoop-${HADOOP_VERSION}.tar.gz
