FROM yukisaito/spark:3.5.5

USER root

COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

USER pyspark

# COPY ./conf/spark/spark-defaults.conf /home/pyspark/spark/conf/spark-defaults.conf
# COPY ./conf/spark/log4j.properties /home/pyspark/spark/conf/log4j.properties
# COPY ./conf/hive/hive-site.xml /home/pyspark/spark/conf/hive-site.xml

#ENV YARN_CONF_DIR=/home/pyspark/spark/conf/

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]