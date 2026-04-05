# 必要なライブラリをマルチステージビルドでインストール
FROM yukisaito/spark:3.5.5 AS base_image

FROM apache/hive:4.0.0

USER root

ARG COPY_TO=/opt/hive/lib/

COPY --from=base_image /root/depend_jars/*.jar ${COPY_TO}

USER hive