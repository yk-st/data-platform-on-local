# ベースイメージを指定
FROM yukisaito/spark:3.5.5

USER root

# RUN (type -p wget >/dev/null || ( apt update &&  apt-get install wget -y)) \
# && mkdir -p -m 755 /etc/apt/keyrings \
# && wget -qO- https://cli.github.com/packages/githubcli-archive-keyring.gpg |  tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
# &&  chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
# && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" |  tee /etc/apt/sources.list.d/github-cli.list > /dev/null

# 必要なパッケージをインストール
RUN apt-get update && apt-get install -y \
    vim \
    postgresql-client \
    netcat-traditional \
    iputils-ping \
    net-tools \
    less \
    jq \
    locales \
    # gh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

#### AWS CLI V2###

# ------------ 任意でバージョン固定 -------------
ARG AWSCLI_VERSION=2.19.1        # 空で最新、数字で固定
# ---------------------------------------------

RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends \
        curl unzip groff less ca-certificates; \
    rm -rf /var/lib/apt/lists/*

# ---- アーキを検出して対応する ZIP を取得 ----
RUN ARCH="$(dpkg --print-architecture)"; \
    case "${ARCH}" in \
      amd64)  AWS_ARCH="x86_64"  ;; \
      arm64)  AWS_ARCH="aarch64" ;; \
      *) echo "Unsupported arch: ${ARCH}" && exit 1 ;; \
    esac; \
    if [ -z "${AWSCLI_VERSION}" ]; then \
        URL="https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}.zip"; \
    else \
        URL="https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}-${AWSCLI_VERSION}.zip"; \
    fi; \
    curl -fsSL "$URL" -o /tmp/awscliv2.zip; \
    unzip -q /tmp/awscliv2.zip -d /tmp; \
    /tmp/aws/install --install-dir /usr/local/aws-cli --bin-dir /usr/local/bin; \
    rm -rf /tmp/aws /tmp/awscliv2.zip

# Kafka Cli
ENV KAFKA_VERSION=3.9.1
ENV SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}

RUN curl -fSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
      -o /tmp/kafka.tgz \
 && tar xzf /tmp/kafka.tgz -C /opt \
 && rm /tmp/kafka.tgz

RUN ln -s "${KAFKA_HOME}" /home/pyspark/kafka_cli

# スクリプトをコンテナにコピー
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 日本語パッケージ
RUN sed -i 's/# ja_JP.UTF-8 UTF-8/ja_JP.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG ja_JP.UTF-8

USER pyspark

RUN pip install attrs==25.3.0
RUN pip install cachetools==6.1.0
RUN pip install httpx==0.28.1
RUN pip install Authlib==1.6.0
RUN pip install confluent_kafka==2.10.1
RUN pip install fastavro==1.11.1
RUN pip install requests
#RUN pip install awscli==1.36.8
RUN pip install psycopg2==2.9.10
RUN pip install pdfminer.six==20250506
RUN pip install s3fs==2025.5.1
RUN pip install setuptools==80.9.0
RUN pip install jump-consistent-hash==3.5.1
RUN pip install mmh3==5.2.0


# MINIOの操作で利用するアクセスキーとシークレット
# 利便性のため設定しています。本当はENVに入れるのはダメです。secretで設定しましょう。
ENV AWS_ACCESS_KEY_ID=5nCJP6jHFJd7PDsLlT3a
ENV AWS_SECRET_ACCESS_KEY=FXn6MFKDbNamyMzxiMBGIpgTDFu2r1IfymESfRJd
ENV S3_ENDPOINT_URL=http://minio.local.data.platform:9000

# yarn-site.xmlの場所を読み込ませる必要がある
# ENV YARN_CONF_DIR=/home/pyspark/spark/conf/

# Trino Cli
RUN wget https://repo1.maven.org/maven2/io/trino/trino-cli/476/trino-cli-476-executable.jar -O trino
RUN chmod +x trino

# 起動時にスクリプトを実行
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]