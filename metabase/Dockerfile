# Dockerfile

FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && apt-get install -y curl && apt-get install -y jq

# 作業ディレクトリの設定
WORKDIR /metabase

# MetabaseのJARファイルをダウンロード
RUN curl -o metabase.jar -L https://downloads.metabase.com/v0.50.19/metabase.jar
COPY ./conf/starburst-5.0.0.metabase-driver.jar /metabase/plugins/starburst-5.0.0.metabase-driver.jar
#ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.2.12/duckdb.metabase-driver.jar  /metabase/plugins/duckdb.metabase-driver.jar

# 環境変数でLDAP設定を行う
ENV MB_LDAP_HOST=ldap.local.data.platform
ENV MB_LDAP_PORT=389
ENV MB_LDAP_SECURITY=NONE
ENV MB_LDAP_BIND_DN="cn=admin,dc=local,dc=data,dc=platform"
ENV MB_LDAP_PASSWORD=admin
ENV MB_LDAP_USER_BASE="ou=people,dc=local,dc=data,dc=platform"
ENV MB_LDAP_USER_FILTER="(uid={login})"
ENV MB_LDAP_FIRST_NAME_ATTRIBUTE="givenName"
ENV MB_LDAP_LAST_NAME_ATTRIBUTE="sn"
ENV MB_LDAP_EMAIL_ATTRIBUTE="mail"
ENV MB_LDAP_ENABLED=true
ENV MB_SITE_LOCALE=jp

COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# コンテナ起動時にMetabaseを実行
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]