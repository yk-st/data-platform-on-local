# KeyCloak

Keycloakは、認証と認可を統合的に管理するためのオープンソースのIAM基盤です。
OpenID Connect、OAuth 2.0、SAMLなどに対応しており、ログイン認証、トークン発行、シングルサインオン、ユーザー連携をまとめて扱うことができます。

アプリケーションはKeycloakと連携することで、個別に認証機能を実装しなくても、アクセストークンの発行やユーザー認証を標準的な仕組みで利用できます。
外部のユーザー管理基盤と連携して、認証情報を集約する用途でもよく利用されます。

本リポジトリでは、OpenLDAPと連携した認証基盤としてKeycloakを利用しています。
ローカル環境でも、LDAPユーザーの同期やトークン発行の流れを確認できるようにし、APIアクセス時の認証基盤として利用します。


# 接続情報

## relm
openldap

## ホストからのアクセス
| 項目                   | アクセス              | ユーザー | パスワード |
|-------------------------------|-----------------------|---------|-----------|
| KeyCloak Web UI | http://localhost:28080/       | admin                 | admin   | admin     |

## ワーキングコンテナからのアクセス

プログラム的に利用するエンドポイント

| 項目               | アクセス                                                                |
|--------------------|-----------------------------------------------------------------------|
| KeyCloak Certs     | http://keycloak:8080/realms/openldap/protocol/openid-connect/certs   |
| KeyCloak Token     | http://keycloak:8080/realms/openldap/protocol/openid-connect/token  |

# 初期設定
openldapレルムの作成と、LDAPユーザーの同期および天気APIへのアクセストークン発行のための設定を行なっています。
設定は同フォルダのopenldap-relm.jsonに保存済み。

# 制約など
解説や準備の都合上データソース側と共通で利用します。

# オペレーション
設定ファイルのバックアップ

## relmのエクスポート
docker exec -it keycloak /opt/keycloak/bin/kc.sh export --dir /tmp --users realm_file --realm openldap
docker cp keycloak:/tmp/openldap-realm.json openldap-realm.json

## インポート
default_realmにopenldap-realm.jsonを配置しコンテナを再起動する