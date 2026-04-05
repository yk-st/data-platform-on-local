# OpenLDAP

OpenLDAPは、LDAPプロトコルに対応したディレクトリサービスのオープンソース実装です。
ユーザー、グループ、組織情報などを一元的に管理でき、認証や認可の基盤として幅広く利用されています。

アプリケーション側はLDAPサーバーに対してユーザー情報を問い合わせることで、ログイン認証やグループベースのアクセス制御を行えます。
そのため、複数のシステムでユーザー管理を共通化したい場合に有効です。

本リポジトリでは、ローカル分析基盤における共通のユーザー管理基盤としてOpenLDAPを利用しています。
Trinoなどのコンポーネントと連携し、ローカル環境でも認証やロールベースのアクセス制御を確認できるようにしています。


# 接続情報
今回はユーザの管理にldapを利用します。
コンテナの数を減らすためにデータソースとデータ分析基盤側で共通のユーザ管理システムを利用します。
本来は、データソース側は別管理のシステムのためそれぞれのLDapなどのユーザ管理システムがあることにご注意ください。
ローカルでは主にRBACの確認用に幾つかのユーザーが事前に登録されています(ABACはクラウドで確認)。


## ホストからのアクセス

| 項目               | アクセス                                                                | ユーザー | パスワード|
|--------------------|-----------------------------------------------------------------------|----------|------------|
| LDAP UI| http://localhost/lam/templates/lists/list.php?type=user    | admin     | admin          |


## ワーキングコンテナからの接続情報

プログラム的に利用するエンドポイント

| 項目               | アクセス                                                                | ユーザー(BINDDN) | パスワード(BINDDN_PASS) |
|--------------------|-----------------------------------------------------------------------|----------|------------|
| Trino Cluster OverView| ldap://ldap.local.data.platform:386(ldaps://ldap.local.data.platform:636)     | cn=admin,dc=local,dc=data,dc=platform      | admin          |


Tips: SSSDは使っていません。
使う要素は絞ろうということで、SSSDは使用しないことにしました。
実際のオンプレ構築ではSSSDを利用してユーザデータをキャッシュすることでADやLDAPへのアクセスを軽減します。

# 初期設定

## 作成済みユーザとロール
ユーザーの作成を行なっています。

- pyspark@local.data.platform(admin権限)
- user1@local.data.platform(analyst権限)

## 作成済みユーザーの認証情報

@前の名称と同じパスワードになっています。

- pyspark@local.data.platformはpyspark
- user1@local.data.platformはuser1

# オペレーション

## 認証確認

```
ldapwhoami -x -D "cn=pyspark,ou=people,dc=local,dc=data,dc=platform" -w pyspark -H ldap://local.data.platform:389
ldapwhoami -x -D "uid=pyspark@local.data.platform,ou=people,dc=local,dc=data,dc=platform"  -w pyspark -H ldap://local.data.platform:389
```

## ユーザパスワード作成
Ldapユーザーのパスワードを作成するとき

```
docker exec ldap.local.data.platform slappasswd -s user1
```

