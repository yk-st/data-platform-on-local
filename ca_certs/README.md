# data-platform-on-local

以下の書籍に関するリポジトリです。

<div style="text-align: center;">
    <figure style="display: inline-block; margin: 0 20px; text-align: center;">
        <a href="https://www.amazon.co.jp/dp/4297145634/ref=sspa_dk_detail_0?psc=1&pd_rd_i=4297145634&pd_rd_w=BXEhW&content-id=amzn1.sym.f293be60-50b7-49bc-95e8-931faf86ed1e&pf_rd_p=f293be60-50b7-49bc-95e8-931faf86ed1e&pf_rd_r=VZ7P7XN3YX1NAMJAPZEB&pd_rd_wg=CuOVv&pd_rd_r=31953068-34be-40e1-978d-b417f6b20227&s=books&sp_csd=d2lkZ2V0TmFtZT1zcF9kZXRhaWw">
            <img alt="エンジニアのためのデータ分析基盤 基本編" src="../書影.png" width="150px" style="margin-right: 10px;">
        </a>
         <figcaption>基本編</figcaption>
    </figure>
    <figure style="display: inline-block; margin: 0 20px; text-align: center;">
        <a href>
            <img alt="エンジニアのためのデータ分析基盤 実践編" src="../title.jpg" width="150px">
        </a>
         <figcaption>実践編</figcaption>
    </figure>
</div>

# はじめに

本環境では、Trust Storeは共通です。
keystoreがある意味サーバー証明書に当たります。
パスワードはいずれも123456を指定しています。

証明書の生成方法の1~6を実行ください。

## 用語
- **Keystore**：秘密鍵とそれに対応する証明書を保存するファイル。サーバー側が自分自身を証明するために使用する。
- **TrustStore**：信頼できる公開鍵証明書を保存するファイル。相手先（クライアントや他サーバー）の正当性を確認するために使用する。

# 証明書の生成方法

## 1. KeyToolを利用して、TrustStoreとKeystoreを生成する。

以下でTruststoreおよびkeystoreを作る(keystoreはサーバー証明書みたいなもの)

```

作成コマンド（※下部の実行ログ参照）

### イメージのビルド
docker build -f Dockerfile.keytool -t keytool .

### コマンドの実行
docker run -v $(pwd):/work -it --rm --user 1000 keytool bash generate-keystore-ssl.sh 1

※引数はkafka1向けのkeystoreを作る場合は1、kafka2向けのkeystoreを作る場合は2を指定してください。引数に応じてkafka1.keystore.jksもしくはkafka2.keystore.jksが生成されます。

※https://github.com/confluentinc/confluent-platform-security-tools/raw/master/kafka-generate-ssl.sh

```

CNがサーバー名に一致するように作成する。
参考として以下の実行ログを参考にすると良い。

## keytool実行時のログ

What is your first and last name?　がいわゆるCNのことです。
truststoreはすでに存在する場合は、そのパスを指定して共通のCAからkeystoreを作るようにすることで、共通のCAを利用することができます。

以下のログに沿って実行してください。

### kafak1向け実行ログ

PEMフレーズは123456を指定している。
他パスワードも全て123456を指定している。

```
saitouyuuki@yukisaitos-MacBook-Pro secrets % docker run -v $(pwd):/work -it --rm --user 1000 keytool bash generate-keystore-ssl.sh 1

Welcome to the Kafka SSL keystore and truststore generator script.

First, do you need to generate a trust store and associated private key,
or do you already have a trust store file and private key?

Do you need to generate a trust store and associated private key? [yn] y

OK, we'll generate a trust store and associated private key.

First, the private key.

You will be prompted for:
 - A password for the private key. Remember this.
 - Information about you and your company.
 - NOTE that the Common Name (CN) is currently not important.
Generating a RSA private key
.......................................................................................................+++++
..................+++++
unable to write 'random state'
writing new private key to 'truststore/ca-key'
Enter PEM pass phrase:
Verifying - Enter PEM pass phrase:
-----
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) []:JP
State or Province Name (full name) []:TOKYO
Locality Name (eg, city) []:YOKO
Organization Name (eg, company) []:hoeg
Organizational Unit Name (eg, section) []:peke
Common Name (eg, fully qualified host name) []:kafka1.local.data.platform
Email Address []:

Two files were created:
 - truststore/ca-key -- the private key used later to
   sign certificates
 - truststore/ca-cert -- the certificate that will be
   stored in the trust store in a moment and serve as the certificate
   authority (CA). Once this certificate has been stored in the trust
   store, it will be deleted. It can be retrieved from the trust store via:
   $ keytool -keystore <trust-store-file> -export -alias CARoot -rfc

Now the trust store will be generated from the certificate.

You will be prompted for:
 - the trust store's password (labeled 'keystore'). Remember this
 - a confirmation that you want to import the certificate
Enter keystore password:  
Re-enter new password: 
Owner: CN=kafka1.local.data.platform, OU=peke, O=hoeg, L=YOKO, ST=TOKYO, C=JP
Issuer: CN=kafka1.local.data.platform, OU=peke, O=hoeg, L=YOKO, ST=TOKYO, C=JP
Serial number: 94f50398ffd16649
Valid from: Fri Nov 22 02:33:46 GMT 2024 until: Thu Mar 25 02:33:46 GMT 3024
Certificate fingerprints:
         MD5:  23:8C:44:2A:56:0C:46:A9:4F:AC:94:E4:A9:53:06:A5
         SHA1: 19:08:3E:00:99:BB:8A:D7:A2:54:47:42:94:CE:95:09:A9:54:85:9E
         SHA256: 70:FB:E4:CD:A7:44:1A:DA:01:FB:F0:A8:8E:4F:01:2A:73:53:66:FD:89:37:B8:4E:5A:29:89:A8:44:B6:06:3E
Signature algorithm name: SHA256withRSA
Subject Public Key Algorithm: 2048-bit RSA key
Version: 1
Trust this certificate? [no]:  yes
Certificate was added to keystore

truststore/kafka.truststore.jks was created.

Continuing with:
 - trust store file:        truststore/kafka.truststore.jks
 - trust store private key: truststore/ca-key

Now, a keystore will be generated. Each broker and logical client needs its own
keystore. This script will create only one keystore. Run this script multiple
times for multiple keystores.

You will be prompted for the following:
 - A keystore password. Remember it.
 - Personal information, such as your name.
     NOTE: currently in Kafka, the Common Name (CN) does not need to be the FQDN of
           this host. However, at some point, this may change. As such, make the CN
           the FQDN. Some operating systems call the CN prompt 'first / last name'
 - A key password, for the key being generated within the keystore. Remember this.
Enter keystore password:  
Re-enter new password: 
What is your first and last name?
  [Unknown]:  kafka1.local.data.platform
What is the name of your organizational unit?
  [Unknown]:  kafka1.local.data.platform
What is the name of your organization?
  [Unknown]:  kafka1.local.data.platform
What is the name of your City or Locality?
  [Unknown]:  kafka1.local.data.platform
What is the name of your State or Province?
  [Unknown]:  kafka1.local.data.platform
What is the two-letter country code for this unit?
  [Unknown]:  JP                        
Is CN=kafka1.local.data.platform, OU=kafka1.local.data.platform, O=kafka1.local.data.platform, L=kafka1.local.data.platform, ST=kafka1.local.data.platform, C=JP correct?
  [no]:    
What is your first and last name?
  [kafka1.local.data.platform]:  kafka1.local.data.platform
What is the name of your organizational unit?
  [kafka1.local.data.platform]:  YUKI
What is the name of your organization?
  [kafka1.local.data.platform]:  SAITO
What is the name of your City or Locality?
  [kafka1.local.data.platform]:  YOKO
What is the name of your State or Province?
  [kafka1.local.data.platform]:  HOGE
What is the two-letter country code for this unit?
  [JP]:  JP
Is CN=kafka1.local.data.platform, OU=YUKI, O=SAITO, L=YOKO, ST=HOGE, C=JP correct?
  [no]:  yes

Enter key password for <localhost>
        (RETURN if same as keystore password):  
Re-enter new password: 

Warning:
The JKS keystore uses a proprietary format. It is recommended to migrate to PKCS12 which is an industry standard format using "keytool -importkeystore -srckeystore keystore/kafka.keystore.jks -destkeystore keystore/kafka.keystore.jks -deststoretype pkcs12".

'keystore/kafka.keystore.jks' now contains a key pair and a
self-signed certificate. Again, this keystore can only be used for one broker or
one logical client. Other brokers or clients need to generate their own keystores.

Fetching the certificate from the trust store and storing in ca-cert.

You will be prompted for the trust store's password (labeled 'keystore')
Enter keystore password:  
Certificate stored in file <ca-cert>

Now a certificate signing request will be made to the keystore.

You will be prompted for the keystore's password.
Enter keystore password:  

Warning:
The JKS keystore uses a proprietary format. It is recommended to migrate to PKCS12 which is an industry standard format using "keytool -importkeystore -srckeystore keystore/kafka.keystore.jks -destkeystore keystore/kafka.keystore.jks -deststoretype pkcs12".

Now the trust store's private key (CA) will sign the keystore's certificate.

You will be prompted for the trust store's private key password.
Signature ok
subject=/C=JP/ST=HOGE/L=YOKO/O=SAITO/OU=YUKI/CN=kafka1.local.data.platform
Getting CA Private Key
Enter pass phrase for truststore/ca-key:
unable to write 'random state'

Now the CA will be imported into the keystore.

You will be prompted for the keystore's password and a confirmation that you want to
import the certificate.
Enter keystore password:  
Owner: CN=kafka1.local.data.platform, OU=peke, O=hoeg, L=YOKO, ST=TOKYO, C=JP
Issuer: CN=kafka1.local.data.platform, OU=peke, O=hoeg, L=YOKO, ST=TOKYO, C=JP
Serial number: 94f50398ffd16649
Valid from: Fri Nov 22 02:33:46 GMT 2024 until: Thu Mar 25 02:33:46 GMT 3024
Certificate fingerprints:
         MD5:  23:8C:44:2A:56:0C:46:A9:4F:AC:94:E4:A9:53:06:A5
         SHA1: 19:08:3E:00:99:BB:8A:D7:A2:54:47:42:94:CE:95:09:A9:54:85:9E
         SHA256: 70:FB:E4:CD:A7:44:1A:DA:01:FB:F0:A8:8E:4F:01:2A:73:53:66:FD:89:37:B8:4E:5A:29:89:A8:44:B6:06:3E
Signature algorithm name: SHA256withRSA
Subject Public Key Algorithm: 2048-bit RSA key
Version: 1
Trust this certificate? [no]:  yes
Certificate was added to keystore

Warning:
The JKS keystore uses a proprietary format. It is recommended to migrate to PKCS12 which is an industry standard format using "keytool -importkeystore -srckeystore keystore/kafka.keystore.jks -destkeystore keystore/kafka.keystore.jks -deststoretype pkcs12".

Now the keystore's signed certificate will be imported back into the keystore.

You will be prompted for the keystore's password.
Enter keystore password:  
Certificate reply was installed in keystore

Warning:
The JKS keystore uses a proprietary format. It is recommended to migrate to PKCS12 which is an industry standard format using "keytool -importkeystore -srckeystore keystore/kafka.keystore.jks -destkeystore keystore/kafka.keystore.jks -deststoretype pkcs12".

All done!

Delete intermediate files? They are:
 - 'ca-cert.srl': CA serial number
 - 'cert-file': the keystore's certificate signing request
   (that was fulfilled)
 - 'cert-signed': the keystore's certificate, signed by the CA, and stored back
    into the keystore
Delete? [yn] y
```


### kafka2向け

すでにtruststore(CA)があるので、そちらを指定するようにする。
別で作らない。

cert-files,cert-sighed,ca-cert.srlが消えていなかったら削除するようにする。

```
saitouyuuki@yukisaitos-MacBook-Pro secrets % docker run -v $(pwd):/work -it --rm --user 1000 keytool bash generate-keystore-ssl.sh 2

Welcome to the Kafka SSL keystore and truststore generator script.

First, do you need to generate a trust store and associated private key,
or do you already have a trust store file and private key?

Do you need to generate a trust store and associated private key? [yn] n

Enter the path of the trust store file. ./truststore/kafka.truststore.jks
Enter the path of the trust store's private key. ./truststore/ca-key

Continuing with:
 - trust store file:        ./truststore/kafka.truststore.jks
 - trust store private key: ./truststore/ca-key

Now, a keystore will be generated. Each broker and logical client needs its own
keystore. This script will create only one keystore. Run this script multiple
times for multiple keystores.

You will be prompted for the following:
 - A keystore password. Remember it.
 - Personal information, such as your name.
     NOTE: currently in Kafka, the Common Name (CN) does not need to be the FQDN of
           this host. However, at some point, this may change. As such, make the CN
           the FQDN. Some operating systems call the CN prompt 'first / last name'
 - A key password, for the key being generated within the keystore. Remember this.
Enter keystore password:  
Re-enter new password: 
What is your first and last name?
  [Unknown]:  kafka2.local.data.platform
What is the name of your organizational unit?
  [Unknown]:  kafka2.local.data.platform
What is the name of your organization?
  [Unknown]:  kafka2.local.data.platform
What is the name of your City or Locality?
  [Unknown]:  kafka2.local.data.platform
What is the name of your State or Province?
  [Unknown]:  kafka2.local.data.platform
What is the two-letter country code for this unit?
  [Unknown]:  JP
Is CN=kafka2.local.data.platform, OU=kafka2.local.data.platform, O=kafka2.local.data.platform, L=kafka2.local.data.platform, ST=kafka2.local.data.platform, C=JP correct?
  [no]:  yes

Enter key password for <localhost>
        (RETURN if same as keystore password):  
Re-enter new password: 

Warning:
The JKS keystore uses a proprietary format. It is recommended to migrate to PKCS12 which is an industry standard format using "keytool -importkeystore -srckeystore keystore/kafka.keystore.jks -destkeystore keystore/kafka.keystore.jks -deststoretype pkcs12".

'keystore/kafka.keystore.jks' now contains a key pair and a
self-signed certificate. Again, this keystore can only be used for one broker or
one logical client. Other brokers or clients need to generate their own keystores.

Fetching the certificate from the trust store and storing in ca-cert.

You will be prompted for the trust store's password (labeled 'keystore')
Enter keystore password:  
Certificate stored in file <ca-cert>

Now a certificate signing request will be made to the keystore.

You will be prompted for the keystore's password.
Enter keystore password:  

Warning:
The JKS keystore uses a proprietary format. It is recommended to migrate to PKCS12 which is an industry standard format using "keytool -importkeystore -srckeystore keystore/kafka.keystore.jks -destkeystore keystore/kafka.keystore.jks -deststoretype pkcs12".

Now the trust store's private key (CA) will sign the keystore's certificate.

You will be prompted for the trust store's private key password.
Signature ok
subject=/C=JP/ST=kafka2.local.data.platform/L=kafka2.local.data.platform/O=kafka2.local.data.platform/OU=kafka2.local.data.platform/CN=kafka2.local.data.platform
Getting CA Private Key
Enter pass phrase for ./kafka1/ca-key:
unable to write 'random state'

Now the CA will be imported into the keystore.

You will be prompted for the keystore's password and a confirmation that you want to
import the certificate.
Enter keystore password:  
Owner: CN=kafka1.local.data.platform, OU=peke, O=hoeg, L=YOKO, ST=TOKYO, C=JP
Issuer: CN=kafka1.local.data.platform, OU=peke, O=hoeg, L=YOKO, ST=TOKYO, C=JP
Serial number: 94f50398ffd16649
Valid from: Fri Nov 22 02:33:46 GMT 2024 until: Thu Mar 25 02:33:46 GMT 3024
Certificate fingerprints:
         MD5:  23:8C:44:2A:56:0C:46:A9:4F:AC:94:E4:A9:53:06:A5
         SHA1: 19:08:3E:00:99:BB:8A:D7:A2:54:47:42:94:CE:95:09:A9:54:85:9E
         SHA256: 70:FB:E4:CD:A7:44:1A:DA:01:FB:F0:A8:8E:4F:01:2A:73:53:66:FD:89:37:B8:4E:5A:29:89:A8:44:B6:06:3E
Signature algorithm name: SHA256withRSA
Subject Public Key Algorithm: 2048-bit RSA key
Version: 1
Trust this certificate? [no]:  yes
Certificate was added to keystore

Warning:
The JKS keystore uses a proprietary format. It is recommended to migrate to PKCS12 which is an industry standard format using "keytool -importkeystore -srckeystore keystore/kafka.keystore.jks -destkeystore keystore/kafka.keystore.jks -deststoretype pkcs12".

Now the keystore's signed certificate will be imported back into the keystore.

You will be prompted for the keystore's password.
Enter keystore password:  
Certificate reply was installed in keystore

Warning:
The JKS keystore uses a proprietary format. It is recommended to migrate to PKCS12 which is an industry standard format using "keytool -importkeystore -srckeystore keystore/kafka.keystore.jks -destkeystore keystore/kafka.keystore.jks -deststoretype pkcs12".

All done!

Delete intermediate files? They are:
 - 'ca-cert.srl': CA serial number
 - 'cert-file': the keystore's certificate signing request
   (that was fulfilled)
 - 'cert-signed': the keystore's certificate, signed by the CA, and stored back
    into the keystore
Delete? [yn] y
```

# 2. Trust storeからclient(pythonで利用する等のca.certを作る)

## der形式のcertを作る

### ホストで動かす場合

```
keytool -exportcert -keystore ./truststore/kafka.truststore.jks -alias caroot -file ./server_certs/ca-cert.der -storepass 123456
```

### dockerで動かす場合

```
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  keytool \
  keytool -exportcert \
    -keystore ./truststore/kafka.truststore.jks \
    -alias caroot \
    -file ./server_certs/ca-cert.der \
    -storepass 123456
```

## csrファイルを作成する

### ホストで動かす場合

```

keytool -certreq \
  -keystore ./keystore1/kafka1.keystore.jks \
  -alias localhost \
  -file ./server_certs/server.csr \
  -storepass 123456

```

### dockerで動かす場合

```
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  keytool \
  keytool -certreq \
    -keystore ./keystore1/kafka1.keystore.jks \
    -alias localhost \
    -file ./server_certs/server.csr \
    -storepass 123456
```

## 別名の確認は以下のコマンドで行います。

### ホストで動かす場合 

```

 keytool -list -v \
  -keystore ./keystore1/kafka1.keystore.jks \
  -storepass 123456

```

### dockerで動かす場合

```
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  keytool \
  keytool -list -v \
    -keystore ./keystore1/kafka1.keystore.jks \
    -storepass 123456
```

# 3. pem形式に変換
pem形式に変換してPython等のプログラムで利用する場合がある。

## ホストで動かす場合

```
openssl x509 -inform DER -in ./server_certs/ca-cert.der -out ./server_certs/ca-cert.pem
```

## dockerで動かす場合

```
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  keytool \
  openssl x509 -inform DER \
    -in ./server_certs/ca-cert.der \
    -out ./server_certs/ca-cert.pem
```

# 4. サーバー証明書crtを作成する


## ホストで動かす場合
```
openssl x509 -req \
  -in ./server_certs/server.csr \
  -CA ./server_certs/ca-cert.pem \
  -CAkey ./truststore/ca-key \
  -CAcreateserial \
  -out ./server_certs/server.crt \
  -days 365000 \
  -sha256 \
  -extfile ./kafkacreds/server.ext

```

## dockerで動かす場合

```
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  keytool \
  openssl x509 -req \
    -in ./server_certs/server.csr \
    -CA ./server_certs/ca-cert.pem \
    -CAkey ./truststore/ca-key \
    -passin pass:123456 \
    -CAcreateserial \
    -out ./server_certs/server.crt \
    -days 365000 \
    -sha256 \
    -extfile ./kafkacreds/server.ext
```

# 5. サーバー用のkeyを作成する

## 5.1 PKCS12形式に変換する

### ホストで動かす場合

```

keytool -importkeystore \
  -srckeystore ./keystore1/kafka1.keystore.jks \
  -srcstorepass 123456 \
  -srcalias localhost \
  -destkeystore ./server_certs/server.p12 \
  -deststoretype PKCS12 \
  -deststorepass 123456

```

### dockerで動かす場合

```
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  keytool \
  keytool -importkeystore \
    -srckeystore ./keystore1/kafka1.keystore.jks \
    -srcstorepass 123456 \
    -srcalias localhost \
    -destkeystore ./server_certs/server.p12 \
    -deststoretype PKCS12 \
    -deststorepass 123456 \
    -noprompt

```

## 5.2 pem形式に変換する

### ホストで動かす場合

```

openssl pkcs12 \
  -in ./server_certs/server.p12 \
  -nocerts \
  -nodes \
  -out ./server_certs/server-raw.key

```

### dockerで動かす場合


```
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  keytool \
  openssl pkcs12 \
    -in ./server_certs/server.p12 \
    -nocerts \
    -nodes \
    -out ./server_certs/server-raw.key \
    -passin pass:123456

```

## 5.3 key形式をpem形式に変換する

### ホストで動かす場合

```

openssl pkey \
  -in ./server_certs/server-raw.key \
  -out ./server_certs/server.key

```

### dockerで動かす場合

```
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  keytool \
  openssl pkey \
    -in ./server_certs/server-raw.key \
    -out ./server_certs/server.key
```

# 6. 必要ファイルをkeystore1,2にまとめる

kafkaで利用するために一つのフォルダにまとめてしまいます。

```
cp ./truststore/kafka.truststore.jks ./keystore1
cp ./truststore/kafka.truststore.jks ./keystore2
```

---以下は参考情報です

# （補助）各種ストアの確認コマンド

### トラストストアの内容を確認する
keytool -list -keystore ./secrets/ssl/kafka.truststore.jks -v

### 証明書の内容を確認する
keytool -printcert -file ./secrets/ssl/broker1.crt


## TLSの検証コマンド

たとえば、ワーキングコンテナから証明書を検証することができる

```
openssl s_client -debug -connect kafka1.local.data.platform:9092 -tls1_2
```

## subject確認
keytool -list -keystore ./kafka1/kafka.keystore.jks -alias caroot -v


# 自己証明書のサーバーへのインストール(Ubunts系統)

## サーバー自体へのインストール

いくつか設定の方法があります。

### 証明書をサーバー自身に設定する
COPY ca-cert.pem /usr/local/share/ca-certificates/self_signed_ca.crt
COPY server.crt /usr/local/share/ca-certificates/proxy_cert.crt

RUN update-ca-certificates

## keystoreへのインストール
workingのdocker-entrypoint.shを参照

に証明書を登録することで、Javaが自己証明書のような正式でない証明書を信頼するようにする。
$JAVA_HOME/lib/security/cacerts

## Python 用の certifi にも追加

必要に応じてcertifiにも

RUN pip install --upgrade certifi \
    && cat /usr/local/share/ca-certificates/proxy_cert.crt >> $(python -m certifi)


## CA関連ファイル
ca-cert.pem(CAの証明証)
ca-key(CAの秘密鍵)

証明書は全て自己証明書です。
証明書の有効期間は1000年としています。



## Truststore
Java関連の証明書設定

CAによってサインされている。
本環境でtruststoreが必要になった場合は使い回し可能。

証明書の有効期間は1000年としています(実運用では長くても1年程度を推奨)。

## Keystore

サーバーごとに個別のkeystoreを使用し、証明書を一意にします。
各証明書のCNまたはSANがそのBrokerのホスト名に対応するようにします。

そのため、各サーバーでkeysotreを作る必要がある。

おなじく、nginxに設定するサーバー証明書やクライアント証明書も同様であるが
*.local.data.platformを許可しているので、同様のホスト名であれば使いましが可能

証明書の有効期間は1000年としています(実運用では長くても1年程度を推奨)　。

# そのほかの証明書の設定値

altは以下のように設定しています。

```

[alt_names]
DNS.1 = localhost
DNS.2 = *.local.data.platform

```