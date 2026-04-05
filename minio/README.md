
# はじめに

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


# Mnio

https://min.io/

MiniOは、Amazon S3と互換性のあるオブジェクトストレージです。
本書では主にデータレイクやDWHにおけるデータの置き場や処理途中の中間ファイルの置き場として利用しています。

留意点としては、本来はデータソース側とデータ分析基盤側でそれぞれMiniOを用意するのが好ましいですが、
設定の煩雑さ等を顧みてデータソース側と共通のMinio環境を利用しています。

# 接続情報

## ホストからのアクセス

データを配置したりする操作がGUIで可能です。

| 項目               | アクセス                                                                | ユーザー | パスワード |
|--------------------|-----------------------------------------------------------------------|----------|------------|
| Web UI| https://localhost/ui/     | admin       | admin123          |


GUI操作における認可は行なっておりません。

## ワーキングコンテナからのアクセス

| 項目                   | アクセス                                                     |
|------------------------|--------------------------------------------------------|
| MinIO エンドポイントURL | http://minio.local.data.platform:9000                  |

システム的に利用する場合は上記のURLを利用します。
例えば、ワーキングコンテナからaws cliでバケットの中身を見たい場合は以下のような方法で実行可能です。

接続例

```
export AWS_ACCESS_KEY_ID=5nCJP6jHFJd7PDsLlT3a
export AWS_SECRET_ACCESS_KEY=FXn6MFKDbNamyMzxiMBGIpgTDFu2r1IfymESfRJd

aws --endpoint-url http://minio.local.data.platform:9000 s3 ls s3://local-data-platform/
```

### アクセストークンによる認証認可
本書では共通して利用する、ACCESS KEY と SECRET KEYを用意しています。

1. Superユーザ（操作に制限なし）

```
AWS_ACCESS_KEY_ID=5nCJP6jHFJd7PDsLlT3a
AWS_SECRET_ACCESS_KEY=FXn6MFKDbNamyMzxiMBGIpgTDFu2r1IfymESfRJd
```

2. ディレクトリ側で認可を確認するために制限のあるユーザ

```
AWS_ACCESS_KEY_ID=6nCJP6jHFJd7PDsLlT3a
AWS_SECRET_ACCESS_KEY=GXn6MFKDbNamyMzxiMBGIpgTDFu2r1IfymESfRJd
```

ポリシー制限されており、local-data-platformバケットなど一部のデータを参照することができません。

## SSL
SSL化なし

# 初期設定

## バケットの作成

以下のバケットをブートストラップ時に作成済みです。

- local-data-platform:　ローカル環境におけるデータレイク(もしくはDWH)
- dev-local-data-platform:　開発環境におけるデータレイク(もしくはDWH)
- data-source: データソースのデータ
- process-bucket: 処理の中間ファイルなどの配置場所
- misc-data-platform: 雑多なデータ置き場

## 固定データの配置
接続情報に記載のバケットなどを作成しdata-sourceバケットへ初期データを配置している。

fixed_dataset/fund -> ファンド関連のCSVデータ
fixed_dataset/legacy -> 別システム or 旧システム想定のファンドやOrdersデータのCSV
fixed_dataset/ref -> 参照データ用CSV
fixed_dataset/wrangling -> ラングリング用データ(pdfやexecelなど)

# コマンドスニペット

```
export AWS_ACCESS_KEY_ID=5nCJP6jHFJd7PDsLlT3a
export AWS_SECRET_ACCESS_KEY=FXn6MFKDbNamyMzxiMBGIpgTDFu2r1IfymESfRJd

aws --endpoint-url http://minio.local.data.platform:9000 s3 ls s3://local.data.platform/

export AWS_ACCESS_KEY_ID=6nCJP6jHFJd7PDsLlT3a
export AWS_SECRET_ACCESS_KEY=GXn6MFKDbNamyMzxiMBGIpgTDFu2r1IfymESfRJd

aws --endpoint-url http://minio.local.data.platform:9000 s3 ls s3://migration.data.platform/

エラーになります。

pyspark@work:~$ aws --endpoint-url http://minio.local.data.platform:9000 s3 ls s3://local.data.platform/

An error occurred (AccessDenied) when calling the ListObjectsV2 operation: Access Denied.
pyspark@work:~$ aws --endpoint-url http://minio.local.data.platform:9000 s3 ls s3://migration.data.platform/
```

# MinoのDocker image配布停止
MinioのDockerのイメージ提供が終了しています。
仮にDocker Hubのイメージが見つからなくなってしまった場合は、以下のミラーHubを利用してください。

https://hub.docker.com/repository/docker/yukisaito/minio/general

## ライセンス

これは minio/minio:RELEASE.2025-07-18T21-56-31Z の unmodified mirror です。
書籍付録の再現性確保のためにミラーしています。公式配布元ではありません。

```
Mirror image:
- yuki-saito/minio:RELEASE.2025-07-18T21-56-31Z

Upstream image:
- minio/minio:RELEASE.2025-07-18T21-56-31Z
- (optional) digest: sha256:a0fe26595711d0fb93dd28e24552520f68897195f4a23f5a17ffa9924ec3fac

Corresponding Source (upstream):
- https://github.com/minio/minio
- Release tag: RELEASE.2025-07-18T21-56-31Z

License:
- MinIO is dual-licensed; this image is provided under GNU AGPL v3.
- https://www.min.io/commercial-license
- AGPL v3 text: https://www.gnu.org/licenses/agpl-3.0.en.html

Not affiliated with MinIO, Inc. Use at your own risk.

```

## 参考コマンド
筆者の環境でイメージをミラー(改変なし)した時のコマンド

docker buildx imagetools create \
  --tag yukisaito/minio:RELEASE.2025-07-18T21-56-31Z \
  minio/minio:RELEASE.2025-07-18T21-56-31Z

docker buildx imagetools inspect yukisaito/minio:RELEASE.2025-07-18T21-56-31Z
sha256:da0fe26595711d0fb93dd28e24552520f68897195f4a23f5a17ffa9924ec3fac


## 今後の更新予定

https://hub.docker.com/r/pgsty/minio/tags

pagstyへの変更を予定しています。