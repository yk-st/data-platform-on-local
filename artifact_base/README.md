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

![Spark Version](https://img.shields.io/badge/Spark-3.5.6-orange)

# artifact_base

Sparkの環境統一と利用者における手順の簡略化のため事前に必要な設定を施したイメージをビルドしています。
事前にビルドしたイメージは以下にPushされています。

Docker Hub
- https://hub.docker.com/repository/docker/yukisaito/spark/tags

DebianのBookwormをベースにしています。

## マルチステージビルド

本ビルドのイメージは単純にコンテナ起動のイメージとしてだけでなく、
マルチステージビルド時にライブラリのコピー元にもなっています。

共通のライブラリ(jar)が多いためこのような方式をとっています。

マルチステージビルドの例

```

# 必要なライブラリ
FROM yukisaito/spark:3.5.3 AS base_image
FROM apache/hive:4.0.0
USER root
ARG COPY_TO=/opt/hive/lib/
COPY --from=base_image /root/depend_jars/*.jar ${COPY_TO}
```

# 接続情報

Sparkの接続作成の簡略化のため、SparkのDockerイメージへ「spark_utils.py(SparkSessionを作成するUtil)」「schema_utils.py(Avroのスキーマエボリューションで自動的に過去のスキーマへフォールバックするUtil)」を事前にPYTHONPATHに追加しています。

使用自体は任意ですが、本書で利用しているPysparkのプログラムはこれらのUtilを利用しています。

## スキーマレジストリからのデータ取得

```artifact_base/schema_utils.py
def fetch_schema(subject: str, version: int, registry: str) -> str:
    """Schema Registry REST から schema JSON (str) を取得"""
    url = f"{registry.rstrip('/')}/subjects/{subject}/versions/{version}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["schema"]
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Failed to GET {url} – {e.code} {e.reason}") from e

```s

# (参考) 筆者のローカルで本イメージをビルドした時のコマンド
docker buildx create --name mybuilder --use
docker login
cd artifact_base
docker buildx build --memory=8g --platform linux/amd64,linux/arm64 \
    -t yukisaito/spark:3.5.5 \
    --push .
docker buildx rm mybuilder