# pdf_to_spark.py  ― S3 に置いた PDF を読み取り Spark DataFrame 化
from io import BytesIO
import os
import re
import s3fs 
from pdfminer.high_level import extract_text
import pandas as pd
from spark_utils import get_spark

# ---------------------------------------------------------------------------
# 1. S3 接続設定 ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――
#    └ MinIO を使う場合は endpoint_url に MinIO の URL を渡す
#      （Glue など AWS 環境なら endpoint_url は省略して OK）
# ---------------------------------------------------------------------------
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID",     "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
S3_ENDPOINT_URL       = os.getenv("S3_ENDPOINT_URL") 
S3_URI                = "s3://data-source/wrangling/no_table_pdf.pdf"

fs_kwargs = {"anon": False}
if S3_ENDPOINT_URL:                     # MinIO / ローカル S3
    fs_kwargs["client_kwargs"] = {"endpoint_url": S3_ENDPOINT_URL}

fs = s3fs.S3FileSystem(**fs_kwargs)

# ---------------------------------------------------------------------------
# 2. PDF → 文字列抽出
# ---------------------------------------------------------------------------
with fs.open(S3_URI, "rb") as f:
    text = extract_text(f)

# 空行除去
lines = list(filter(None, text.split("\n")))

# ---------------------------------------------------------------------------
# 3. 行 → dict → Pandas → Spark
# ---------------------------------------------------------------------------
data = {
    lines[0]: lines[2],
    lines[1]: lines[3],
    lines[4]: lines[5],
}

# 行を列に転置
pdf_pd = pd.DataFrame.from_dict(data, orient="index").T

spark = get_spark(app_name="PdfToSpark")
pdf_spark = spark.createDataFrame(pdf_pd)

pdf_spark.printSchema()
pdf_spark.show(truncate=False)
