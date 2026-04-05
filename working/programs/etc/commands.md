# aws cli band

aws configure set default.s3.max_bandwidth 1MB/s
aws configure set default.s3.max_concurrent_requests 2

aws --endpoint-url http://minio.local.data.platform:9000 s3 sync \
  /home/pyspark/spark/ \
  s3://dev-local-data-platform/spark-bin/ \
  --checksum-algorithm CRC32C \
  --exact-timestamps

# aws migrate
aws --endpoint-url http://minio.local.data.platform:9000 s3 sync \
  s3://misc-data-platform/warehouse/legacy.db/legacy_orders/ingest_date=2024-07-30/ \
  s3://dev-local-data-platform/warehouse/legacy.db/legacy_order/ingest_date=2024-07-03/ \
  --checksum-algorithm CRC32C \
  --exact-timestamps
