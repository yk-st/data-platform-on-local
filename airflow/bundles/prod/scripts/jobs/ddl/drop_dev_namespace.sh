# ── ここだけ環境に合わせて ─────────────────────────────
CAT="dev_local_data_platform"       # Iceberg カタログ名
SPARK_SQL="spark-sql"
# ────────────────────────────────────────────────

${SPARK_SQL} ${ICE_EXT} --silent -e "USE  ${CAT}; SHOW NAMESPACES;" \
| tail -n +1 | awk '{print $1}' | grep -vE '^(default|system)$' \
| while read NS; do
  echo "### ${NS}"
  # テーブル一覧 → 各テーブルを即時削除
  ${SPARK_SQL} ${ICE_EXT} --silent -e "USE ${CAT}; SHOW TABLES IN ${NS};" \
  | tail -n +1 | awk '{print $1}' \
  | while read TBL; do
      echo "  DROP TABLE ${NS}.${TBL}"
      ${SPARK_SQL} ${ICE_EXT} --silent -e "USE  ${CAT}; DROP TABLE ${NS}.${TBL};"
    done
  # 空になった名前空間を削除
  echo "  DROP NAMESPACE ${NS}"
  ${SPARK_SQL} ${ICE_EXT} --silent -e "USE ${CAT}; DROP NAMESPACE IF EXISTS ${NS};"
done
