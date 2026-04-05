#!/bin/bash

# シンプルなIcebergテーブル一括作成スクリプト

set -e  # エラー時に停止

# 設定
SQL_BASE_DIR="/opt/airflow/sql"
LOG_FILE="/opt/airflow/logs/create_tables_$(date +%Y%m%d_%H%M%S).log"

# ログディレクトリ作成
mkdir -p "$(dirname "$LOG_FILE")"

echo "=== Icebergテーブル一括作成開始 ===" | tee -a "$LOG_FILE"
echo "開始時刻: $(date)" | tee -a "$LOG_FILE"

# 実行順序（依存関係順）
NAMESPACES=("namespace" "ref" "brz_ingestion" "slv_entities" "gld_fund" "gld_mart" "legacy" "slv_fund")

# 各ネームスペースのSQLファイルを順番に実行
for namespace in "${NAMESPACES[@]}"; do
    sql_dir="$SQL_BASE_DIR/$namespace"
    
    echo "### ネームスペース: $namespace ###" | tee -a "$LOG_FILE"
    
    if [ ! -d "$sql_dir" ]; then
        echo "ディレクトリが存在しません: $sql_dir" | tee -a "$LOG_FILE"
        continue
    fi
    
    # SQLファイルを順番に実行
    find "$sql_dir" -name "*.sql" -type f | sort | while read -r sql_file; do
        echo "実行中: $sql_file" | tee -a "$LOG_FILE"
        
        # Spark SQLで直接実行
        if spark-sql $ICE_EXT \
            --name "CreateTables-$(basename "$sql_file")" \
            -f "$sql_file" 2>&1 | tee -a "$LOG_FILE"; then
            echo "✅ 成功: $(basename "$sql_file")" | tee -a "$LOG_FILE"
        else
            echo "❌ エラー: $(basename "$sql_file")" | tee -a "$LOG_FILE"
            # エラーが発生しても続行
        fi
        
        echo "---" | tee -a "$LOG_FILE"
    done
    
    echo "" | tee -a "$LOG_FILE"
done

# 結果確認
echo "### 作成結果確認 ###" | tee -a "$LOG_FILE"

echo "ネームスペース一覧:" | tee -a "$LOG_FILE"
spark-sql $ICE_EXT \
    -e "USE local_data_platform; SHOW NAMESPACES;" 2>&1 | tee -a "$LOG_FILE"

echo "テーブル一覧:" | tee -a "$LOG_FILE"
for namespace in "${NAMESPACES[@]}"; do
    if [ "$namespace" != "namespace" ]; then
        echo "  === $namespace ===" | tee -a "$LOG_FILE"
        spark-sql $ICE_EXT \
            -e "USE local_data_platform; SHOW TABLES IN $namespace;" 2>&1 | tee -a "$LOG_FILE"
    fi
done

echo "=== 処理完了 ===" | tee -a "$LOG_FILE"
echo "終了時刻: $(date)" | tee -a "$LOG_FILE"
echo "ログファイル: $LOG_FILE"