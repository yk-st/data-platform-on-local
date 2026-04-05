# branch.py 主要部分だけ完全版

import re, sys
from spark_utils import get_spark
from pyspark.sql import SparkSession
from base_config import BaseConfig

# ---------- CTASテーブルを作る ----------
def create_ctas_table(
    spark: SparkSession,
    config: BaseConfig,
    args,
    tables: str  # カンマ区切りの文字列
) -> list[str]:  # 作成したCTASテーブルのFQNリストを返す
    """
    CTAS (CREATE TABLE AS SELECT) でブランチテーブルを作成
    BaseConfigのget_dynamic_table_nameを使用してロジックを統一
    """
    success_count = 0
    created_tables = []  # 作成成功したテーブルのFQNを保存
    
    # カンマ区切りの文字列を分割
    table_list = []
    for raw in re.split(r"[,\s]+", tables.strip()):
        if raw:
            table_list.append(raw)
    
    for table_fqn in table_list:
        try:
            # BaseConfigのロジックを使用してZERO_COPYモードでのテーブル名を取得
            temp_args = type('Args', (), {
                'e2e_mode': 'ZERO_COPY',
                'env': args.env
            })()
            
            target_fqn = config.get_dynamic_table_name(table_fqn, temp_args)
            
            print(f"Creating CTAS table: {target_fqn}")
            print(f"  Source: {table_fqn}.branch_main")
            
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {target_fqn}
                USING iceberg
                AS
                SELECT * FROM {table_fqn}.branch_main
                WHERE 1 = 0
            """)
            
            print(f"✔ Successfully created CTAS table: {target_fqn}")
            created_tables.append(target_fqn)  # 成功したテーブルを追加
            success_count += 1
            
        except Exception as e:
            print(f"❌ Failed to create CTAS table from {table_fqn}: {str(e)}")
            raise e  # エラーで終了
    
    print(f"📊 CTAS table creation summary: {success_count} success")
    return created_tables

# ---------- ブランチを作る ----------  
def create_branches(
    spark: SparkSession,
    config: BaseConfig,
    args,
    tables: str  # カンマ区切りの文字列
) -> None:
    """
    ブランチを作成
    BRANCHモードの場合は元のテーブルに対してブランチを作成
    """
    # カンマ区切りの文字列を分割
    table_list = []
    for raw in re.split(r"[,\s]+", tables.strip()):
        if raw:
            table_list.append(raw)
    
    for table_fqn in table_list:
        try:
            if args.e2e_mode == "BRANCH":
                # BRANCHモードの場合は元のテーブルに対してブランチを作成
                target_table = table_fqn  # 元のテーブルをそのまま使用
                print(f"🔍 create_branches: BRANCH mode - using original table: {target_table}")
            else:
                # その他の場合は動的テーブル名を使用
                target_table = config.get_dynamic_table_name(table_fqn, args)
                print(f"🔍 create_branches: {args.e2e_mode} mode - using dynamic table: {target_table}")
            
            spark.sql(f"""
                ALTER TABLE {target_table}
                  CREATE BRANCH IF NOT EXISTS {args.env}
            """)
            print(f"✔ created branch {args.env} on {target_table}")
            
        except Exception as e:
            print(f"❌ Failed to create branch on {table_fqn}: {str(e)}")
            raise e  # エラーで終了

# ---------- メイン ----------
if __name__ == "__main__":
    try:
        config = BaseConfig()
        args = config.parse_args()
        spark = get_spark()

        if args.e2e_mode == "ZERO_COPY":
            # 1. CTASテーブルを作成
            print(f"🔄 Creating CTAS tables for environment: {args.env}")
            ctas_tables = create_ctas_table(
                spark,
                config=config,
                args=args,
                tables=args.tables
            )

            # 2. CTASテーブルにブランチを作成
            if ctas_tables:
                print(f"🌿 Creating branches on {len(ctas_tables)} CTAS tables for environment: {args.env}")
                
                # CTASテーブル名をそのまま使用
                #ctas_tables_str = ",".join(ctas_tables)
                create_branches(
                    spark,
                    config=config,
                    args=args,
                    tables=args.tables
                )
            else:
                print("❌ No CTAS tables were created successfully")
                sys.exit(1)
                
        elif args.e2e_mode == "BRANCH":
            print(f"🌿 Creating branches on original tables for environment: {args.env}")
            create_branches(
                spark,
                config=config,
                args=args,
                tables=args.tables
            )
            
        else:
            sys.exit(f"ERROR: Unsupported e2e_mode: {args.e2e_mode}")
            
        print("🎉 Bootstrap completed successfully!")
        
    except Exception as e:
        print(f"💥 Bootstrap failed: {str(e)}")
        sys.exit(1)
    finally:
        # Sparkセッションのクリーンアップ
        try:
            spark.stop()
        except:
            pass