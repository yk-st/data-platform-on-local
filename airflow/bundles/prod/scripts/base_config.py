import os
import argparse

class BaseConfig:
    """全体共通の設定クラス"""
    POSTGRES_URL = os.getenv(
        "POSTGRES_URL",
        "jdbc:postgresql://host.docker.internal:5435/domain_database"
    )
    POSTGRES_USER = os.getenv("POSTGRES_USER", "domain")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "domain")

    # カタログ
    CATALOG_NAME = "local_data_platform"

    # Namespace名
    BRONZE_NAMESPACE = "brz_ingestion"
    SILVER_NAMESPACE = "slv_entities"
    GOLD_NAMESPACE = "gld_mart"
    GLD_FUND_NAMESPACE = "gld_fund"
    SILVER_FUND_NAMESPACE = "slv_fund"
    REF_NAMESPACE = "ref"

    # Namespace
    NAMESPACES = [
        f'{BRONZE_NAMESPACE}=s3a://local-data-platform/warehouse/slv/{BRONZE_NAMESPACE}.db',
        f'{SILVER_NAMESPACE}=s3a://local-data-platform/warehouse/slv/{SILVER_NAMESPACE}.db',
        f'{GOLD_NAMESPACE}=s3a://local-data-platform/warehouse/gld/{GOLD_NAMESPACE}.db',
        f'{GLD_FUND_NAMESPACE}=s3a://local-data-platform/warehouse/slv/{GLD_FUND_NAMESPACE}.db',
        f'{SILVER_FUND_NAMESPACE}=s3a://local-data-platform/warehouse/slv/{SILVER_FUND_NAMESPACE}.db',
        f'{REF_NAMESPACE}=s3a://local-data-platform/warehouse/ref/{REF_NAMESPACE}.db'
    ]

    # # マイグレーション用SQLパス (s3aスキーム)
    # MIGRATIONS_SQL_S3_PATH = os.getenv(
    #     "MIGRATIONS_SQL_S3_PATH",
    #     "s3a://data-platform-sql/section2/migrations.sql"
    # )

    # def create_snapshot_mappings(self, SNAPSHOT_MAPPING):
    #     MAPPING_ARGS = []
    #     for src_ns, src_tbl, dst_ns, dst_tbl in SNAPSHOT_MAPPING:
    #         MAPPING_ARGS += [
    #             "--mapping",
    #             f"{src_ns}.{src_tbl}.{dst_ns}.{dst_tbl}"
    #         ]
    #     print(f"Mapping: {src_ns}.{src_tbl} -> {dst_ns}.{dst_tbl}")
    #     return MAPPING_ARGS

    def get_iceberg_tables_string(self):
        """TABLESリストをカンマ区切り文字列に変換"""
        if hasattr(self, 'ICEBERG_TABLES') and self.ICEBERG_TABLES:
            return ",".join(self.ICEBERG_TABLES)
        return ""

    def get_namespaces_string(self):
        """NAMESPACESリストをカンマ区切り文字列に変換"""
        if hasattr(self, 'NAMESPACES') and self.NAMESPACES:
            return ",".join(self.NAMESPACES)
        return ""

    def get_dynamic_table_name(self, base_table: str, args) -> str:
        """
        実行モードに応じてテーブル名を動的に決定
        
        Args:
            base_table: 基本テーブル名 (例: local_data_platform.brz_raw.orders)
            args: parse_args()で取得した引数オブジェクト
            
        Returns:
            実際のテーブル名
        """
        if not hasattr(args, 'e2e_mode') or not hasattr(args, 'env'):
            # e2e_modeやenvが無い場合はそのまま返す
            print(f"🔍 get_dynamic_table_name: No e2e_mode/env found, returning base_table: {base_table}")
            return base_table
            
        if args.e2e_mode == "ZERO_COPY":
            # CTASテーブルの場合
            # local_data_platform.brz_raw.orders
            # → dev_local_data_platform.brz_raw_feature-xyz.orders
            parts = base_table.split(".", 2)
            if len(parts) == 3:
                catalog, namespace, table = parts
                result = f"dev_{catalog}.{namespace}_{args.env}.{table}.branch_{args.env}"
                print(f"🔍 get_dynamic_table_name: ZERO_COPY mode - {base_table} → {result}")
                return result
            else:
                print(f"🔍 get_dynamic_table_name: ZERO_COPY mode - invalid format, returning base_table: {base_table}")
                return base_table
        
        elif args.e2e_mode == "BRANCH":
            # ブランチテーブルの場合
            # local_data_platform.brz_raw.orders
            # → local_data_platform.brz_raw.orders.branch_feature-xyz
            if args.env != "main":
                result = f"{base_table}.branch_{args.env}"
                print(f"🔍 get_dynamic_table_name: BRANCH mode - {base_table} → {result}")
                return result
            else:
                print(f"🔍 get_dynamic_table_name: BRANCH mode (main env) - returning base_table: {base_table}")
                return base_table
        
        else:
            # デフォルト（通常のテーブル）
            print(f"🔍 get_dynamic_table_name: Unknown mode ({args.e2e_mode}) - returning base_table: {base_table}")
            return base_table
    
    def get_table_with_mode(self, table_attr_name: str, args) -> str:
        """
        クラス属性からテーブル名を取得し、実行モードに応じて変換
        
        Args:
            table_attr_name: クラス属性名 (例: "TABLE_ORDERS")
            args: parse_args()で取得した引数オブジェクト
            
        Returns:
            実際のテーブル名
        """
        if hasattr(self, table_attr_name):
            base_table = getattr(self, table_attr_name)
            return self.get_dynamic_table_name(base_table, args)
        else:
            raise AttributeError(f"Table attribute '{table_attr_name}' not found in {self.__class__.__name__}")

    @classmethod
    def parse_args(cls) -> argparse.Namespace:
        """
        引数をすべてここで宣言する。
        Airflow の SparkSubmitOperator から渡す場合は
        application_args=[ "--env", "stg", "--ingest-date", "{{ ds }}" ] のように指定。
        """
        parser = argparse.ArgumentParser(
            prog="foo_job",
            description="Bronze -> Silver 変換 Job（べき等処理付き）",
        )

        # --- 必須パラメータ ----------------------------------------------------
        parser.add_argument(
            "--env",
            default="main",
            required=True,
            help="実行環境（main/local/stg/prod または任意のブランチ名）。デフォルト: main",
        )
        parser.add_argument(
            "--ingest-date",
            metavar="YYYY-MM-DD",
            required=True,
            help="処理対象日（Airflow マクロ {{ ds }} 相当）",
        )

        parser.add_argument(
            "--run-id",
            default=os.getenv("RUN_ID", "unknown"),
            required=True,
            help="Airflow の run_id をそのまま渡しておくとデバッグが楽",
        )

        parser.add_argument(
            "--e2e-mode",
            default="BRANCH",
            choices=["BRANCH", "ZERO_COPY"],
            required=True,
            help="Airflow の e2e_mode を指定。BRANCH: 通常のブランチテスト(mainの場合はmainブランチで実行), ZERO_COPY: ZERO_COPYテスト",
        )

        parser.add_argument(
            "--extract-mode",
            default="logical_date",
            choices=["logical_date", "ALL"],
            required=True,
            help="Airflow の extract_mode を指定。logical_date: ds を基準に抽出, ALL: 全件抽出",
        )

        # --- 任意パラメータ ----------------------------------------------------

        parser.add_argument(
            "--master-data",
            default="v1",
            choices=["v1", "v2"],
            required=False,
            help="Airflow の master_data を指定。v1: オリジナルのマスター, v2: 一部変更したマスター",
        )


        # parser.add_argument(
        #     "-c", "--catalog", default="", help="Iceberg カタログ名 (省略可)")
        # parser.add_argument(
        #     "-n", "--ns", action="append", metavar="NS=LOCATION",
        #             help="個別名前空間 (複数指定可)")
        # parser.add_argument(
        #     "-N", "--namespaces", metavar="LIST",
        #             help="カンマ・空白区切りでまとめた名前空間一覧")
        parser.add_argument("-T", "--tables",  metavar="LIST",
                    help="カンマ or 空白区切りテーブル一覧。FQN形式で指定。例: local_data_platform.slv_analytics.orders,local_data_platform.slv_analytics.products")

        return parser.parse_args()