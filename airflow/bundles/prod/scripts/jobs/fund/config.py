import os
from base_config import BaseConfig

class FundConfig(BaseConfig):
    def __init__(self):
        self.TABLE_FUND_NAV = f"{self.CATALOG_NAME}.{self.BRONZE_NAMESPACE}.fund_nav"
        self.TABLE_FUND_MASTER = f"{self.CATALOG_NAME}.{self.BRONZE_NAMESPACE}.fund_master"
        self.TABLE_DIM_FUND = f"{self.CATALOG_NAME}.{self.SILVER_FUND_NAMESPACE}.dim_fund"
        self.TABLE_DIM_DATE = f"{self.CATALOG_NAME}.{self.SILVER_FUND_NAMESPACE}.dim_date"

        self.TABLE_FCT_FUND_PERFORMANCE = f"{self.CATALOG_NAME}.{self.SILVER_FUND_NAMESPACE}.fct_fund_performance"
        self.TABLE_FUND_DAILY_WIDE = f"{self.CATALOG_NAME}.{self.SILVER_FUND_NAMESPACE}.fund_daily_wide"

        self.FUND_MASTER_S3_SOURCE="s3a://data-source/fund/fund_master"
        self.FUND_NAV_S3_SOURCE="s3a://data-source/fund/fund_nav"

        self.ICEBERG_TABLES = [
            self.TABLE_FUND_NAV,
            self.TABLE_FUND_MASTER,
            self.TABLE_FUND_DAILY_WIDE,
            self.TABLE_DIM_FUND,
            self.TABLE_DIM_DATE,
            self.TABLE_FCT_FUND_PERFORMANCE
        ]