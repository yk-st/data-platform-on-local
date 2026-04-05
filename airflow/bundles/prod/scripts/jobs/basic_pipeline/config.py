import os
from base_config import BaseConfig

class BasicPipelineConfig(BaseConfig):
    def __init__(self):

        self.TABLE_ORDERS = f"{self.CATALOG_NAME}.{self.BRONZE_NAMESPACE}.orders"
        self.TABLE_PRODUCTS = f"{self.CATALOG_NAME}.{self.BRONZE_NAMESPACE}.products"
        self.TABLE_USERS = f"{self.CATALOG_NAME}.{self.BRONZE_NAMESPACE}.users"
        self.TABLE_USER_ORDERS_WIDE = f"{self.CATALOG_NAME}.{self.SILVER_NAMESPACE}.user_orders_wide"
        self.TABLE_USER_SALES = f"{self.CATALOG_NAME}.{self.GOLD_NAMESPACE}.user_sales"
        self.TABLE_DIM_CUSTOMER = f"{self.CATALOG_NAME}.{self.SILVER_NAMESPACE}.dim_customer"
        self.TABLE_DIM_PRODUCT = f"{self.CATALOG_NAME}.{self.SILVER_NAMESPACE}.dim_product"
        self.TABLE_DIM_DATE = f"{self.CATALOG_NAME}.{self.SILVER_NAMESPACE}.dim_date"
        self.TABLE_FACT_ORDERS = f"{self.CATALOG_NAME}.{self.SILVER_NAMESPACE}.fact_orders"

        self.ICEBERG_TABLES = [
            self.TABLE_ORDERS,
            self.TABLE_PRODUCTS,
            self.TABLE_USERS,
            self.TABLE_USER_ORDERS_WIDE,
            self.TABLE_USER_SALES,
            self.TABLE_DIM_CUSTOMER,
            self.TABLE_DIM_PRODUCT,
            self.TABLE_DIM_DATE,
            self.TABLE_FACT_ORDERS
        ]