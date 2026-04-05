import os
from base_config import BaseConfig

class UtilizeConfig(BaseConfig):
    def __init__(self):

        self.MONGO_DB="user_data"

        self.TABLE_USER_ORDERS_WIDE_WEATHER_ENRICHED = f"{self.CATALOG_NAME}.{self.SILVER_NAMESPACE}.user_orders_wide_weather_enriched"
        self.TABLE_USER_CTX_FEATURE = f"{self.CATALOG_NAME}.{self.GOLD_NAMESPACE}.user_ctx_feature"
        self.TABLE_USER_CTX_MONGO = f"{self.MONGO_DB}.user_ctx"

        self.ICEBERG_TABLES = [
            self.TABLE_USER_ORDERS_WIDE_WEATHER_ENRICHED,
            self.TABLE_USER_CTX_FEATURE
        ]