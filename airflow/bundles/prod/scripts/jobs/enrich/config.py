import os
from base_config import BaseConfig

class EnrichConfig(BaseConfig):
    def __init__(self):

        self.TABLE_USER_ORDERS_WIDE = f"{self.CATALOG_NAME}.{self.SILVER_NAMESPACE}.user_orders_wide"
        self.TABLE_ZIP_GEOCODE = f"{self.CATALOG_NAME}.{self.REF_NAMESPACE}.zip_geocode"
        self.TABLE_USER_ORDERS_WIDE_ENRICHED = f"{self.CATALOG_NAME}.{self.SILVER_NAMESPACE}.user_orders_wide_enriched"
        self.TABLE_USER_ORDERS_WIDE_WEATHER_ENRICHED = f"{self.CATALOG_NAME}.{self.SILVER_NAMESPACE}.user_orders_wide_weather_enriched"

        self.ICEBERG_TABLES = [
            self.TABLE_USER_ORDERS_WIDE,
            self.TABLE_ZIP_GEOCODE,
            self.TABLE_USER_ORDERS_WIDE_ENRICHED,
            self.TABLE_USER_ORDERS_WIDE_WEATHER_ENRICHED
        ]