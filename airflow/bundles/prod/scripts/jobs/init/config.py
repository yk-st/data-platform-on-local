import os
from base_config import BaseConfig

class IntConfig(BaseConfig):
    def __init__(self,):
        self.TABLE_ZIP_GEOCODE = f"{self.CATALOG_NAME}.{self.REF_NAMESPACE}.zip_geocode"
        self.TABLE_AGE_GROUP_REF = f"{self.CATALOG_NAME}.{self.REF_NAMESPACE}.age_group_ref"
        self.TABLE_COLUMN_ALIAS_MAP = f"{self.CATALOG_NAME}.{self.REF_NAMESPACE}.column_alias_map"

        self.ZIP_GEOCODE_S3_SOURCE="s3a://data-source/ref/zip_geocode.csv"
        self.AGE_GROUP_REF_S3_SOURCE="s3a://data-source/ref/age_group_ref.csv"

        self.ICEBERG_TABLES = [
            self.TABLE_ZIP_GEOCODE,
            self.TABLE_AGE_GROUP_REF,
            self.TABLE_COLUMN_ALIAS_MAP
        ]