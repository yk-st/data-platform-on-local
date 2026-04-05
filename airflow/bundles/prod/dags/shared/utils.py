# dags/_shared/dataset_utils.py

from airflow.datasets import Dataset
from airflow.models import Variable, Param

def iceberg_dataset(fqn: str, env: str) -> Dataset:
    """fqn: catalog.namespace.table → Dataset("iceberg://catalog/namespace.table")"""
    catalog, namespace_table = fqn.split(".", 1)
    return Dataset(f"iceberg://{env}/{catalog}{namespace_table}", extra={"env": env})

def mongodb_dataset(fqn: str, env: str) -> Dataset:
    """fqn: database.collection → Dataset("mongodb://database/collection")"""
    database, collection = fqn.split(".", 1)
    return Dataset(f"mongodb://{env}/{database}/{collection}", extra={"env": env})

def s3_dataset(uri: str, env: str) -> Dataset:
    """s3a://bucket/key … をそのまま Dataset に包む"""
    return Dataset(f"{uri}@{env}", extra={"env": env})
