import os

from airflow.sdk import AssetAlias

from airflow import Asset
from data_pipelines import (
    USER_REPORT,
    DATALAKE_SILVER_LOCATION,
    DATALAKE_BRONZE_LOCATION,
    DATALAKE_GOLD_LOCATION,
)

GOLD_USERS = Asset(
    name="users_without_pii",
    uri=os.path.join("duckdb://", DATALAKE_GOLD_LOCATION, "users.db"),
    group=USER_REPORT,
    extra={
        "owner": "user-team@taxfix.de",
    },
)
SILVER_USERS = Asset(
    name="users",
    uri=os.path.join("duckdb://", DATALAKE_SILVER_LOCATION, "users.db"),
    group=USER_REPORT,
    extra={
        "owner": "user-team@taxfix.de",
    },
)
BRONZE_USERS_CDC = Asset(
    name="users_cdc",
    uri=os.path.join("duckdb://", DATALAKE_BRONZE_LOCATION, "users_cdc.db"),
    group=USER_REPORT,
    extra={
        "owner": "user-team@taxfix.de",
    },
)
BRONZE_USERS_CDC_ALIAS = AssetAlias(f"bronze-{BRONZE_USERS_CDC.name}")
