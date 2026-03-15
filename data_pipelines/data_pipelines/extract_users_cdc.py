import logging
from datetime import timedelta, datetime
from typing import Iterator

import duckdb
from _duckdb import IOException
from airflow.sdk import dag, task, Metadata
from pendulum import DateTime

from data_pipelines import DATALAKE_BRONZE_LOCATION
from data_pipelines import assets
from data_pipelines.queries import (
    CREATE_USER_CDC_TABLE,
    TABLE_NAME_PLACEHOLDER,
    INSERT_USER_CDC_EVENTS,
    CDC_EVENT_FILE_PLACEHOLDER,
)


@dag(
    schedule=timedelta(minutes=1),
    start_date=datetime(2026, 2, 5, 16, 20),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2},
)
def extract_user_cdc() -> None:
    @task()
    def resolve_cdc_users_path(logical_date: DateTime | None = None) -> str:
        if not logical_date:
            raise RuntimeError("logical date is required")

        return str(
            DATALAKE_BRONZE_LOCATION
            / "users"
            / logical_date.format("YYYY")
            / logical_date.format("MM")
            / logical_date.format("DD")
            / logical_date.format("HH")
            / logical_date.format("mm")
            / "events-*.jsonl"
        )

    @task(outlets=[assets.BRONZE_USERS_CDC_ALIAS])
    def ingest_users_cdc(ingestion_path: str) -> Iterator[Metadata]:
        logging.info(f"Start ingesting '{ingestion_path}'")
        try:
            with duckdb.connect(assets.BRONZE_USERS_CDC.uri) as connection:
                query = f"""
                -- initially create the table with given schema
                {CREATE_USER_CDC_TABLE.replace(TABLE_NAME_PLACEHOLDER, assets.BRONZE_USERS_CDC.name)}
                {INSERT_USER_CDC_EVENTS.replace(TABLE_NAME_PLACEHOLDER, assets.BRONZE_USERS_CDC.name).replace(CDC_EVENT_FILE_PLACEHOLDER, str(ingestion_path))}
                """
                result = connection.execute(query).fetchone()
                if not result:
                    raise RuntimeError("Failed to insert data")

                rows_inserted = result[0]
                logging.info(f"Inserted {rows_inserted} rows")
            logging.info("Done ingesting")
            yield Metadata(
                asset=assets.BRONZE_USERS_CDC,
                extra={"rows_inserted": rows_inserted, "glob": ingestion_path},
                alias=assets.BRONZE_USERS_CDC_ALIAS,
            )
        except IOException as error:
            logging.warning("Was not able to find cdc files, skipping", exc_info=error)

    ingest_users_cdc(resolve_cdc_users_path())  # type: ignore[arg-type]


extract_user_cdc()
