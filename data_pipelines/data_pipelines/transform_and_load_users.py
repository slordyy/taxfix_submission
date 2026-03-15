import logging
from typing import Iterator

import duckdb
from airflow.sdk import dag, task, Metadata
from airflow.sdk.execution_time.context import InletEventsAccessors

from data_pipelines import assets
from data_pipelines.queries import (
    TABLE_NAME_PLACEHOLDER,
    SELECT_USER_TABLE_WITHOUT_PII,
    SELECT_USER_TABLE_SNAPSHOT_WITH_HISTORY,
)


@dag(
    schedule=assets.BRONZE_USERS_CDC,
    max_active_runs=1,
    default_args={"retries": 2},
)
def transform_and_load_users() -> None:
    @task(inlets=[assets.BRONZE_USERS_CDC], outlets=[assets.SILVER_USERS])
    def reconstruct_snapshot(
        *, inlet_events: InletEventsAccessors
    ) -> Iterator[Metadata]:
        events = inlet_events[assets.BRONZE_USERS_CDC]
        latest_event = events[-1]
        glob = latest_event.extra["glob"]  # type: ignore
        logging.info(
            f"Creating users table snapshot including latest partition: {glob}"
        )
        with duckdb.connect(assets.SILVER_USERS.uri) as connection:
            attachment_name = "bronze"
            connection.execute(
                f"ATTACH '{assets.BRONZE_USERS_CDC.uri}' AS {attachment_name} (READ_ONLY)"
            )
            fully_quantified_source = (
                f"{attachment_name}.{assets.BRONZE_USERS_CDC.name}"
            )
            result = connection.execute(
                f"CREATE OR REPLACE TABLE {assets.SILVER_USERS.name} AS {SELECT_USER_TABLE_SNAPSHOT_WITH_HISTORY.replace(TABLE_NAME_PLACEHOLDER, fully_quantified_source)}"
            ).fetchone()
            if not result:
                raise RuntimeError("Failed to insert data")

            rows_inserted = result[0]
            logging.info(f"Inserted {rows_inserted} rows")

        logging.info("Done creating snapshot")
        yield Metadata(
            asset=assets.SILVER_USERS,
            extra={"rows_inserted": rows_inserted},
        )

    @task(inlets=[assets.SILVER_USERS], outlets=[assets.GOLD_USERS])
    def load_users_without_pii() -> Iterator[Metadata]:
        logging.info("Loading users table into gold layer without PII data")
        with duckdb.connect(assets.GOLD_USERS.uri) as connection:
            attachment_name = "silver"
            connection.execute(
                f"ATTACH '{assets.SILVER_USERS.uri}' AS {attachment_name} (READ_ONLY)"
            )
            fully_quantified_source = f"{attachment_name}.{assets.SILVER_USERS.name}"
            result = connection.execute(
                f"CREATE OR REPLACE TABLE {assets.GOLD_USERS.name} AS {SELECT_USER_TABLE_WITHOUT_PII.replace(TABLE_NAME_PLACEHOLDER, fully_quantified_source)}"
            ).fetchone()
            if not result:
                raise RuntimeError("Failed to insert data")

            rows_inserted = result[0]
            logging.info(f"Inserted {rows_inserted} rows")

        logging.info("Done creating snapshot")
        yield Metadata(
            asset=assets.GOLD_USERS,
            extra={"rows_inserted": rows_inserted},
        )

    snapshot = reconstruct_snapshot()  # type: ignore
    users_without_pii = load_users_without_pii()
    snapshot >> users_without_pii


transform_and_load_users()
