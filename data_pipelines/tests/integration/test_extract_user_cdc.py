from datetime import datetime
from pathlib import Path

import duckdb
from airflow.sdk import Metadata, AssetAlias

from airflow import Asset
from data_pipelines.extract_users_cdc import extract_user_cdc
from data_pipelines.queries import CREATE_USER_CDC_TABLE, TABLE_NAME_PLACEHOLDER


def test_insert_data(mocker):
    test_users_path = Path(__file__).parent / f"{datetime.now()}test_users.db"
    table_name = "test_user_cdc"
    asset_mock = Asset(name=table_name, uri=f"duckdb://{test_users_path}")
    mocker.patch("data_pipelines.assets.BRONZE_USERS_CDC", asset_mock)
    asset_alias_mock = AssetAlias(name="asset_alias")
    mocker.patch("data_pipelines.assets.BRONZE_USERS_CDC_ALIAS", asset_alias_mock)
    dag = extract_user_cdc()
    fn = dag.task_dict["ingest_users_cdc"].python_callable
    ingestion_path = Path(__file__).parent / "*.jsonl"

    actual = next(fn(ingestion_path))
    expected = Metadata(
        asset=asset_mock,
        extra={"rows_inserted": 1, "glob": ingestion_path},
        alias=asset_alias_mock,
    )
    expected_data = [
        (
            "0d6abef6-01a0-446e-9dc9-96be5e678c41",
            datetime(2026, 2, 5, 17, 1, 41, 10848),
            datetime(2026, 2, 5, 16, 32, 0, 10848),
            "users",
            "mongodb-change-stream",
            "mongodb-test-stream",
            "[1770305542,1337]",
            '{"database":"app","collection":"users","primary_keys":["_id"],"change_type":"DELETE","is_deleted":true,"cluster_time":{"t":1770305520,"i":27478},"txn_number":null,"lsid":null}',
            '{"_id":"0283cba53a90492da75e4995"}',
            None,
            '{"uuid":"0d6abef6-01a0-446e-9dc9-96be5e678c41","read_timestamp":"2026-02-05T17:01:41.010848Z","source_timestamp":"2026-02-05T16:32:00.010848Z","object":"users","read_method":"mongodb-change-stream","stream_name":"mongodb-test-stream","sort_keys":[1770305542,1337],"source_metadata":{"database":"app","collection":"users","primary_keys":["_id"],"change_type":"DELETE","is_deleted":true,"cluster_time":{"t":1770305520,"i":27478},"txn_number":null,"lsid":null},"payload":{"_id":"0283cba53a90492da75e4995"}}',
        )
    ]

    with duckdb.connect(asset_mock.uri) as connection:
        actual_data = connection.execute(f"SELECT * FROM {table_name}").fetchall()
    test_users_path.unlink(missing_ok=True)

    assert expected == actual
    assert expected_data == actual_data


def test_insert_data_into_existing_table(mocker):
    test_users_path = Path(__file__).parent / f"{datetime.now()}test_users.db"
    table_name = "test_user_cdc"
    asset_mock = Asset(name=table_name, uri=f"duckdb://{test_users_path}")
    mocker.patch("data_pipelines.assets.BRONZE_USERS_CDC", asset_mock)
    asset_alias_mock = AssetAlias(name="asset_alias")
    mocker.patch("data_pipelines.assets.BRONZE_USERS_CDC_ALIAS", asset_alias_mock)
    dag = extract_user_cdc()
    fn = dag.task_dict["ingest_users_cdc"].python_callable
    ingestion_path = Path(__file__).parent / "*.jsonl"
    with duckdb.connect(asset_mock.uri) as connection:
        connection.sql(
            CREATE_USER_CDC_TABLE.replace(TABLE_NAME_PLACEHOLDER, table_name)
        )
        connection.sql(f"""
        INSERT INTO {table_name}
                (uuid, read_ts, source_ts, object, read_method, stream_name, sort_keys, source_metadata, payload, update_description, raw_json)
            VALUES
                ('foobar', now(), now(), '', 'read method', 'stream_name', '[1,1]', '{{}}', '{{}}', NULL, '{{}}')
        """)

    actual = next(fn(ingestion_path))
    expected = Metadata(
        asset=asset_mock,
        extra={"rows_inserted": 1, "glob": ingestion_path},
        alias=asset_alias_mock,
    )
    expected_data_rows = 2
    expected_last_insert = (
        "0d6abef6-01a0-446e-9dc9-96be5e678c41",
        datetime(2026, 2, 5, 17, 1, 41, 10848),
        datetime(2026, 2, 5, 16, 32, 0, 10848),
        "users",
        "mongodb-change-stream",
        "mongodb-test-stream",
        "[1770305542,1337]",
        '{"database":"app","collection":"users","primary_keys":["_id"],"change_type":"DELETE","is_deleted":true,"cluster_time":{"t":1770305520,"i":27478},"txn_number":null,"lsid":null}',
        '{"_id":"0283cba53a90492da75e4995"}',
        None,
        '{"uuid":"0d6abef6-01a0-446e-9dc9-96be5e678c41","read_timestamp":"2026-02-05T17:01:41.010848Z","source_timestamp":"2026-02-05T16:32:00.010848Z","object":"users","read_method":"mongodb-change-stream","stream_name":"mongodb-test-stream","sort_keys":[1770305542,1337],"source_metadata":{"database":"app","collection":"users","primary_keys":["_id"],"change_type":"DELETE","is_deleted":true,"cluster_time":{"t":1770305520,"i":27478},"txn_number":null,"lsid":null},"payload":{"_id":"0283cba53a90492da75e4995"}}',
    )

    with duckdb.connect(asset_mock.uri) as connection:
        actual_data = connection.execute(f"SELECT * FROM {table_name}").fetchall()
    test_users_path.unlink(missing_ok=True)

    assert expected == actual
    assert expected_data_rows == len(actual_data)
    assert expected_last_insert == actual_data[1]
