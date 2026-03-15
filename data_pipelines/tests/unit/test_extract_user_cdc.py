from pendulum import DateTime

from data_pipelines import DATALAKE_BRONZE_LOCATION
from data_pipelines.extract_users_cdc import extract_user_cdc


def test_return_one_cdc_user_path():
    dag = extract_user_cdc()
    fn = dag.task_dict["resolve_cdc_users_path"].python_callable

    actual = fn(DateTime(2026, 1, 1, 13, 37, 42)).replace(
        str(DATALAKE_BRONZE_LOCATION), ""
    )
    expected = "/users/2026/01/01/13/37/events-*.jsonl"

    assert expected == actual


def test_return_another_cdc_user_path():
    dag = extract_user_cdc()
    fn = dag.task_dict["resolve_cdc_users_path"].python_callable

    actual = fn(DateTime(2026, 12, 13, 6, 1, 24)).replace(
        str(DATALAKE_BRONZE_LOCATION), ""
    )
    expected = "/users/2026/12/13/06/01/events-*.jsonl"

    assert expected == actual
