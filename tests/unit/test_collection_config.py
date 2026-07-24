import pendulum
import pytest

from dags.collection_config import DEFAULT_COLLECTION_CONFIG, collection_schedule_matches, get_collection_dag_config


def test_get_collection_dag_config_returns_default_for_unknown_collection():
    assert get_collection_dag_config("central-activities-zone") is DEFAULT_COLLECTION_CONFIG


def test_get_collection_dag_config_returns_override_for_title_boundary():
    config = get_collection_dag_config("title-boundary")
    assert config.schedule_rrule == "FREQ=MONTHLY;BYDAY=1MO"
    assert config.transform_batch_size == 100
    assert config.max_executors == 50


def test_default_collection_schedule_is_daily():
    assert DEFAULT_COLLECTION_CONFIG.schedule_rrule == "FREQ=DAILY"


def test_collection_schedule_matches_accepts_timezone_aware_logical_date():
    """Regression test: Airflow's logical_date is UTC-aware. dateutil raises "can't compare
    offset-naive and offset-aware datetimes" if the rrule's own anchor isn't aware too."""
    logical_date = pendulum.datetime(2026, 8, 3, tz="UTC")
    collection_schedule_matches("title-boundary", logical_date)  # should not raise


@pytest.mark.parametrize(
    "logical_date,expected",
    [
        (pendulum.datetime(2026, 8, 3, tz="UTC"), True),  # first Monday of August 2026
        (pendulum.datetime(2026, 8, 2, tz="UTC"), False),  # first Sunday - not a match
        (pendulum.datetime(2026, 8, 10, tz="UTC"), False),  # second Monday - not a match
        (pendulum.datetime(2026, 9, 7, tz="UTC"), True),  # first Monday of September 2026
    ],
)
def test_collection_schedule_matches_title_boundary_first_monday(logical_date, expected):
    assert collection_schedule_matches("title-boundary", logical_date) is expected


@pytest.mark.parametrize(
    "logical_date",
    [
        pendulum.datetime(2026, 8, 1, tz="UTC"),
        pendulum.datetime(2026, 8, 15, tz="UTC"),
        pendulum.datetime(2026, 12, 25, tz="UTC"),
    ],
)
def test_collection_schedule_matches_default_daily_collection_always_matches(logical_date):
    assert collection_schedule_matches("central-activities-zone", logical_date) is True
