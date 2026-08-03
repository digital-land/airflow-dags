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


def test_collection_schedule_matches_accepts_timezone_aware_data_interval_end():
    """Regression test: Airflow's data_interval_end is UTC-aware. dateutil raises "can't compare
    offset-naive and offset-aware datetimes" if the rrule's own anchor isn't aware too."""
    data_interval_end = pendulum.datetime(2026, 8, 3, tz="UTC")
    collection_schedule_matches("title-boundary", data_interval_end)  # should not raise


def test_collection_schedule_matches_uses_data_interval_end_not_logical_date():
    """Regression test: Airflow's context includes both logical_date (the *start* of a run's
    data interval - the previous day, for a run firing just after midnight) and
    data_interval_end (the day the run is actually for). Because collection_schedule_matches
    takes **kwargs, Airflow passes the whole context through regardless of which name the
    function's parameter uses - so a call passing both, with values that disagree, is the only
    way to prove the right one is actually being read rather than just "a" date."""
    result = collection_schedule_matches(
        "title-boundary",
        logical_date=pendulum.datetime(2026, 8, 2, tz="UTC"),  # Sunday - would not match
        data_interval_end=pendulum.datetime(2026, 8, 3, tz="UTC"),  # Monday - first Monday, matches
    )
    assert result is True


@pytest.mark.parametrize(
    "data_interval_end,expected",
    [
        (pendulum.datetime(2026, 8, 3, tz="UTC"), True),  # first Monday of August 2026
        (pendulum.datetime(2026, 8, 2, tz="UTC"), False),  # first Sunday - not a match
        (pendulum.datetime(2026, 8, 10, tz="UTC"), False),  # second Monday - not a match
        (pendulum.datetime(2026, 9, 7, tz="UTC"), True),  # first Monday of September 2026
    ],
)
def test_collection_schedule_matches_title_boundary_first_monday(data_interval_end, expected):
    assert collection_schedule_matches("title-boundary", data_interval_end) is expected


@pytest.mark.parametrize(
    "data_interval_end",
    [
        pendulum.datetime(2026, 8, 1, tz="UTC"),
        pendulum.datetime(2026, 8, 15, tz="UTC"),
        pendulum.datetime(2026, 12, 25, tz="UTC"),
    ],
)
def test_collection_schedule_matches_default_daily_collection_always_matches(data_interval_end):
    assert collection_schedule_matches("central-activities-zone", data_interval_end) is True
