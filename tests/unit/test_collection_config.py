from datetime import timedelta

import pendulum
import pytest

from dags.collection_config import (
    ASSEMBLE_WAITER_DELAY_SECONDS,
    COLLECTION_CONFIG_OVERRIDES,
    DEFAULT_COLLECTION_CONFIG,
    assemble_waiter_max_attempts,
    collection_schedule_matches,
    get_collection_dag_config,
)


def test_get_collection_dag_config_returns_default_for_unknown_collection():
    assert get_collection_dag_config("central-activities-zone") is DEFAULT_COLLECTION_CONFIG


def test_get_collection_dag_config_returns_override_for_title_boundary():
    config = get_collection_dag_config("title-boundary")
    assert config.schedule_rrule == "FREQ=MONTHLY;BYDAY=MO;BYMONTHDAY=2,3,4,5,6,7,8"
    assert config.transform_batch_size == 50
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
        data_interval_end=pendulum.datetime(2026, 8, 3, tz="UTC"),  # Monday after the first Sunday, matches
    )
    assert result is True


@pytest.mark.parametrize(
    "data_interval_end,expected",
    [
        (pendulum.datetime(2026, 8, 3, tz="UTC"), True),  # Monday after the first Sunday (2 Aug)
        (pendulum.datetime(2026, 8, 2, tz="UTC"), False),  # the release Sunday itself - not a match
        (pendulum.datetime(2026, 8, 10, tz="UTC"), False),  # a week late - not a match
        (pendulum.datetime(2026, 9, 7, tz="UTC"), True),  # Monday after the first Sunday (6 Sep)
    ],
)
def test_collection_schedule_matches_title_boundary_monday_after_first_sunday(data_interval_end, expected):
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


def _first_sunday(year, month):
    """HM Land Registry publish the INSPIRE Index Polygons on the first Sunday of the month."""
    for day in range(1, 8):
        candidate = pendulum.datetime(year, month, day, tz="UTC")
        if candidate.weekday() == 6:  # Monday is 0, Sunday is 6
            return candidate
    raise AssertionError("every seven-day window contains a Sunday")


def test_title_boundary_runs_once_a_month_the_day_after_the_hmlr_release():
    """The schedule must land on the Monday immediately after HMLR's first-Sunday release - the
    deliberate one-day buffer recorded in collection_config.

    "FREQ=MONTHLY;BYDAY=1MO" (the first Monday) is not that rule. When the 1st of the month falls
    on a Monday it precedes the first Sunday by six days, so we collect the previous month's
    release and do not run again until the month after. It fails silently, because the pipeline
    succeeds identically whether or not there is new data. Across these 30 months the first-Monday
    rule breaks five times - 2026-06, 2027-02, 2027-03, 2027-11 and 2028-05 - and 2026-06 already
    happened: we ran on 1 June, HMLR published on 7 June, and that release was not collected until
    6 July, so published boundaries were a month stale for five weeks.

    Asserting every matching day in the month, rather than probing individual dates, also pins that
    the rule fires exactly once a month - the existing cases above all sit in months where both
    rules agree, which is why they never caught this.
    """
    start = pendulum.datetime(2026, 1, 1, tz="UTC")
    for offset in range(30):
        month_start = start.add(months=offset)
        expected = _first_sunday(month_start.year, month_start.month).add(days=1)
        matching = [day for day in (month_start.add(days=i) for i in range(month_start.days_in_month)) if collection_schedule_matches("title-boundary", day)]
        assert matching == [expected], f"{month_start:%Y-%m}: expected {expected:%a %d %b}, got {[format(d, '%a %d %b') for d in matching]}"


def test_default_assemble_timeout_is_three_hours():
    assert DEFAULT_COLLECTION_CONFIG.assemble_timeout == timedelta(hours=3)


def test_title_boundary_assemble_timeout_is_raised_above_the_default():
    """The 2026-09-07 production run was cancelled at the default three hours having not finished.
    Raising it is only safe because nothing waits on this collection any more - see dag_triggers."""
    assert get_collection_dag_config("title-boundary").assemble_timeout == timedelta(hours=12)


@pytest.mark.parametrize("collection", ["central-activities-zone", *COLLECTION_CONFIG_OVERRIDES])
def test_waiter_budget_always_outlasts_execution_timeout(collection):
    """The assemble EMR operator's two bounds are not interchangeable and execution_timeout must
    always be the one that fires.

    Exceeding execution_timeout raises AirflowTaskTimeout, which Airflow answers by calling the
    operator's on_kill(); EmrServerlessStartJobOperator.on_kill() calls cancel_job_run(), so the
    EMR job actually stops. Exhausting the waiter raises a plain exception with no on_kill() - the
    task would fail while the job carried on running on the shared EMR Serverless application with
    nothing left tracking it.
    """
    config = get_collection_dag_config(collection)
    waiter_budget_seconds = assemble_waiter_max_attempts(config.assemble_timeout) * ASSEMBLE_WAITER_DELAY_SECONDS

    assert waiter_budget_seconds > config.assemble_timeout.total_seconds()


@pytest.mark.parametrize(
    "assemble_timeout",
    [
        timedelta(hours=3),
        timedelta(hours=12),
        timedelta(minutes=90),
        timedelta(minutes=90, seconds=30),  # not a whole number of poll intervals
        timedelta(seconds=1),  # shorter than a single poll interval
    ],
)
def test_assemble_waiter_max_attempts_always_exceeds_its_input(assemble_timeout):
    """Flooring to whole poll intervals and adding one has to stay strictly above the input for
    every timeout, not just the ones that divide evenly."""
    assert assemble_waiter_max_attempts(assemble_timeout) * ASSEMBLE_WAITER_DELAY_SECONDS > assemble_timeout.total_seconds()
