"""
Default DAG parameter configuration for the new-style collection DAGs.

Most collections share the same generic defaults, but some need different values
because of the resources involved (e.g. title-boundary uses a smaller transform
batch size as its transform jobs are larger). Collection-specific overrides are
defined here rather than being hardcoded into the DAG generation logic.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from dateutil.rrule import rrulestr
from pydantic import BaseModel

# fixed anchor for expanding schedule_rrule recurrences from; only matters for cadences whose
# phase depends on a start date (e.g. fortnightly) - harmless for DAILY/MONTHLY;BYDAY rules.
# Must be timezone-aware since Airflow's logical_date is UTC-aware, and dateutil can't compare
# naive and aware datetimes.
RRULE_SERIES_START = datetime(2025, 1, 1, tzinfo=timezone.utc)


class CollectionDagConfig(BaseModel):
    cpu: int = 8192
    memory: int = 32768
    transformed_jobs: int = 8
    dataset_jobs: int = 8
    transform_batch_size: int = 200
    incremental_loading_override: bool = False
    regenerate_log_override: bool = False
    force_reprocessing: bool = False
    # caps spark.dynamicAllocation.maxExecutors on the assemble EMR Serverless job; None leaves
    # EMR Serverless's own default in place
    max_executors: Optional[int] = None
    # how often (in seconds) the ECS task operators poll CloudWatch for new log lines; too low a
    # value puts workers under load when many ECS tasks are triggered at once
    awslogs_fetch_interval_seconds: int = 10
    # RFC 5545 recurrence rule (RRULE) controlling which of the scheduler's daily runs should
    # actually trigger this collection; "FREQ=DAILY" (the default) means every run
    schedule_rrule: str = "FREQ=DAILY"
    # wall-clock budget for the assemble EMR Serverless job. Both bounds on the operator derive
    # from this - see assemble_waiter_max_attempts below for why they must not be equal.
    assemble_timeout: timedelta = timedelta(hours=3)


DEFAULT_COLLECTION_CONFIG = CollectionDagConfig()

# collections whose defaults diverge from DEFAULT_COLLECTION_CONFIG
COLLECTION_CONFIG_OVERRIDES = {
    # title-boundary's EMR job can otherwise consume all of the vCPU available to the
    # shared EMR Serverless application, starving other collections' jobs. It's also only
    # scheduled monthly, the day after HM Land Registry's own release schedule for the
    # INSPIRE Index Polygons data it's built from (first Sunday), so their data is available
    # by the time we run.
    # BYMONTHDAY=2..8 selects the Monday that follows the first Sunday.
    "title-boundary": CollectionDagConfig(
        transform_batch_size=50,
        max_executors=50,
        schedule_rrule="FREQ=MONTHLY;BYDAY=MO;BYMONTHDAY=2,3,4,5,6,7,8",
        # raised from the default three hours, which cancelled the 2026-09-07 run mid-flight.
        # Safe to be generous now that nothing waits on this collection - see dag_triggers.py.
        assemble_timeout=timedelta(hours=12),
    ),
}


# how often the EMR operator polls for job completion
ASSEMBLE_WAITER_DELAY_SECONDS = 60


def assemble_waiter_max_attempts(assemble_timeout: timedelta) -> int:
    """Poll count giving the waiter a strictly larger budget than assemble_timeout.

    The EMR operator has two independent bounds and they are not interchangeable. Exceeding
    execution_timeout raises AirflowTaskTimeout, which Airflow answers by calling the operator's
    on_kill(), which calls cancel_job_run() - the EMR job actually stops. Exhausting the waiter
    raises a plain exception with no on_kill(), so the job would be left running on the shared
    EMR Serverless application with nothing tracking it.

    execution_timeout must therefore always fire first. Today the two are both three hours and
    execution_timeout wins only because its clock starts at task start while the waiter's starts
    after start_job_run returns - a few seconds of accident. Adding one poll interval makes the
    ordering explicit instead.
    """
    return int(assemble_timeout.total_seconds() // ASSEMBLE_WAITER_DELAY_SECONDS) + 1


def get_collection_dag_config(collection: str) -> CollectionDagConfig:
    """Return the default param config for a collection, falling back to the generic defaults."""
    return COLLECTION_CONFIG_OVERRIDES.get(collection, DEFAULT_COLLECTION_CONFIG)


def collection_schedule_matches(collection: str, data_interval_end, **_) -> bool:
    """Whether a collection's schedule_rrule has an occurrence on data_interval_end's date.

    data_interval_end - not logical_date - is used deliberately: for a daily schedule,
    logical_date is the *start* of the interval (i.e. the previous day relative to when the run
    actually fires), while data_interval_end is the day the run corresponds to.
    """
    rrule_str = get_collection_dag_config(collection).schedule_rrule
    occurrence = rrulestr(rrule_str, dtstart=RRULE_SERIES_START).after(data_interval_end - timedelta(seconds=1), inc=True)
    return occurrence is not None and occurrence.date() == data_interval_end.date()
