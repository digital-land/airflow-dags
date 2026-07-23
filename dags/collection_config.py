"""
Default DAG parameter configuration for the new-style collection DAGs.

Most collections share the same generic defaults, but some need different values
because of the resources involved (e.g. title-boundary uses a smaller transform
batch size as its transform jobs are larger). Collection-specific overrides are
defined here rather than being hardcoded into the DAG generation logic.
"""

from datetime import datetime, timedelta
from typing import Optional

from dateutil.rrule import rrulestr
from pydantic import BaseModel

# fixed anchor for expanding schedule_rrule recurrences from; only matters for cadences whose
# phase depends on a start date (e.g. fortnightly) - harmless for DAILY/MONTHLY;BYDAY rules
RRULE_SERIES_START = datetime(2025, 1, 1)


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


DEFAULT_COLLECTION_CONFIG = CollectionDagConfig()

# collections whose defaults diverge from DEFAULT_COLLECTION_CONFIG
COLLECTION_CONFIG_OVERRIDES = {
    # title-boundary's EMR job can otherwise consume all of the vCPU available to the
    # shared EMR Serverless application, starving other collections' jobs. It's also only
    # scheduled for the first Monday of the month, a day after HM Land Registry's own release
    # schedule for the INSPIRE Index Polygons data it's built from (first Sunday), so their
    # data is available by the time we run
    "title-boundary": CollectionDagConfig(transform_batch_size=100, max_executors=50, schedule_rrule="FREQ=MONTHLY;BYDAY=1MO"),
}


def get_collection_dag_config(collection: str) -> CollectionDagConfig:
    """Return the default param config for a collection, falling back to the generic defaults."""
    return COLLECTION_CONFIG_OVERRIDES.get(collection, DEFAULT_COLLECTION_CONFIG)


def collection_schedule_matches(collection: str, logical_date, **_) -> bool:
    """Whether a collection's schedule_rrule has an occurrence on logical_date's date."""
    rrule_str = get_collection_dag_config(collection).schedule_rrule
    occurrence = rrulestr(rrule_str, dtstart=RRULE_SERIES_START).after(logical_date - timedelta(seconds=1), inc=True)
    return occurrence is not None and occurrence.date() == logical_date.date()
