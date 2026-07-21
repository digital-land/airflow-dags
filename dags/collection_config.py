"""
Default DAG parameter configuration for the new-style collection DAGs.

Most collections share the same generic defaults, but some need different values
because of the resources involved (e.g. title-boundary uses a smaller transform
batch size as its transform jobs are larger). Collection-specific overrides are
defined here rather than being hardcoded into the DAG generation logic.
"""

from typing import Optional

from pydantic import BaseModel


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


DEFAULT_COLLECTION_CONFIG = CollectionDagConfig()

# collections whose defaults diverge from DEFAULT_COLLECTION_CONFIG
COLLECTION_CONFIG_OVERRIDES = {
    # title-boundary's EMR job can otherwise consume all of the vCPU available to the
    # shared EMR Serverless application, starving other collections' jobs
    "title-boundary": CollectionDagConfig(transform_batch_size=100, max_executors=50),
}


def get_collection_dag_config(collection: str) -> CollectionDagConfig:
    """Return the default param config for a collection, falling back to the generic defaults."""
    return COLLECTION_CONFIG_OVERRIDES.get(collection, DEFAULT_COLLECTION_CONFIG)
