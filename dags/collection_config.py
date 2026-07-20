"""
Default DAG parameter configuration for the new-style collection DAGs.

Most collections share the same generic defaults, but some need different values
because of the resources involved (e.g. title-boundary uses a smaller transform
batch size as its transform jobs are larger). Collection-specific overrides are
defined here rather than being hardcoded into the DAG generation logic.
"""

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


DEFAULT_COLLECTION_CONFIG = CollectionDagConfig()

# collections whose defaults diverge from DEFAULT_COLLECTION_CONFIG
COLLECTION_CONFIG_OVERRIDES = {
    "title-boundary": CollectionDagConfig(transform_batch_size=100),
}


def get_collection_dag_config(collection: str) -> CollectionDagConfig:
    """Return the default param config for a collection, falling back to the generic defaults."""
    return COLLECTION_CONFIG_OVERRIDES.get(collection, DEFAULT_COLLECTION_CONFIG)
