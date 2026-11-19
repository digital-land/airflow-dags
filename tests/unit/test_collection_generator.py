#from unittest.mock import MagicMock

import pytest

import dags.collection_generator

from unittest.mock import Mock, call

from dags.collection_config import get_collection_dag_config


def test_branch_params_default_to_main():
    dag = dags.collection_generator.dag

    assert dag.params["digital-land-python-branch"] == "main"
    assert dag.params["makerules-branch"]== "main"

def test_dag_has_branch_params():
    dag = dags.collection_generator.dag

    assert "digital-land-python-branch" in dag.params
    assert "makerules-branch" in dag.params

    assert dag.params["digital-land-python-branch"]== "main"
    assert dag.params["makerules-branch"] == "main"


def test_all_collection_dags_are_generated():
    assert dags.collection_generator.dag

from unittest.mock import Mock, call


def get_collection_params(collection):
    config = get_collection_dag_config(collection)

    return {
        "cpu": config.cpu,
        "memory": config.memory,
        "transformed-jobs": config.transformed_jobs,
        "dataset-jobs": config.dataset_jobs,
        "transform-batch-size": config.transform_batch_size,
        "incremental-loading-override": config.incremental_loading_override,
        "regenerate-log-override": config.regenerate_log_override,
        "force-reprocessing": config.force_reprocessing,
    }

def test_configure_dag_uses_custom_branches():
    ti = Mock()

    params = get_collection_params("ancient-woodland")
    params.update({
        "digital-land-python-branch": "feature/test-branch",
        "makerules-branch": "feature/test-makerules",
    })

    # Call configure_dag using the existing test context.

    assert call(
        key="digital-land-python-ref",
        value="feature/test-branch",
    ) in ti.xcom_push.call_args_list