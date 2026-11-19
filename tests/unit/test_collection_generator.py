#from unittest.mock import MagicMock

import pytest

import dags.collection_generator

from unittest.mock import Mock, call

from dags.collection_config import get_collection_dag_config


def test_branch_params_default_to_main():
    dag = dags.collection_generator.dag

    assert dag.params["digital-land-python-branch"] == "main"
    assert dag.params["makerules-branch"] == "main"

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
    conf = Mock()

    conf.get.side_effect = [
        "test-collection-bucket",
        "test-tiles-bucket",
    ]

    params = get_collection_params("ancient-woodland")
    params.update({
        "digital-land-python-branch": "feature/test-branch",
        "makerules-branch": "feature/test-makerules",
    })

    with patch.object(
        dags.collection_generator,
        "push_log_variables",
    ), patch.object(
        dags.collection_generator,
        "push_vpc_config",
    ):
        dags.collection_generator.configure_dag(
            ti=ti,
            params=params,
            conf=conf,
        )

    ti.xcom_push.assert_any_call(
        key="digital-land-python-ref",
        value="feature/test-branch",
    )

    ti.xcom_push.assert_any_call(
        key="makerules-url",
        value="https://raw.githubusercontent.com/digital-land/makerules/feature/test-makerules/",
    )




import importlib
from unittest.mock import patch

import dags.collection_generator


def test_branch_params_not_available_in_production():
    #with patch(
    #    {"env": "production"},
    #    "dags.collection_generator.config",
    #):
    with patch(
        "dags.collection_generator.get_config",
        return_value={"env": "production"},
    ):
        importlib.reload(dags.collection_generator)

    dag = dags.collection_generator.dag

    assert "digital-land-python-branch" not in dag.params
    assert "makerules-branch" not in dag.params