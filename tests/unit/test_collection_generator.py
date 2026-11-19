#from unittest.mock import MagicMock

import pytest

import dags.collection_generator


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


def test_configure_dag_uses_custom_branches():
    ti = Mock()

    params = {
        "cpu": 1024,
        "memory": 2048,
        "transformed-jobs": 1,
        "dataset-jobs": 1,
        "transform-batch-size": 100,
        "incremental-loading-override": False,
        "regenerate-log-override": False,
        "force-reprocessing": False,
        "digital-land-python-branch": "feature/test-branch",
        "makerules-branch": "feature/test-makerules",
    }

    dags.collection_generator.configure_dag(
        ti=ti,
        **{"params": params},
        #params=params,
    )

    assert call(
        key="digital-land-python-ref",
        value="feature/test-branch",
    ) in ti.xcom_push.call_args_list

    assert call(
        key="makerules-url",
        value="https://raw.githubusercontent.com/digital-land/makerules/feature/test-makerules/",
    ) in ti.xcom_push.call_args_list