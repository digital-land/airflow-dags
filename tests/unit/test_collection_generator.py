import importlib
import os
from unittest.mock import Mock, patch

import utils

import dags.collection_generator
from dags.collection_config import get_collection_dag_config


def test_branch_params_default_to_main():
    dag = dags.collection_generator.dag

    assert "digital-land-python-branch" in dag.params
    assert "makerules-branch" in dag.params

    assert dag.params["digital-land-python-branch"] == "main"
    assert dag.params["makerules-branch"] == "main"


def test_all_collection_dags_are_generated():
    assert dags.collection_generator.dag


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
    params.update(
        {
            "digital-land-python-branch": "feature/test-branch",
            "makerules-branch": "feature/test-makerules",
        }
    )

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


def test_branch_params_not_available_in_production1():
    with patch.object(
        dags.collection_generator,
        "config",
        {"env": "production"},
    ):
        # Recreate the params using the production config.
        params = get_collection_params("ancient-woodland")

        print(dags.collection_generator.get_config())
        print(dags.collection_generator.params)

        assert "digital-land-python-branch" not in params
        assert "makerules-branch" not in params


BRANCH_PARAMS = {"digital-land-python-branch", "makerules-branch"}


def _dags_for_env(env):
    """Rebuild the collection DAGs as `env` would, and return them."""
    config = {**utils.get_config(), "env": env}
    # The DAG module reads the EMR execution role secret at import time; conftest only fakes development's
    secret = {
        f"SECRET__{env.upper()}_PD_BATCH_DEPLOYMENT_VARIABLES_SECRET": os.environ[
            "SECRET__DEVELOPMENT_PD_BATCH_DEPLOYMENT_VARIABLES_SECRET"
        ]
    }
    try:
        with patch("utils.get_config", return_value=config), patch.dict(
            os.environ, secret
        ):
            module = importlib.reload(dags.collection_generator)
            return [
                obj for obj in vars(module).values() if obj.__class__.__name__ == "DAG"
            ]
    finally:
        importlib.reload(dags.collection_generator)


def test_branch_params_only_outside_production():
    assert all(BRANCH_PARAMS <= set(dag.params) for dag in _dags_for_env("development"))
    production = _dags_for_env("production")
    assert production
    assert not any(BRANCH_PARAMS & set(dag.params) for dag in production)
