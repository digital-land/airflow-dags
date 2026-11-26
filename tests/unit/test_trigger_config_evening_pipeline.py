import importlib
import sys
from datetime import timedelta
from unittest.mock import Mock

import pytest

MODULE = "dags.trigger_config_evening_pipeline"


@pytest.fixture
def load_dag_module(monkeypatch):
    """Import the DAG file afresh with the given environment, as Airflow would parse it there."""

    def load(env):
        monkeypatch.setattr("utils.get_config", lambda path=None: {"env": env})
        sys.modules.pop(MODULE, None)
        return importlib.import_module(MODULE)

    yield load
    sys.modules.pop(MODULE, None)


def test_trigger_evening_pipeline_starts_the_workflow_without_inputs(
    load_dag_module, monkeypatch
):
    module = load_dag_module("production")
    trigger_workflow = Mock()
    monkeypatch.setattr(module, "trigger_workflow", trigger_workflow)

    module.trigger_evening_pipeline()

    trigger_workflow.assert_called_once_with(
        "production",
        repository="digital-land/config",
        workflow="config-evening-pipeline.yml",
    )


def test_dag_runs_at_23_00_on_weekdays_in_production(load_dag_module):
    dag = load_dag_module("production").dag
    task = dag.get_task("trigger-evening-pipeline")

    assert dag.schedule_interval == "0 23 * * 1-5"
    assert dag.catchup is False
    assert task.retries == 2
    assert task.retry_delay == timedelta(minutes=5)


@pytest.mark.parametrize("env", ["development", "staging"])
def test_dag_is_not_created_outside_production(load_dag_module, env):
    assert not hasattr(load_dag_module(env), "dag")
