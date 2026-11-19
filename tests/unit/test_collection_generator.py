from unittest.mock import MagicMock

import pytest

import dags.collection_generator


def test_branch_params_default_to_main():
    dag = dags.collection_generator.dag

    assert dag.params["digital-land-python-branch"].value == "main"
    assert dag.params["makerules-branch"].value == "main"


def test_all_collection_dags_are_generated():
    assert dags.collection_generator.dag


def test_configure_dag_pushes_branch_xcoms(mocker):
    dag = dags.collection_generator.dag
    configure_task = dag.get_task("configure-dag")

    ti = mocker.MagicMock()

    mocker.patch.object(
        dags.collection_generator,
        "push_log_variables",
    )
    mocker.patch.object(
        dags.collection_generator,
        "push_vpc_config",
    )

    params = {
        "memory": 2048,
        "cpu": 1024,
        "transformed-jobs": 1,
        "dataset-jobs": 1,
        "transform-batch-size": 100,
        "incremental-loading-override": False,
        "regenerate-log-override": False,
        "force-reprocessing": False,
        "digital-land-python-branch": "feature/test",
        "makerules-branch": "feature/rules",
    }

    configure_task.python_callable(
        ti=ti,
        params=params,
        conf=mocker.MagicMock(),
    )

    pushed_values = {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}

    assert pushed_values["digital-land-python-ref"] == "feature/test"

    assert pushed_values["makerules-url"] == ("https://raw.githubusercontent.com/" "digital-land/makerules/feature/rules/")


def test_configure_dag_defaults_to_main(mocker):
    dag = dags.collection_generator.dag
    configure_task = dag.get_task("configure-dag")

    ti = mocker.MagicMock()

    mocker.patch.object(
        dags.collection_generator,
        "push_log_variables",
    )
    mocker.patch.object(
        dags.collection_generator,
        "push_vpc_config",
    )

    params = {
        "memory": 2048,
        "cpu": 1024,
        "transformed-jobs": 1,
        "dataset-jobs": 1,
        "transform-batch-size": 100,
        "incremental-loading-override": False,
        "regenerate-log-override": False,
        "force-reprocessing": False,
    }

    configure_task.python_callable(
        ti=ti,
        params=params,
        conf=mocker.MagicMock(),
    )

    pushed_values = {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}

    assert pushed_values["digital-land-python-ref"] == ""
    assert pushed_values["makerules-url"] == ""


@pytest.mark.parametrize(
    "task_suffix",
    ["collect", "package"],
)
def test_collection_tasks_use_branch_xcoms(task_suffix):
    dag = dags.collection_generator.dag

    task = next(task for task in dag.tasks if task.task_id.endswith(task_suffix))

    environment = task.overrides["containerOverrides"][0]["environment"]

    environment_values = {item["name"]: item["value"] for item in environment}

    assert environment_values["DIGITAL_LAND_PYTHON_REF"] == ('\'{{ task_instance.xcom_pull(task_ids="configure-dag", ' 'key="digital-land-python-ref") }}\'')

    assert environment_values["MAKERULES_URL"] == ('\'{{ task_instance.xcom_pull(task_ids="configure-dag", ' 'key="makerules-url") }}\'')


def test_dag_has_branch_params():
    dag = dags.collection_generator.dag

    assert "digital-land-python-branch" in dag.params
    assert "makerules-branch" in dag.params

    assert dag.params["digital-land-python-branch"].value == "main"
    assert dag.params["makerules-branch"].value == "main"


def test_configure_dag_pushes_branch_xcoms(mocker):
    dag = dags.collection_generator.dag
    configure_task = dag.get_task("configure-dag")

    ti = mocker.MagicMock()

    mocker.patch.object(
        dags.collection_generator,
        "push_log_variables",
    )
    mocker.patch.object(
        dags.collection_generator,
        "push_vpc_config",
    )

    params = {
        "memory": 2048,
        "cpu": 1024,
        "transformed-jobs": 1,
        "dataset-jobs": 1,
        "transform-batch-size": 100,
        "incremental-loading-override": False,
        "regenerate-log-override": False,
        "force-reprocessing": False,
        "digital-land-python-branch": "feature/test",
        "makerules-branch": "feature/rules",
    }

    conf = mocker.MagicMock()

    configure_task.python_callable(
        ti=ti,
        params=params,
        conf=conf,
    )

    pushed_values = {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}

    assert pushed_values["digital-land-python-ref"] == "feature/test"

    assert pushed_values["makerules-url"] == ("https://raw.githubusercontent.com/" "digital-land/makerules/feature/rules/")


def test_configure_dag_defaults_to_main(mocker):
    dag = dags.collection_generator.dag
    configure_task = dag.get_task("configure-dag")

    ti = mocker.MagicMock()

    mocker.patch.object(
        dags.collection_generator,
        "push_log_variables",
    )
    mocker.patch.object(
        dags.collection_generator,
        "push_vpc_config",
    )

    params = {
        "memory": 2048,
        "cpu": 1024,
        "transformed-jobs": 1,
        "dataset-jobs": 1,
        "transform-batch-size": 100,
        "incremental-loading-override": False,
        "regenerate-log-override": False,
        "force-reprocessing": False,
    }

    configure_task.python_callable(
        ti=ti,
        params=params,
        conf=mocker.MagicMock(),
    )

    pushed_values = {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}

    assert pushed_values["digital-land-python-ref"] == ""
    assert pushed_values["makerules-url"] == ""


def test_collect_task_has_branch_environment_variables():
    dag = dags.collection_generator.dag
    task = dag.get_task("ancient-woodland-collect")

    environment = task.overrides["containerOverrides"][0]["environment"]

    environment_names = [item["name"] for item in environment]

    assert "DIGITAL_LAND_PYTHON_REF" in environment_names
    assert "MAKERULES_URL" in environment_names


def test_package_task_has_branch_environment_variables():
    dag = dags.collection_generator.dag
    task = dag.get_task("ancient-woodland-package")

    environment = task.overrides["containerOverrides"][0]["environment"]

    environment_names = [item["name"] for item in environment]

    assert "DIGITAL_LAND_PYTHON_REF" in environment_names
    assert "MAKERULES_URL" in environment_names


def test_production_dag_does_not_have_branch_params(mocker):
    import importlib

    import utils

    mocker.patch(
        "utils.get_config",
        return_value={"env": "production"},
    )

    importlib.reload(dags.collection_generator)

    dag = dags.collection_generator.dag

    assert "digital-land-python-branch" not in dag.params
    assert "makerules-branch" not in dag.params
