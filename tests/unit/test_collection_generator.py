import dags.collection_generator
import pytest

def get_dependency_branch_values(params):
    dlp_branch = params.get("digital-land-python-branch", "main")
    makerules_branch = params.get("makerules-branch", "main")

    return {
        "digital-land-python-ref": (
            dlp_branch if dlp_branch != "main" else ""
        ),
        "makerules-url": (
            f"https://raw.githubusercontent.com/digital-land/makerules/{makerules_branch}/"
            if makerules_branch != "main"
            else ""
        ),
    }

def test_dependency_branches_default_to_main():
    result = get_dependency_branch_values({})

    assert result["digital-land-python-ref"] == ""
    assert result["makerules-url"] == ""


def test_dependency_branches_use_selected_branches():
    result = get_dependency_branch_values({
        "digital-land-python-branch": "feature/dlp",
        "makerules-branch": "feature/makerules",
    })

    assert result["digital-land-python-ref"] == "feature/dlp"

    assert result["makerules-url"] == (
        "https://raw.githubusercontent.com/"
        "digital-land/makerules/feature/makerules/"
    )

def test_configure_dag_pushes_branch_xcoms(mocker):
    dag = dags.collection_generator.dag
    configure_task = dag.get_task("configure-dag")

    ti = mocker.MagicMock()

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

    mocker.patch.object(
        collection_generator,
        "push_log_variables",
    )
    mocker.patch.object(
        collection_generator,
        "push_vpc_config",
    )

    configure_task.python_callable(
        ti=ti,
        params=params,
    )

    pushed_values = {
        call.kwargs["key"]: call.kwargs["value"]
        for call in ti.xcom_push.call_args_list
    }

    assert pushed_values["digital-land-python-ref"] == "feature/test"

    assert pushed_values["makerules-url"] == (
        "https://raw.githubusercontent.com/"
        "digital-land/makerules/feature/rules/"
    )