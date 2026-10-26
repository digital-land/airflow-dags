from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest
from airflow.exceptions import AirflowSkipException
from botocore.exceptions import ClientError, WaiterError

from dags.digital_land_builder import config, dag


def get_task_callable(task_id):
    return dag.get_task(task_id).python_callable


def make_conf(digital_land_cloudfront_distribution_ids):
    conf = Mock()
    conf.get = Mock(return_value=digital_land_cloudfront_distribution_ids)
    return conf


def make_cloudfront_client():
    """A CloudFront client whose create_invalidation returns a new invalidation ID on each call."""
    invalidation_ids = iter(f"I{number}" for number in range(1, 100))
    create_invalidation = Mock(side_effect=lambda **kwargs: {"Invalidation": {"Id": next(invalidation_ids)}})
    waiter = Mock()
    return Mock(create_invalidation=create_invalidation, get_waiter=Mock(return_value=waiter)), waiter


def test_invalidate_cloudfront_cache_invalidates_each_distribution_twice(monkeypatch):
    cloudfront_client, _ = make_cloudfront_client()
    boto3_client = Mock(return_value=cloudfront_client)
    monkeypatch.setattr("dags.digital_land_builder.boto3.client", boto3_client)

    invalidate_cloudfront_cache = get_task_callable("invalidate-cloudfront-cache")
    invalidate_cloudfront_cache(
        dag_run=SimpleNamespace(run_id="test-run"),
        conf=make_conf("E1234567890ABC,E0987654321XYZ"),
    )

    boto3_client.assert_called_once_with("cloudfront")
    create_invalidation = cloudfront_client.create_invalidation
    assert [c.kwargs["DistributionId"] for c in create_invalidation.call_args_list] == [
        "E1234567890ABC",
        "E0987654321XYZ",
        "E1234567890ABC",
        "E0987654321XYZ",
    ]
    assert create_invalidation.call_args_list[0].kwargs["InvalidationBatch"]["Paths"] == {
        "Quantity": 1,
        "Items": ["/*"],
    }


def test_invalidate_cloudfront_cache_waits_for_first_pass_before_second(monkeypatch):
    cloudfront_client, waiter = make_cloudfront_client()
    calls = Mock()
    calls.attach_mock(cloudfront_client.create_invalidation, "create_invalidation")
    calls.attach_mock(waiter.wait, "wait")
    monkeypatch.setattr("dags.digital_land_builder.boto3.client", Mock(return_value=cloudfront_client))

    invalidate_cloudfront_cache = get_task_callable("invalidate-cloudfront-cache")
    invalidate_cloudfront_cache(dag_run=SimpleNamespace(run_id="test-run"), conf=make_conf("EAAA,EBBB"))

    cloudfront_client.get_waiter.assert_called_with("invalidation_completed")
    assert [(c[0], c.kwargs.get("DistributionId"), c.kwargs.get("Id")) for c in calls.mock_calls] == [
        ("create_invalidation", "EAAA", None),
        ("create_invalidation", "EBBB", None),
        ("wait", "EAAA", "I1"),
        ("wait", "EBBB", "I2"),
        ("create_invalidation", "EAAA", None),
        ("create_invalidation", "EBBB", None),
    ]


def test_invalidate_cloudfront_cache_skips_when_no_distribution_ids_configured(monkeypatch):
    invalidate_cloudfront_cache = get_task_callable("invalidate-cloudfront-cache")

    with pytest.raises(AirflowSkipException):
        invalidate_cloudfront_cache(dag_run=SimpleNamespace(run_id="test-run"), conf=make_conf(""))


def test_invalidate_cloudfront_cache_does_not_raise_when_cloudfront_rejects_invalidation(monkeypatch):
    create_invalidation = Mock(
        side_effect=ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            operation_name="CreateInvalidation",
        )
    )
    cloudfront_client = Mock(create_invalidation=create_invalidation)
    monkeypatch.setattr("dags.digital_land_builder.boto3.client", Mock(return_value=cloudfront_client))

    invalidate_cloudfront_cache = get_task_callable("invalidate-cloudfront-cache")
    invalidate_cloudfront_cache(dag_run=SimpleNamespace(run_id="test-run"), conf=make_conf("E1234567890ABC"))

    assert create_invalidation.call_count == 2
    cloudfront_client.get_waiter.assert_not_called()


def test_invalidate_cloudfront_cache_still_runs_second_pass_when_waiting_fails(monkeypatch):
    cloudfront_client, waiter = make_cloudfront_client()
    waiter.wait.side_effect = WaiterError(name="InvalidationCompleted", reason="Max attempts exceeded", last_response={})
    monkeypatch.setattr("dags.digital_land_builder.boto3.client", Mock(return_value=cloudfront_client))

    invalidate_cloudfront_cache = get_task_callable("invalidate-cloudfront-cache")
    invalidate_cloudfront_cache(dag_run=SimpleNamespace(run_id="test-run"), conf=make_conf("E1234567890ABC"))

    assert cloudfront_client.create_invalidation.call_count == 2
    waiter.wait.assert_has_calls([call(DistributionId="E1234567890ABC", Id="I1")])


def test_caches_are_cleared_after_datasette_has_had_time_to_reload():
    invalidate_cloudfront_cache_task = dag.get_task("invalidate-cloudfront-cache")

    if config["env"] == "production":
        assert invalidate_cloudfront_cache_task.upstream_task_ids == {"wait-for-datasette-reload"}
        assert dag.get_task("run-reporting-task").upstream_task_ids == {"invalidate-cloudfront-cache"}
    else:
        assert invalidate_cloudfront_cache_task.upstream_task_ids == {"build-digital-land-builder"}
