from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from botocore.exceptions import ClientError

from dags.digital_land_builder import dag


def get_task_callable(task_id):
    return dag.get_task(task_id).python_callable


def make_conf(digital_land_cloudfront_distribution_ids):
    conf = Mock()
    conf.get = Mock(return_value=digital_land_cloudfront_distribution_ids)
    return conf


def test_invalidate_cloudfront_cache_creates_invalidation_for_each_distribution(monkeypatch):
    create_invalidation = Mock()
    cloudfront_client = Mock(create_invalidation=create_invalidation)
    boto3_client = Mock(return_value=cloudfront_client)
    monkeypatch.setattr("dags.digital_land_builder.boto3.client", boto3_client)

    invalidate_cloudfront_cache = get_task_callable("invalidate-cloudfront-cache")
    invalidate_cloudfront_cache(
        dag_run=SimpleNamespace(run_id="test-run"),
        conf=make_conf("E1234567890ABC,E0987654321XYZ"),
    )

    boto3_client.assert_called_once_with("cloudfront")
    assert create_invalidation.call_count == 2
    assert create_invalidation.call_args_list[0].kwargs["DistributionId"] == "E1234567890ABC"
    assert create_invalidation.call_args_list[1].kwargs["DistributionId"] == "E0987654321XYZ"
    assert create_invalidation.call_args_list[0].kwargs["InvalidationBatch"]["Paths"] == {
        "Quantity": 1,
        "Items": ["/*"],
    }


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

    create_invalidation.assert_called_once()


def make_datasette_conf(distribution_id):
    def get(section, key, fallback=None):
        assert (section, key) == ("custom", "datasette_cloudfront_distribution_id")
        return distribution_id

    return Mock(get=Mock(side_effect=get))


def test_invalidate_datasette_cache_invalidates_and_waits_for_completion(monkeypatch):
    create_invalidation = Mock(return_value={"Invalidation": {"Id": "I123"}})
    waiter = Mock()
    cloudfront_client = Mock(create_invalidation=create_invalidation, get_waiter=Mock(return_value=waiter))
    monkeypatch.setattr("dags.digital_land_builder.boto3.client", Mock(return_value=cloudfront_client))

    invalidate_datasette_cache = get_task_callable("invalidate-datasette-cache")
    invalidate_datasette_cache(dag_run=SimpleNamespace(run_id="test-run"), conf=make_datasette_conf("EDATASETTE123"))

    create_invalidation.assert_called_once()
    assert create_invalidation.call_args.kwargs["DistributionId"] == "EDATASETTE123"
    assert create_invalidation.call_args.kwargs["InvalidationBatch"]["Paths"] == {"Quantity": 1, "Items": ["/*"]}
    cloudfront_client.get_waiter.assert_called_once_with("invalidation_completed")
    waiter.wait.assert_called_once_with(DistributionId="EDATASETTE123", Id="I123")


def test_invalidate_datasette_cache_skips_when_no_distribution_id_configured():
    invalidate_datasette_cache = get_task_callable("invalidate-datasette-cache")

    with pytest.raises(AirflowSkipException):
        invalidate_datasette_cache(dag_run=SimpleNamespace(run_id="test-run"), conf=make_datasette_conf(""))


def test_invalidate_datasette_cache_does_not_raise_when_cloudfront_rejects_invalidation(monkeypatch):
    create_invalidation = Mock(
        side_effect=ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            operation_name="CreateInvalidation",
        )
    )
    cloudfront_client = Mock(create_invalidation=create_invalidation)
    monkeypatch.setattr("dags.digital_land_builder.boto3.client", Mock(return_value=cloudfront_client))

    invalidate_datasette_cache = get_task_callable("invalidate-datasette-cache")
    invalidate_datasette_cache(dag_run=SimpleNamespace(run_id="test-run"), conf=make_datasette_conf("EDATASETTE123"))

    create_invalidation.assert_called_once()
    cloudfront_client.get_waiter.assert_not_called()


def test_front_end_caches_are_cleared_after_datasette_cache():
    invalidate_cloudfront_cache_task = dag.get_task("invalidate-cloudfront-cache")

    assert invalidate_cloudfront_cache_task.upstream_task_ids == {"invalidate-datasette-cache"}
    assert invalidate_cloudfront_cache_task.trigger_rule == TriggerRule.NONE_FAILED
