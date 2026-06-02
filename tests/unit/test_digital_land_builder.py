from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from airflow.exceptions import AirflowSkipException
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
