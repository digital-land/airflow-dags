"""Integration tests for utils module functions."""

import json
from unittest.mock import MagicMock

import boto3
from moto import mock_aws

from dags.utils import get_task_log_config, get_transform_batch_configs


def test_get_task_log_config_gets_config_from_aws(ecs_client):
    # Register a task definition
    ecs_client.register_task_definition(
        family="test-task",
        containerDefinitions=[
            {
                "name": "test-container",
                "image": "test",
                "memory": 512,
                "cpu": 256,
                "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": "/ecs/my-task", "awslogs-region": "us-east-1", "awslogs-stream-prefix": "ecs"}},
            }
        ],
    )

    # get the config
    log_config = get_task_log_config(ecs_client, "test-task")

    assert log_config


@mock_aws
def test_get_transform_batch_configs_creates_correct_batches(mock_aws_credentials):
    """Test that get_transform_batch_configs creates the correct number of batches based on transform_count_by_dataset."""
    # Setup S3 with test data
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-collection-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create a state.json file with transform_count_by_dataset
    state_data = {"transform_count_by_dataset": {"test-dataset": 125, "other-dataset": 50}}  # This should create 3 batches with batch_size=50
    s3_client.put_object(Bucket=bucket_name, Key="test-collection/state.json", Body=json.dumps(state_data))

    # Mock the TaskInstance and XCom pulls
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        ("configure-dag", "transform-batch-size"): 50,
        ("configure-dag", "collection-dataset-bucket-name"): bucket_name,
        ("configure-dag", "cpu"): 8192,
        ("configure-dag", "memory"): 32768,
        ("configure-dag", "env"): "development",
        ("configure-dag", "transformed-jobs"): "8",
        ("configure-dag", "dataset-jobs"): "8",
        ("configure-dag", "incremental-loading-override"): False,
        ("configure-dag", "regenerate-log-override"): False,
    }[(task_ids, key)]

    # Execute the function with dataset parameter
    result = get_transform_batch_configs(mock_ti, "test", "test-task", "test-dataset")

    # Assertions
    assert len(result) == 3, f"Expected 3 batches but got {len(result)}"

    # Check first batch
    assert result[0]["containerOverrides"][0]["environment"][-2]["value"] == "50"  # TRANSFORM_LIMIT
    assert result[0]["containerOverrides"][0]["environment"][-1]["value"] == "0"  # TRANSFORM_OFFSET

    # Check second batch
    assert result[1]["containerOverrides"][0]["environment"][-2]["value"] == "50"  # TRANSFORM_LIMIT
    assert result[1]["containerOverrides"][0]["environment"][-1]["value"] == "50"  # TRANSFORM_OFFSET

    # Check third batch
    assert result[2]["containerOverrides"][0]["environment"][-2]["value"] == "50"  # TRANSFORM_LIMIT
    assert result[2]["containerOverrides"][0]["environment"][-1]["value"] == "100"  # TRANSFORM_OFFSET

    # Verify CPU and memory are set correctly
    for override in result:
        assert override["containerOverrides"][0]["cpu"] == 8192
        assert override["containerOverrides"][0]["memory"] == 32768

    # Verify environment variables are set correctly (not templated)
    assert result[0]["containerOverrides"][0]["environment"][0]["value"] == "development"  # ENVIRONMENT
    assert result[0]["containerOverrides"][0]["environment"][1]["value"] == "test"  # COLLECTION_NAME
    assert result[0]["containerOverrides"][0]["environment"][2]["value"] == "test-dataset"  # DATASET_NAME
    assert result[0]["containerOverrides"][0]["environment"][3]["value"] == bucket_name  # COLLECTION_DATASET_BUCKET_NAME
    assert result[0]["containerOverrides"][0]["environment"][5]["value"] == "8"  # TRANSFORMED_JOBS
    assert result[0]["containerOverrides"][0]["environment"][6]["value"] == "8"  # DATASET_JOBS
    assert result[0]["containerOverrides"][0]["environment"][7]["value"] == "False"  # INCREMENTAL_LOADING_OVERRIDE
    assert result[0]["containerOverrides"][0]["environment"][8]["value"] == "False"  # REGENERATE_LOG_OVERRIDE


@mock_aws
def test_get_transform_batch_configs_handles_zero_resources(mock_aws_credentials):
    """Test that get_transform_batch_configs handles zero transform_count correctly."""
    # Setup S3 with test data
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-collection-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create a state.json file with transform_count_by_dataset with 0 count
    state_data = {"transform_count_by_dataset": {"test-dataset": 0}}
    s3_client.put_object(Bucket=bucket_name, Key="test-collection/state.json", Body=json.dumps(state_data))

    # Mock the TaskInstance
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        ("configure-dag", "transform-batch-size"): 50,
        ("configure-dag", "collection-dataset-bucket-name"): bucket_name,
        ("configure-dag", "cpu"): 8192,
        ("configure-dag", "memory"): 32768,
        ("configure-dag", "env"): "development",
        ("configure-dag", "transformed-jobs"): "8",
        ("configure-dag", "dataset-jobs"): "8",
        ("configure-dag", "incremental-loading-override"): False,
        ("configure-dag", "regenerate-log-override"): False,
    }[(task_ids, key)]

    # Execute the function with dataset parameter
    result = get_transform_batch_configs(mock_ti, "test", "test-task", "test-dataset")

    # Should default to 1 batch when transform_count is 0
    assert len(result) == 1, f"Expected 1 batch for zero resources but got {len(result)}"


@mock_aws
def test_get_transform_batch_configs_handles_missing_file(mock_aws_credentials):
    """Test that get_transform_batch_configs handles missing state file gracefully."""
    # Setup S3 without creating the state file
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-collection-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    # Note: Not creating the state.json file

    # Mock the TaskInstance
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        ("configure-dag", "transform-batch-size"): 50,
        ("configure-dag", "collection-dataset-bucket-name"): bucket_name,
        ("configure-dag", "cpu"): 8192,
        ("configure-dag", "memory"): 32768,
        ("configure-dag", "env"): "development",
        ("configure-dag", "transformed-jobs"): "8",
        ("configure-dag", "dataset-jobs"): "8",
        ("configure-dag", "incremental-loading-override"): False,
        ("configure-dag", "regenerate-log-override"): False,
    }[(task_ids, key)]

    # Execute the function with dataset parameter
    result = get_transform_batch_configs(mock_ti, "test", "test-task", "test-dataset")

    # Should default to 1 batch when file is missing
    assert len(result) == 1, f"Expected 1 batch when file is missing but got {len(result)}"


@mock_aws
def test_get_transform_batch_configs_with_exact_multiple(mock_aws_credentials):
    """Test that get_transform_batch_configs works correctly when transform_count is an exact multiple of batch_size."""
    # Setup S3 with test data
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-collection-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # 100 resources with batch size 50 should create exactly 2 batches
    state_data = {"transform_count_by_dataset": {"test-dataset": 100}}
    s3_client.put_object(Bucket=bucket_name, Key="test-collection/state.json", Body=json.dumps(state_data))

    # Mock the TaskInstance
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        ("configure-dag", "transform-batch-size"): 50,
        ("configure-dag", "collection-dataset-bucket-name"): bucket_name,
        ("configure-dag", "cpu"): 8192,
        ("configure-dag", "memory"): 32768,
        ("configure-dag", "env"): "development",
        ("configure-dag", "transformed-jobs"): "8",
        ("configure-dag", "dataset-jobs"): "8",
        ("configure-dag", "incremental-loading-override"): False,
        ("configure-dag", "regenerate-log-override"): False,
    }[(task_ids, key)]

    # Execute the function with dataset parameter
    result = get_transform_batch_configs(mock_ti, "test", "test-task", "test-dataset")

    # Should create exactly 2 batches
    assert len(result) == 2, f"Expected 2 batches but got {len(result)}"
    assert result[0]["containerOverrides"][0]["environment"][-1]["value"] == "0"
    assert result[1]["containerOverrides"][0]["environment"][-1]["value"] == "50"
