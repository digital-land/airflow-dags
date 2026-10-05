import boto3
import pytest
from airflow.exceptions import AirflowFailException
from moto import mock_aws

from dags.retire_dataset import check_dataset_can_be_retired, dag, find_dataset_files

DATASETS = {
    "in-production": {"dataset": "in-production", "environment": "production"},
    "in-staging": {"dataset": "in-staging", "environment": "staging"},
    "switched-off": {"dataset": "switched-off", "environment": ""},
}


def test_dataset_still_switched_on_is_refused():
    with pytest.raises(AirflowFailException, match="still includes production"):
        check_dataset_can_be_retired("in-production", DATASETS, "production")


@pytest.mark.parametrize(
    "dataset,env",
    [
        ("switched-off", "production"),
        ("in-staging", "production"),
        ("not-in-specification", "production"),
    ],
)
def test_dataset_switched_off_or_removed_can_be_retired(dataset, env):
    check_dataset_can_be_retired(dataset, DATASETS, env)


def test_staging_dataset_is_refused_in_staging():
    with pytest.raises(AirflowFailException):
        check_dataset_can_be_retired("in-staging", DATASETS, "staging")


@pytest.fixture
def s3_client(mock_aws_credentials):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-collection-data")
        yield client


def test_find_dataset_files_returns_only_user_facing_files(s3_client):
    keys = [
        # user-facing: the public downloads and the collection's dataset folder
        "dataset/tree.csv",
        "dataset/tree.geojson",
        "tree-preservation-order-collection/dataset/tree.sqlite3",
        "tree-preservation-order-collection/dataset/tree.sqlite3.json",
        # another dataset whose name starts the same way
        "dataset/tree-preservation-order.csv",
        "tree-preservation-order-collection/dataset/tree-preservation-order.sqlite3",
        # raw and intermediate files, which are kept
        "tree-preservation-order-collection/collection/resource/abc123",
        "tree-preservation-order-collection/transformed/tree/abc123.parquet",
    ]
    for key in keys:
        s3_client.put_object(Bucket="test-collection-data", Key=key, Body=b"x")

    files = find_dataset_files(s3_client, "test-collection-data", "tree")

    assert sorted(file["key"] for file in files) == [
        "dataset/tree.csv",
        "dataset/tree.geojson",
        "tree-preservation-order-collection/dataset/tree.sqlite3",
        "tree-preservation-order-collection/dataset/tree.sqlite3.json",
    ]
    assert all(file["size"] == 1 for file in files)


def test_find_dataset_files_returns_nothing_for_an_unknown_dataset(s3_client):
    s3_client.put_object(Bucket="test-collection-data", Key="dataset/tree.csv", Body=b"x")

    assert find_dataset_files(s3_client, "test-collection-data", "no-such-dataset") == []


def test_guard_runs_before_discover():
    assert dag.get_task("discover").upstream_task_ids == {"guard"}
