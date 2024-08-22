import os
import pytest
import boto3

from moto import mock_aws


@pytest.fixture(scope="function")
def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "mock_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "mock_secret_key"
    os.environ["AWS_SECURITY_TOKEN"] = "mock_security_token"
    os.environ["AWS_SESSION_TOKEN"] = "mock_session_token"

@pytest.fixture
def ecs_client(mock_aws_credentials):
    with mock_aws():
        yield boto3.client("ecs", region_name="us-east-1")
