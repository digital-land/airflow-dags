import os
import json
import pytest
import boto3
from pathlib import Path

from moto import mock_aws


@pytest.fixture(scope="session", autouse=True)
def create_test_config():
    """
    Create a test config.json file for DAG tests.

    This fixture runs automatically before any tests and creates the config file
    that DAGs expect to find in dags/config.json. If a config already exists,
    it will be backed up and restored after tests complete.

    The test configuration matches the output of:
        bin/generate_dag_config.py --env development

    To customize the test config, modify the test_config dictionary below.
    """
    config_path = Path("dags/config.json")

    # Create test configuration (matches development environment)
    test_config = {
        "env": "development",
        "schedule": "0 0 * * *",
        "max_active_tasks": 50,
        "collection_selection": "explicit",
        "collections": [
            "ancient-woodland",
            "organisation"
        ]
    }

    # Check if config already exists (backup if it does)
    backup_path = None
    if config_path.exists():
        backup_path = config_path.with_suffix('.json.backup')
        config_path.rename(backup_path)

    # Write test config
    with open(config_path, 'w') as f:
        json.dump(test_config, f, indent=4)

    yield config_path

    # Cleanup: remove test config and restore backup if it existed
    if config_path.exists():
        config_path.unlink()

    if backup_path and backup_path.exists():
        backup_path.rename(config_path)


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
