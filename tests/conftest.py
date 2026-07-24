import json
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

CONFIG_PATH = Path("dags/config.json")
CONFIG_BACKUP_PATH = CONFIG_PATH.with_suffix(".json.backup")

# Matches the output of: bin/generate_dag_config.py --env development
TEST_CONFIG = {"env": "development", "schedule": "0 0 * * *", "max_active_tasks": 50, "collection_selection": "explicit", "collections": ["ancient-woodland", "organisation"]}

# Several DAG modules call get_secrets("emr_execution_role", env) at import time, which hits real
# AWS Secrets Manager unless overridden. aws_secrets_manager.get_secret_emr_compatible supports a
# SECRET_<name> environment variable escape hatch specifically for this - see its docstring.
SECRET_ENV_VAR = "SECRET__DEVELOPMENT_PD_BATCH_DEPLOYMENT_VARIABLES_SECRET"
TEST_SECRET_VALUE = json.dumps({"emr_execution_role": "arn:aws:iam::000000000000:role/test-emr-execution-role"})


def pytest_configure(config):
    """
    Create a test config.json file, and fake the AWS secret DAGs read at import time.

    Some tests import DAG modules directly at module scope (e.g. `from dags.digital_land_builder
    import dag`), which happens during collection - before any pytest fixture would run. This
    hook runs earlier still, so both are guaranteed to be in place before anything is collected.
    If a config already exists, it's backed up and restored in pytest_unconfigure.
    """
    if CONFIG_PATH.exists():
        CONFIG_PATH.rename(CONFIG_BACKUP_PATH)

    with open(CONFIG_PATH, "w") as f:
        json.dump(TEST_CONFIG, f, indent=4)

    os.environ[SECRET_ENV_VAR] = TEST_SECRET_VALUE


def pytest_unconfigure(config):
    if CONFIG_PATH.exists():
        CONFIG_PATH.unlink()

    if CONFIG_BACKUP_PATH.exists():
        CONFIG_BACKUP_PATH.rename(CONFIG_PATH)

    os.environ.pop(SECRET_ENV_VAR, None)


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
