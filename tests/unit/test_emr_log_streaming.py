import time
from datetime import timedelta

import boto3
import pytest
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from moto import mock_aws

from dags.emr_log_streaming import EmrServerlessStartJobWithLogsOperator, emr_log_group, emr_monitoring_configuration

LOG_GROUP = emr_log_group("development")
STREAM_PREFIX = "/applications/test-app/jobs/test-job/SPARK_DRIVER/"


def test_monitoring_configuration_sends_driver_logs_to_the_named_log_group():
    config = emr_monitoring_configuration("s3://development-pd-batch-jobs-logs-bucket/", LOG_GROUP)

    monitoring = config["monitoringConfiguration"]
    assert monitoring["s3MonitoringConfiguration"] == {"logUri": "s3://development-pd-batch-jobs-logs-bucket/"}
    assert monitoring["cloudWatchLoggingConfiguration"] == {
        "enabled": True,
        "logGroupName": "/aws/emr-serverless/development-pd-batch",
        "logTypes": {"SPARK_DRIVER": ["stdout", "stderr"]},
    }


@pytest.fixture
def logs_client(mock_aws_credentials, monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
    monkeypatch.setenv("AIRFLOW_CONN_AWS_DEFAULT", "aws://")
    with mock_aws():
        client = boto3.client("logs", region_name="eu-west-2")
        client.create_log_group(logGroupName=LOG_GROUP)
        yield client


def create_streams(logs_client):
    for log_type in ("stdout", "stderr"):
        logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=STREAM_PREFIX + log_type)


def write_log(logs_client, log_type, message):
    logs_client.put_log_events(
        logGroupName=LOG_GROUP,
        logStreamName=STREAM_PREFIX + log_type,
        logEvents=[{"timestamp": int(time.time() * 1000), "message": message}],
    )


def fake_job(monkeypatch, while_running, error=None):
    """Replace the EMR calls with a job that runs while_running(), then succeeds or raises error."""

    def execute(self, context, event=None):
        self.job_id = "test-job"
        self.persist_links(context)
        while_running()
        if error:
            raise error
        return self.job_id

    monkeypatch.setattr(EmrServerlessStartJobOperator, "execute", execute)
    monkeypatch.setattr(EmrServerlessStartJobOperator, "persist_links", lambda self, context: None)


def make_operator():
    return EmrServerlessStartJobWithLogsOperator(
        task_id="test-task",
        application_id="test-app",
        execution_role_arn="arn:aws:iam::000000000000:role/test-emr-execution-role",
        job_driver={},
        configuration_overrides=emr_monitoring_configuration("s3://test-logs/", LOG_GROUP),
        log_fetch_interval=timedelta(seconds=0.1),
    )


def test_driver_stdout_is_copied_into_the_task_log(logs_client, monkeypatch, caplog):
    create_streams(logs_client)

    def while_running():
        write_log(logs_client, "stdout", "first line")
        write_log(logs_client, "stdout", "last line, written as the job finishes")

    fake_job(monkeypatch, while_running)
    operator = make_operator()

    assert operator.execute({}) == "test-job"

    assert "first line" in caplog.text
    assert "last line, written as the job finishes" in caplog.text
    assert operator._log_fetcher is None


def test_driver_stderr_is_not_logged_when_the_job_succeeds(logs_client, monkeypatch, caplog):
    create_streams(logs_client)
    fake_job(monkeypatch, lambda: write_log(logs_client, "stderr", "spark INFO noise"))

    make_operator().execute({})

    assert "spark INFO noise" not in caplog.text


def test_driver_stderr_is_logged_when_the_job_fails(logs_client, monkeypatch, caplog):
    create_streams(logs_client)
    fake_job(
        monkeypatch,
        lambda: write_log(logs_client, "stderr", "java.lang.OutOfMemoryError: Java heap space"),
        error=RuntimeError("Serverless Job failed"),
    )

    with pytest.raises(RuntimeError, match="Serverless Job failed"):
        make_operator().execute({})

    assert "java.lang.OutOfMemoryError: Java heap space" in caplog.text


def test_job_that_fails_before_the_driver_starts_still_raises_its_own_error(logs_client, monkeypatch, caplog):
    # no log streams: the job failed before the Spark driver wrote anything
    fake_job(monkeypatch, lambda: None, error=RuntimeError("AccessDeniedException"))

    with pytest.raises(RuntimeError, match="AccessDeniedException"):
        make_operator().execute({})

    assert "Could not read the Spark driver's stderr" in caplog.text
