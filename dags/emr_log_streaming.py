"""
Stream EMR Serverless Spark driver logs into the Airflow task log.

EmrServerlessStartJobOperator only logs the job's state while it waits. EmrServerlessStartJobWithLogsOperator
also copies the Spark driver's stdout from CloudWatch into the task log while the job runs, the way
EcsRunTaskOperator does for ECS tasks. The job must send its logs to CloudWatch, which
emr_monitoring_configuration sets up.

stdout is the stream to follow: Spark merges the Python driver's stderr into stdout, so every logger
and print call in pyspark-jobs ends up there. The driver's stderr is Spark's own (JVM) logging, which
is too noisy to stream; its last lines are printed only if the job fails, because that is where the
reason for a failure usually is.
"""

from datetime import timedelta

from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher


def emr_log_group(env):
    # Created by digital-land-infrastructure (modules/emr-serverless/logs.tf). It must be named
    # explicitly: the EMR default, /aws/emr-serverless, is outside what the EMR execution role can write to.
    return f"/aws/emr-serverless/{env}-pd-batch"


def emr_monitoring_configuration(s3_log_uri, log_group):
    return {
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": s3_log_uri},
            "cloudWatchLoggingConfiguration": {
                "enabled": True,
                "logGroupName": log_group,
                "logTypes": {"SPARK_DRIVER": ["stdout", "stderr"]},
            },
        }
    }


class _LogFetcher(AwsTaskLogFetcher):
    """AwsTaskLogFetcher that fetches one last time after stop(), so the end of the output is not lost."""

    def run(self):
        continuation_token = AwsLogsHook.ContinuationToken()
        # wait() returns as soon as stop() is called, rather than finishing the interval
        while not self._event.wait(self.fetch_interval.total_seconds()):
            self._log_events(continuation_token)
        self._log_events(continuation_token)

    def _log_events(self, continuation_token):
        for log_event in self._get_log_events(continuation_token):
            self.logger.info(self.event_to_str(log_event))


class EmrServerlessStartJobWithLogsOperator(EmrServerlessStartJobOperator):
    """
    EmrServerlessStartJobOperator that also streams the Spark driver's stdout into the task log.

    configuration_overrides must come from emr_monitoring_configuration. Do not set deferrable=True:
    the logs are read by a thread on the worker, which a deferred task gives up.
    """

    def __init__(self, *, log_fetch_interval=timedelta(seconds=30), stderr_tail_lines=100, **kwargs):
        super().__init__(**kwargs)
        self.log_fetch_interval = log_fetch_interval
        self.stderr_tail_lines = stderr_tail_lines
        self._log_fetcher = None

    def _log_fetcher_for(self, log_type):
        cloudwatch_config = self.configuration_overrides["monitoringConfiguration"]["cloudWatchLoggingConfiguration"]
        return _LogFetcher(
            log_group=cloudwatch_config["logGroupName"],
            log_stream_name=f"/applications/{self.application_id}/jobs/{self.job_id}/SPARK_DRIVER/{log_type}",
            fetch_interval=self.log_fetch_interval,
            logger=self.log,
            aws_conn_id=self.aws_conn_id,
            region_name=self.hook.conn_region_name,
        )

    def persist_links(self, context):
        # execute() calls this once the job has started and before it waits for the job to finish,
        # so it is the first point at which the job id, and so the log stream name, is known.
        super().persist_links(context)
        self._log_fetcher = self._log_fetcher_for("stdout")
        self._log_fetcher.daemon = True
        self._log_fetcher.start()

    def execute(self, context, event=None):
        try:
            job_id = super().execute(context, event)
        except Exception:
            self._stop_log_fetcher()
            self._log_stderr_tail()
            raise
        self._stop_log_fetcher()
        return job_id

    def _stop_log_fetcher(self):
        if self._log_fetcher:
            self._log_fetcher.stop()
            self._log_fetcher.join()
            self._log_fetcher = None

    def _log_stderr_tail(self):
        if not self.job_id:
            return
        # Reading logs must never be the reason the task fails, or hide the reason it did.
        try:
            lines = self._log_fetcher_for("stderr").get_last_log_messages(self.stderr_tail_lines)
        except Exception as error:
            self.log.warning("Could not read the Spark driver's stderr from CloudWatch (the job may have failed before the driver started): %s", error)
            return
        self.log.info("Last %d lines of the Spark driver's stderr:\n%s", len(lines), "\n".join(lines))
