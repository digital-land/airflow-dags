import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from github_actions import trigger_workflow
from utils import dag_default_args, get_config

config = get_config()
logger = logging.getLogger(__name__)

dag_doc = """
Starts config's evening pipeline (`config-evening-pipeline.yml`) at 23:00 UTC on weekdays, an hour before the
nightly collection run. The pipeline merges the day's `config-manager-update` changes into `main`, standardises
the config and uploads it to S3, so that night's collection uses it. It replaces the GitHub cron, which often
started the pipeline hours late.

Airflow only starts the workflow; the workflow raises its own Slack alert if it fails. Production only, as the
GitHub App credentials it uses are only in the production Secrets Manager.
"""


def trigger_evening_pipeline(**kwargs):
    """
    Start config's evening pipeline. It is started without inputs, which it treats the same as a scheduled run:
    writes enabled, and a Slack alert if any stage fails.
    """
    trigger_workflow(
        config["env"],
        repository="digital-land/config",
        workflow="config-evening-pipeline.yml",
    )


if config["env"] == "production":
    with DAG(
        dag_id="trigger-config-evening-pipeline",
        default_args=dag_default_args,
        doc_md=dag_doc,
        description="Start config's evening pipeline at 23:00 UTC on weekdays",
        schedule="0 23 * * 1-5",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        is_paused_upon_creation=False,
        on_failure_callback=[
            send_slack_notification(
                text="The DAG {{ dag.dag_id }} failed",
                channel="#planning-data-alerts",
                username="Airflow",
            )
        ],
    ) as dag:
        PythonOperator(
            task_id="trigger-evening-pipeline",
            python_callable=trigger_evening_pipeline,
            retries=2,
            retry_delay=timedelta(minutes=5),
            execution_timeout=timedelta(minutes=5),
        )
