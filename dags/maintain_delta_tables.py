import logging
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from emr_dags_utils import get_secrets
from utils import dag_default_args, get_config, push_vpc_config

config = get_config()
logger = logging.getLogger(__name__)

ENV = config["env"]
EXECUTION_ROLE_ARN = get_secrets("emr_execution_role", ENV)
S3_BUCKET = f"{ENV}-pd-batch-jobs-codepackage-bucket"
S3_LOG_BUCKET = f"{ENV}-pd-batch-jobs-logs-bucket"
S3_MAINTENANCE_ENTRY_POINT = f"s3://{S3_BUCKET}/pkg/entry_script/run_maintenance.py"
S3_WHEEL_FILE = f"s3://{S3_BUCKET}/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
S3_LOG_URI = f"s3://{S3_LOG_BUCKET}/"

# 168h (7 days) keeps a week of time-travel history, matching Delta's own
# default minimum retention so retentionDurationCheck doesn't need disabling.
# maintain_datasets auto-discovers every Delta table in the bucket, so this
# value currently applies to `task` (the only Delta table today) but would
# apply to any future Delta table too.
RETENTION_HOURS = 168

failure_callbacks = []
if config["env"] == "production":
    failure_callbacks.append(send_slack_notification(text="The DAG {{ dag.dag_id }} failed", channel="#planning-data-alerts", username="Airflow"))

with DAG(
    dag_id="maintain-delta-tables",
    default_args=dag_default_args,
    description="Run Delta Lake OPTIMIZE and VACUUM on all Delta tables in the parquet datasets bucket",
    schedule="0 13 * * 0",  # weekly, Sunday 13:00 UTC — also triggerable manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "retention-hours": Param(default=RETENTION_HOURS, type="number"),
    },
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
    on_failure_callback=failure_callbacks,
) as dag:

    def configure_dag(**kwargs):
        ti = kwargs["ti"]
        params = kwargs["params"]

        retention_hours = params.get("retention-hours", RETENTION_HOURS)

        maintenance_args = [
            "--parquet-datasets-path",
            f"s3://{config['env']}-parquet-datasets",
            "--retention-hours",
            str(retention_hours),
        ]
        ti.xcom_push(key="maintenance-entry-point-args", value=maintenance_args)

        push_vpc_config(ti, kwargs["conf"])

    configure_dag_task = PythonOperator(
        task_id="configure-dag",
        python_callable=configure_dag,
        dag=dag,
    )

    def get_emr_application_id(**context):
        app_name = f"{ENV}-pd-batch-emrsl-application"
        client = boto3.client("emr-serverless", region_name="eu-west-2")
        response = client.list_applications(maxResults=50)
        for app in response.get("applications", []):
            if app["name"] == app_name:
                app_id = app["id"]
                context["ti"].xcom_push(key="application_id", value=app_id)
                return app_id
        raise ValueError(f"EMR application '{app_name}' not found")

    get_app_id_task = PythonOperator(
        task_id="get-emr-app-id",
        python_callable=get_emr_application_id,
        execution_timeout=timedelta(minutes=2),
        dag=dag,
    )

    maintain_delta_tables_task = EmrServerlessStartJobOperator(
        task_id="maintain-delta-tables",
        application_id='{{ task_instance.xcom_pull(task_ids="get-emr-app-id", key="application_id") }}',
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": S3_MAINTENANCE_ENTRY_POINT,
                "entryPointArguments": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="maintenance-entry-point-args") }}',
                "sparkSubmitParameters": f"--py-files {S3_WHEEL_FILE} " "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer",
            }
        },
        configuration_overrides={"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": S3_LOG_URI}}},
        name="maintain-delta-tables-job",
        wait_for_completion=True,
        aws_conn_id="aws_default",
        waiter_max_attempts=60,
        waiter_delay=60,
        execution_timeout=timedelta(hours=1),
    )

    configure_dag_task >> get_app_id_task >> maintain_delta_tables_task
