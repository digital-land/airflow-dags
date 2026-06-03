"""
Module containing DAG for configuration data

Outputs:
  - configuration data in parquet_datasets bucket
  - configuration data in digital land db
"""

from datetime import timedelta

import boto3
from airflow import DAG
from airflow.operators.python import Param, PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from emr_dags_utils import get_secrets
from utils import dag_default_args, get_config

config = get_config()

with DAG(
    dag_id="configuration",
    default_args=dag_default_args,
    description="Run digital-land-builder and upload files to S3",
    schedule=None,
    catchup=False,
    params={
        "cpu": Param(default=8192, type="integer"),
        "memory": Param(default=32768, type="integer"),
    },
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
) as dag:

    ENV = config["env"]
    EXECUTION_ROLE_ARN = get_secrets("emr_execution_role", ENV)
    S3_BUCKET = f"{ENV}-pd-batch-jobs-codepackage-bucket"
    S3_LOG_BUCKET = f"{ENV}-pd-batch-jobs-logs-bucket"
    S3_TASKS_ENTRY_POINT = f"s3://{S3_BUCKET}/pkg/entry_script/run_config.py"
    S3_WHEEL_FILE = f"s3://{S3_BUCKET}/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
    S3_LOG_URI = f"s3://{S3_LOG_BUCKET}/"
    S3_DATA_PATH = f"s3://{ENV}-collection-data/"

    def get_tasks_emr_application_id(**context):
        app_name = f"{ENV}-pd-batch-emrsl-application"
        client = boto3.client("emr-serverless", region_name="eu-west-2")
        response = client.list_applications(maxResults=50)
        for app in response.get("applications", []):
            if app["name"] == app_name:
                app_id = app["id"]
                context["ti"].xcom_push(key="application_id", value=app_id)
                return app_id
        raise ValueError(f"EMR application '{app_name}' not found")

    get_tasks_app_id = PythonOperator(
        task_id="get-tasks-emr-app-id",
        python_callable=get_tasks_emr_application_id,
        execution_timeout=timedelta(minutes=2),
        dag=dag,
    )

    assemble_tasks_emr_task = EmrServerlessStartJobOperator(
        task_id="assemble-tasks",
        application_id='{{ task_instance.xcom_pull(task_ids="get-tasks-emr-app-id", key="application_id") }}',
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": S3_TASKS_ENTRY_POINT,
                "entryPointArguments": [
                    "--env",
                    ENV,
                    "--collection-data-path",
                    S3_DATA_PATH,
                    "--parquet-datasets-path",
                    f"s3://{ENV}-parquet-datasets",
                ],
                "sparkSubmitParameters": f"--py-files {S3_WHEEL_FILE} " "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer",
            }
        },
        configuration_overrides={"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": S3_LOG_URI}}},
        name="assemble-tasks-job",
        wait_for_completion=True,
        aws_conn_id="aws_default",
        waiter_max_attempts=180,
        waiter_delay=60,
        execution_timeout=timedelta(hours=3),
    )

    get_tasks_app_id >> assemble_tasks_emr_task
