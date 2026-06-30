import logging
import uuid
from datetime import timedelta

import boto3
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.sensors.python import PythonSensor
from botocore.exceptions import BotoCoreError, ClientError
from emr_dags_utils import get_secrets
from utils import dag_default_args, get_config, get_datasette_db_hash, push_log_variables, push_vpc_config

config = get_config()
logger = logging.getLogger(__name__)
ecs_cluster = f"{config['env']}-cluster"
# digital-land-builder task definition name
digital_land_builder_task_name = f"{config['env']}-mwaa-digital-land-builder-task"
sqlite_injection_task_name = f"{config['env']}-sqlite-ingestion-task"
sqlite_injection_task_container_name = f"{config['env']}-sqlite-ingestion"

# reporting-task-definition name
reporting_task_name = f"{config['env']}-reporting-task"
reporting_task_container_name = f"{config['env']}-reporting"

# Datasette is deployed per-environment; the readiness check must query THIS env's
# datasette. Prod has no env subdomain; others are datasette.<env>.planning.data.gov.uk.
if config["env"] == "production":
    datasette_base_url = "https://datasette.planning.data.gov.uk"
else:
    datasette_base_url = f"https://datasette.{config['env']}.planning.data.gov.uk"


failure_callbacks = []
if config["env"] == "production":
    failure_callbacks.append(send_slack_notification(text="The DAG {{ dag.dag_id }} failed", channel="#planning-data-alerts", username="Airflow"))

with DAG(
    dag_id="build-digital-land-builder",
    default_args=dag_default_args,
    description="Run digital-land-builder and upload files to S3",
    schedule=None,
    catchup=False,
    params={
        "cpu": Param(default=8192, type="integer"),
        "memory": Param(default=32768, type="integer"),
        "debug": Param(default=False, type="boolean"),
    },
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
    on_failure_callback=failure_callbacks,
) as dag:

    def invalidate_cloudfront_cache(**kwargs):
        distribution_ids = kwargs["conf"].get(section="custom", key="digital_land_cloudfront_distribution_ids", fallback="")
        distribution_ids = [distribution_id.strip() for distribution_id in distribution_ids.split(",") if distribution_id.strip()]

        if not distribution_ids:
            logger.info("No CloudFront distribution IDs found for environment %s. Skipping cache invalidation.", config["env"])
            raise AirflowSkipException("No digital_land_cloudfront_distribution_ids configured")

        paths = ["/*"]
        cloudfront = boto3.client("cloudfront")
        logger.info("Invalidating CloudFront cache for distributions %s with paths %s", distribution_ids, paths)
        for distribution_id in distribution_ids:
            try:
                cloudfront.create_invalidation(
                    DistributionId=distribution_id,
                    InvalidationBatch={
                        "Paths": {
                            "Quantity": len(paths),
                            "Items": paths,
                        },
                        "CallerReference": f"{kwargs['dag_run'].run_id}-{distribution_id}-{uuid.uuid4()}",
                    },
                )
            except (BotoCoreError, ClientError):
                logger.exception("CloudFront cache invalidation failed for distribution %s", distribution_id)

    def configure_dag(**kwargs):
        """
        function which returns the relevant configuration details
        and stores them in xcoms for other tasks. this includes:
        - get and process params into correct formats
        - read in env variables
        - access options defined in the task definitions
        """

        ti = kwargs["ti"]

        # add env from config
        ti.xcom_push(key="env", value=config["env"])

        # add DAG parameters
        params = kwargs["params"]

        memory = int(params.get("memory"))
        cpu = int(params.get("cpu"))
        transformed_jobs = str(kwargs["params"].get("transformed-jobs"))
        dataset_jobs = str(kwargs["params"].get("dataset-jobs"))
        incremental_loading_override = bool(kwargs["params"].get("incremental-loading-override"))
        regenerate_log_override = bool(kwargs["params"].get("regenerate-log-override"))

        # Push values to XCom
        ti.xcom_push(key="memory", value=memory)
        ti.xcom_push(key="cpu", value=cpu)
        ti.xcom_push(key="transformed-jobs", value=transformed_jobs)
        ti.xcom_push(key="dataset-jobs", value=dataset_jobs)
        ti.xcom_push(key="incremental-loading-override", value=incremental_loading_override)
        ti.xcom_push(key="regenerate-log-override", value=regenerate_log_override)

        # build entry point arguments for assemble-tasks EMR job
        debug = bool(params.get("debug", False))
        tasks_args = [
            "--env",
            config["env"],
            "--collection-data-path",
            S3_DATA_PATH,
            "--parquet-datasets-path",
            f"s3://{config['env']}-parquet-datasets",
        ]
        if debug:
            tasks_args.append("--debug")
        ti.xcom_push(key="tasks-entry-point-args", value=tasks_args)

        # add collection_data bucket # add collection bucket name
        collection_dataset_bucket_name = kwargs["conf"].get(section="custom", key="collection_dataset_bucket_name")
        ti.xcom_push(key="collection-dataset-bucket-name", value=collection_dataset_bucket_name)

        # push collection-task log variables
        push_log_variables(ti, task_definition_name=digital_land_builder_task_name, container_name=digital_land_builder_task_name, prefix="collection-task")

        # push  sqlite_ingestion log variables
        push_log_variables(ti, task_definition_name=sqlite_injection_task_name, container_name=sqlite_injection_task_container_name, prefix="sqlite-ingestion-task")

        # push  sqlite_ingestion log variables
        push_log_variables(ti, task_definition_name=reporting_task_name, container_name=reporting_task_container_name, prefix="reporting-task")

        # Capture the digital-land hash datasette is serving BEFORE the build, so
        # wait-for-datasette-reload can detect when datasette flips to the freshly-built
        # database. Prod-only (the reporting path that consumes it is prod-only).
        if config["env"] in ("production", "staging"):  # TEMP: staging added for sensor test — revert before merge
            prebuild_hash = get_datasette_db_hash("digital-land", base_url=datasette_base_url)
            logger.info("Pre-build datasette digital-land hash: %s", prebuild_hash)
            ti.xcom_push(key="datasette-prebuild-hash", value=prebuild_hash)

        # push aws vpc config
        push_vpc_config(ti, kwargs["conf"])

    configure_dag_task = PythonOperator(
        task_id="configure-dag",
        python_callable=configure_dag,
        dag=dag,
    )

    build_digital_land_builder = EcsRunTaskOperator(
        task_id="build-digital-land-builder",
        dag=dag,
        execution_timeout=timedelta(minutes=1800),
        cluster=ecs_cluster,
        task_definition=digital_land_builder_task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": digital_land_builder_task_name,
                    "environment": [
                        {"name": "ENVIRONMENT", "value": config["env"]},
                        {
                            "name": "COLLECTION_DATASET_BUCKET_NAME",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}\'',
                        },
                    ],
                }
            ]
        },
        network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'},
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1),
    )
    configure_dag_task >> build_digital_land_builder

    invalidate_cloudfront_cache_task = PythonOperator(
        task_id="invalidate-cloudfront-cache",
        python_callable=invalidate_cloudfront_cache,
        dag=dag,
    )

    build_digital_land_builder >> invalidate_cloudfront_cache_task

    def datasette_has_reloaded(**kwargs):
        """
        Poke fn for wait-for-datasette-reload. Returns True once datasette is serving a
        DIFFERENT digital-land hash than it was before the build — i.e. it has picked up
        the freshly-built database. An unreachable endpoint / missing hash counts as
        "not ready yet" (datasette briefly refuses connections while it restarts).
        """
        ti = kwargs["ti"]
        prebuild_hash = ti.xcom_pull(task_ids="configure-dag", key="datasette-prebuild-hash")
        current_hash = get_datasette_db_hash("digital-land", base_url=datasette_base_url)

        if current_hash is None:
            logger.info("datasette not serving a digital-land hash yet (restart/unreachable); waiting")
            return False
        if current_hash == prebuild_hash:
            logger.info("datasette still serving pre-build hash %s; waiting for reload", prebuild_hash)
            return False

        logger.info("datasette reloaded: digital-land hash %s -> %s", prebuild_hash, current_hash)
        return True

    if config["env"] == "production":

        # Wait until datasette has picked up the freshly-built digital-land.sqlite3 before
        # running reporting (which queries datasette over HTTP). Replaces a fixed 10-min
        # sleep with a real readiness check; on timeout the task fails (Slack alert fires)
        # so we never report against stale data.
        wait_for_datasette = PythonSensor(
            task_id="wait-for-datasette-reload",
            python_callable=datasette_has_reloaded,
            poke_interval=30,
            timeout=1200,  # 20-min cap (was a fixed 10-min sleep); tune to max acceptable wait
            mode="reschedule",  # release the worker slot between checks
            dag=dag,
        )

        run_reporting_task = EcsRunTaskOperator(
            task_id="run-reporting-task",
            dag=dag,
            execution_timeout=timedelta(minutes=1800),
            cluster=ecs_cluster,
            task_definition=reporting_task_name,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": f"{reporting_task_container_name}",
                        "environment": [
                            {"name": "ENVIRONMENT", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="env") | string }}\''},
                            {"name": "COLLECTION_DATA_BUCKET", "value": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}'},
                        ],
                    },
                ]
            },
            network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'},
            awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="reporting-task-log-group") }}',
            awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="reporting-task-log-region") }}',
            awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="reporting-task-log-stream-prefix") }}',
            awslogs_fetch_interval=timedelta(seconds=1),
        )

        build_digital_land_builder >> wait_for_datasette >> run_reporting_task

    elif config["env"] == "staging":  # TEMP: sensor-only staging test — revert before merge
        wait_for_datasette = PythonSensor(
            task_id="wait-for-datasette-reload",
            python_callable=datasette_has_reloaded,
            poke_interval=30,
            timeout=1200,
            mode="reschedule",
            dag=dag,
        )
        build_digital_land_builder >> wait_for_datasette

    # now we want to load the digital land db into postgres using the sqlite innjection task
    postgres_loader_task = EcsRunTaskOperator(
        task_id="digital-land-postgres-loader",
        dag=dag,
        execution_timeout=timedelta(minutes=1800),
        cluster=ecs_cluster,
        task_definition=sqlite_injection_task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": f"{sqlite_injection_task_container_name}",
                    "environment": [
                        {"name": "ENVIRONMENT", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="env") | string }}\''},
                        {
                            "name": "S3_OBJECT_ARN",
                            "value": '\'arn:aws:s3:::{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}/digital-land-builder/dataset/digital-land.sqlite3\'',  # noqa E501
                        },
                    ],
                },
            ]
        },
        network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'},
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="sqlite-ingestion-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="sqlite-ingestion-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="sqlite-ingestion-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1),
    )

    build_digital_land_builder >> postgres_loader_task

    ENV = config["env"]
    EXECUTION_ROLE_ARN = get_secrets("emr_execution_role", ENV)
    S3_BUCKET = f"{ENV}-pd-batch-jobs-codepackage-bucket"
    S3_LOG_BUCKET = f"{ENV}-pd-batch-jobs-logs-bucket"
    S3_TASKS_ENTRY_POINT = f"s3://{S3_BUCKET}/pkg/entry_script/run_tasks.py"
    S3_WHEEL_FILE = f"s3://{S3_BUCKET}/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
    S3_LOG_URI = f"s3://{S3_LOG_BUCKET}/"
    S3_DATA_PATH = f"s3://{ENV}-collection-data/"

    def get_tasks_emr_application_id(**context):
        env = context["ti"].xcom_pull(task_ids="configure-dag", key="env")
        app_name = f"{env}-pd-batch-emrsl-application"
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
                "entryPointArguments": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="tasks-entry-point-args") }}',
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

    configure_dag_task >> get_tasks_app_id >> assemble_tasks_emr_task
