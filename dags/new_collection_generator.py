"""
Python Module to create a set of new dags to replcase the old collection dags, this will eventually be removed.
"""

from datetime import timedelta

import boto3
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRunTaskOperator,
)
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.utils.task_group import TaskGroup
from emr_dags_utils import get_secrets
from utils import dag_default_args, get_collections_dict, get_config, get_transform_batch_configs, load_specification_datasets, push_log_variables, push_vpc_config

# read config from file and environment
config = get_config()

# set some variables needed for ECS tasks,
ecs_cluster = f"{config['env']}-cluster"
collection_task_name = f"{config['env']}-mwaa-collection-task"
sqlite_injection_task_name = f"{config['env']}-sqlite-ingestion-task"
sqlite_injection_task_container_name = f"{config['env']}-sqlite-ingestion"
tiles_builder_task_name = f"{config['env']}-tile-builder-task"
tiles_builder_container_name = f"{config['env']}-tile-builder"

datasets_dict = load_specification_datasets()
collections = get_collections_dict(datasets_dict.values())


filtered_collections = {k: v for k, v in collections.items() if k in ["central-activities-zone", "transport-access-node", "title-boundary", "flood-risk-zone"]}

# read config from file and environment
config = get_config()

# set some variables needed for ECS tasks,
ecs_cluster = f"{config['env']}-cluster"
collection_task_name = f"{config['env']}-mwaa-collection-task"
sqlite_injection_task_name = f"{config['env']}-sqlite-ingestion-task"
sqlite_injection_task_container_name = f"{config['env']}-sqlite-ingestion"
tiles_builder_task_name = f"{config['env']}-tile-builder-task"
tiles_builder_container_name = f"{config['env']}-tile-builder"

datasets_dict = load_specification_datasets()
collections = get_collections_dict(datasets_dict.values())

failure_callbacks = []
if config["env"] == "production":
    failure_callbacks.append(send_slack_notification(text="The DAG {{ dag.dag_id }} failed", channel="#planning-data-alerts", username="Airflow"))

for collection, collection_datasets in filtered_collections.items():
    dag_id = f"{collection}-collection"

    with DAG(
        f"new-{collection}-collection",
        default_args=dag_default_args,
        description=f"Collection task for the {collection} collection",
        schedule=None,
        catchup=False,
        params={
            "cpu": Param(default=8192, type="integer"),
            "memory": Param(default=32768, type="integer"),
            "transformed-jobs": Param(default=8, type="integer"),
            "dataset-jobs": Param(default=8, type="integer"),
            "transform-batch-size": Param(default=200, type="integer"),
            "incremental-loading-override": Param(default=False, type="boolean"),
            "regenerate-log-override": Param(default=False, type="boolean"),
            "force-reprocessing": Param(default=False, type="boolean"),
        },
        render_template_as_native_obj=True,
        is_paused_upon_creation=False,
        on_failure_callback=failure_callbacks,
    ) as dag:

        # create functino to call to configure the DAG

        def configure_dag(**kwargs):
            ti = kwargs["ti"]

            # add env from config
            ti.xcom_push(key="env", value=config["env"])

            # add DAG parameters
            params = kwargs["params"]

            memory = int(params.get("memory"))
            cpu = int(params.get("cpu"))
            transformed_jobs = str(kwargs["params"].get("transformed-jobs"))
            dataset_jobs = str(kwargs["params"].get("dataset-jobs"))
            transform_batch_size = int(kwargs["params"].get("transform-batch-size"))
            incremental_loading_override = bool(kwargs["params"].get("incremental-loading-override"))
            regenerate_log_override = bool(kwargs["params"].get("regenerate-log-override"))
            force_reprocessing = bool(kwargs["params"].get("force-reprocessing"))

            # Push values to XCom
            ti.xcom_push(key="memory", value=memory)
            ti.xcom_push(key="cpu", value=cpu)
            ti.xcom_push(key="transformed-jobs", value=transformed_jobs)
            ti.xcom_push(key="dataset-jobs", value=dataset_jobs)
            ti.xcom_push(key="transform-batch-size", value=transform_batch_size)
            ti.xcom_push(key="incremental-loading-override", value=incremental_loading_override)
            ti.xcom_push(key="regenerate-log-override", value=regenerate_log_override)
            ti.xcom_push(key="force-reprocessing", value="True" if force_reprocessing else "")

            # add collection_data bucket # add collection bucket name
            collection_dataset_bucket_name = kwargs["conf"].get(section="custom", key="collection_dataset_bucket_name")
            ti.xcom_push(key="collection-dataset-bucket-name", value=collection_dataset_bucket_name)

            # add tiles bucket name for the tiles processing
            tiles_bucket_name = kwargs["conf"].get(section="custom", key="tiles_bucket_name")
            ti.xcom_push(key="tiles-bucket-name", value=tiles_bucket_name)

            # push collection-task log variables
            push_log_variables(ti, task_definition_name=collection_task_name, container_name=collection_task_name, prefix="collection-task")
            # push sqlite_ingestion task variables
            push_log_variables(ti, task_definition_name=sqlite_injection_task_name, container_name=sqlite_injection_task_container_name, prefix="sqlite-ingestion-task")

            # push tiles builder task variables
            push_log_variables(ti, task_definition_name=tiles_builder_task_name, container_name=tiles_builder_container_name, prefix="tiles-builder-task")
            # push aws vpc config
            push_vpc_config(ti, kwargs["conf"])

        configure_dag_task = PythonOperator(
            task_id="configure-dag",
            python_callable=configure_dag,
            dag=dag,
        )

        collect_ecs_task = EcsRunTaskOperator(
            task_id=f"{collection}-collect",
            dag=dag,
            execution_timeout=timedelta(minutes=1800),
            cluster=ecs_cluster,
            task_definition=collection_task_name,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": collection_task_name,
                        "cpu": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="cpu") | int }}',
                        "memory": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="memory") | int }}',
                        "command": ["./bin/collect.sh"],
                        "environment": [
                            {"name": "ENVIRONMENT", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="env") | string }}\''},
                            {"name": "COLLECTION_NAME", "value": collection},
                            {
                                "name": "COLLECTION_DATASET_BUCKET_NAME",
                                "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}\'',
                            },
                            {
                                "name": "HOISTED_COLLECTION_DATASET_BUCKET_NAME",
                                "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}\'',
                            },
                            # {"name": "TRANSFORMED_JOBS", "value": str('{{ task_instance.xcom_pull(task_ids="configure-dag", key="transformed-jobs") | string }}')},
                            {"name": "TRANSFORMED_JOBS", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="transformed-jobs") | string }}\''},
                            {"name": "DATASET_JOBS", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="dataset-jobs") | string }}\''},
                            {
                                "name": "INCREMENTAL_LOADING_OVERRIDE",
                                "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="incremental-loading-override") | string }}\'',
                            },
                            {"name": "REGENERATE_LOG_OVERRIDE", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="regenerate-log-override") | string }}\''},
                            {"name": "REPROCESS", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="force-reprocessing") }}\''},
                        ],
                    },
                ]
            },
            network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'},
            awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
            awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
            awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}',
            awslogs_fetch_interval=timedelta(seconds=1),
        )

        configure_dag_task >> collect_ecs_task

        # now collection has been completed we run to dataset partitioned pipelines
        for dataset in collection_datasets:

            get_batch_configs = PythonOperator(
                task_id=f"{dataset}-get-transform-batch-configs",
                python_callable=get_transform_batch_configs,
                op_kwargs={"collection": collection, "collection_task_name": collection_task_name, "dataset": dataset},
                dag=dag,
            )

            # Use expand to create mapped tasks
            transform_ecs_tasks = EcsRunTaskOperator.partial(
                task_id=f"{dataset}-transform",
                dag=dag,
                execution_timeout=timedelta(minutes=180),
                cluster=ecs_cluster,
                task_definition=collection_task_name,
                launch_type="FARGATE",
                network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'},
                awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
                awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
                awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}',
                awslogs_fetch_interval=timedelta(seconds=1),
            ).expand(overrides=get_batch_configs.output)

            collect_ecs_task >> get_batch_configs >> transform_ecs_tasks

            with TaskGroup(group_id=f"{dataset}-assemble-load-bake") as assemble_task_group:

                def get_emr_application_id(**context):
                    """Get EMR application ID and push to XCom."""
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

                get_app_id = PythonOperator(task_id="get-emr-app-id", python_callable=get_emr_application_id, execution_timeout=timedelta(minutes=2))

                ENV = config["env"]
                # get execution role arn from airflow executer
                EXECUTION_ROLE_ARN = get_secrets("emr_execution_role", ENV)
                S3_BUCKET = f"{ENV}-pd-batch-jobs-codepackage-bucket"
                S3_LOG_BUCKET = f"{ENV}-pd-batch-jobs-logs-bucket"
                S3_SOURCE_DATA_PATH = f"{ENV}-collection-data"
                S3_ENTRY_POINT = f"s3://{S3_BUCKET}/pkg/entry_script/run_main.py"
                S3_WHEEL_FILE = f"s3://{S3_BUCKET}/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
                S3_LOG_URI = f"s3://{S3_LOG_BUCKET}/"
                S3_DEPENDENCIES_PATH = f"s3://{S3_BUCKET}/pkg/dependencies/dependencies.zip"
                S3_DATA_PATH = f"s3://{S3_SOURCE_DATA_PATH}/"
                S3_ENVIRONMENT_PATH = f"s3://{S3_BUCKET}/pkg/dependencies/environment.tar.gz"

                assemble_emr_task = EmrServerlessStartJobOperator(
                    task_id="assemble-emr-job",
                    application_id=f'{{{{ task_instance.xcom_pull(task_ids="{dataset}-assemble-load-bake.get-emr-app-id", key="application_id") }}}}',
                    execution_role_arn=EXECUTION_ROLE_ARN,
                    job_driver={
                        "sparkSubmit": {
                            "entryPoint": S3_ENTRY_POINT,
                            "entryPointArguments": [
                                "--dataset",
                                dataset,
                                "--collection",
                                collection,
                                "--env",
                                ENV,
                                "--collection-data-path",
                                S3_DATA_PATH,
                                "--parquet-datasets-path",
                                f"s3://{ENV}-parquet-datasets",
                            ],
                            "sparkSubmitParameters": f"--jars /usr/lib/spark/jars/postgresql-42.7.4.jar "
                            f"--archives {S3_ENVIRONMENT_PATH}#environment "
                            f"--py-files {S3_WHEEL_FILE} "
                            "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python "
                            "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python "
                            "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python "
                            "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer "
                            "--conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator "
                            "--conf spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions",
                        }
                    },
                    configuration_overrides={"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": S3_LOG_URI}}},
                    name=f"{dataset}-job",
                    wait_for_completion=True,
                    aws_conn_id="aws_default",
                    waiter_max_attempts=180,
                    waiter_delay=60,
                    execution_timeout=timedelta(hours=3),
                )

                transform_ecs_tasks >> get_app_id >> assemble_emr_task

            # end of task group

            # Now we need to create a data package task to create the sqlite file for datasette
            package_ecs_task = EcsRunTaskOperator(
                task_id=f"{dataset}-package",
                dag=dag,
                execution_timeout=timedelta(minutes=1800),
                cluster=ecs_cluster,
                task_definition=collection_task_name,
                launch_type="FARGATE",
                overrides={
                    "containerOverrides": [
                        {
                            "name": collection_task_name,
                            "cpu": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="cpu") | int }}',
                            "memory": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="memory") | int }}',
                            "command": ["./bin/package.sh"],
                            "environment": [
                                {"name": "ENVIRONMENT", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="env") | string }}\''},
                                {"name": "COLLECTION_NAME", "value": collection},
                                {"name": "DATASET_NAME", "value": dataset},
                                {
                                    "name": "COLLECTION_DATA_BUCKET",
                                    "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}\'',
                                },
                                {
                                    "name": "PARQUET_DATASETS_BUCKET",
                                    "value": f"s3://{ENV}-parquet-datasets",
                                },
                                # {"name": "TRANSFORMED_JOBS", "value": str('{{ task_instance.xcom_pull(task_ids="configure-dag", key="transformed-jobs") | string }}')},
                                {"name": "TRANSFORMED_JOBS", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="transformed-jobs") | string }}\''},
                                {"name": "DATASET_JOBS", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="dataset-jobs") | string }}\''},
                            ],
                        },
                    ]
                },
                network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'},
                awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
                awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
                awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}',
                awslogs_fetch_interval=timedelta(seconds=1),
            )

            assemble_emr_task >> package_ecs_task

            if datasets_dict[dataset].get("typology") == "geography":
                # need to add a tiles loader task here
                tiles_builder_task = EcsRunTaskOperator(
                    task_id=f"{dataset}-tiles-builder",
                    dag=dag,
                    execution_timeout=timedelta(minutes=1800),
                    cluster=ecs_cluster,
                    task_definition=tiles_builder_task_name,
                    launch_type="FARGATE",
                    overrides={
                        "containerOverrides": [
                            {
                                "name": f"{tiles_builder_container_name}",
                                "environment": [
                                    {"name": "ENVIRONMENT", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="env") | string }}\''},
                                    {"name": "DATASET", "value": f"{dataset}"},
                                    {
                                        "name": "READ_S3_BUCKET",
                                        "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}\'',
                                    },
                                    {"name": "WRITE_S3_BUCKET", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="tiles-bucket-name") | string }}\''},
                                ],
                            },
                        ]
                    },
                    network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'},
                    awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="tiles-builder-task-log-group") }}',
                    awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="tiles-builder-task-log-region") }}',
                    awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="tiles-builder-task-log-stream-prefix") }}',
                    awslogs_fetch_interval=timedelta(seconds=1),
                )
                assemble_emr_task >> tiles_builder_task
