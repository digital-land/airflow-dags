"""
Python Module to create a set of new dags to replcase the old collection dags, this will eventually be removed.
"""

from datetime import timedelta

import boto3
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRunTaskOperator,
)
from airflow.utils.task_group import TaskGroup
from emr_dags_utils import extract_and_validate_job_id, get_secrets, wait_for_emr_job_completion
from utils import dag_default_args, get_collections_dict, get_config, load_specification_datasets, push_log_variables, push_vpc_config

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

# wrap all in an if statement to avoid execution in higher environments
if config["env"] in ["development", "staging", "production"]:

    filtered_collections = {k: v for k, v in collections.items() if k in ["central-activities-zone", "transport-access-node", "title-boundary"]}

    for collection, collection_datasets in filtered_collections.items():
        dag_id = f"new-{collection}-collection"

        with DAG(
            dag_id,
            default_args=dag_default_args,
            description=f"Collection task for the {collection} collection",
            schedule=None,
            catchup=False,
            params={
                "cpu": Param(default=8192, type="integer"),
                "memory": Param(default=32768, type="integer"),
                "transformed-jobs": Param(default=8, type="integer"),
                "dataset-jobs": Param(default=8, type="integer"),
                "incremental-loading-override": Param(default=False, type="boolean"),
                "regenerate-log-override": Param(default=False, type="boolean"),
            },
            render_template_as_native_obj=True,
            is_paused_upon_creation=False,
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
                incremental_loading_override = bool(kwargs["params"].get("incremental-loading-override"))
                regenerate_log_override = bool(kwargs["params"].get("regenerate-log-override"))

                # Push values to XCom
                ti.xcom_push(key="memory", value=memory)
                ti.xcom_push(key="cpu", value=cpu)
                ti.xcom_push(key="transformed-jobs", value=transformed_jobs)
                ti.xcom_push(key="dataset-jobs", value=dataset_jobs)
                ti.xcom_push(key="incremental-loading-override", value=incremental_loading_override)
                ti.xcom_push(key="regenerate-log-override", value=regenerate_log_override)

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

            collection_ecs_task = EcsRunTaskOperator(
                task_id=f"{collection}-collect-and-transform",
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
                            "command": ["./bin/collect-and-transform.sh"],
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
                                {
                                    "name": "INCREMENTAL_LOADING_OVERRIDE",
                                    "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="incremental-loading-override") | string }}\'',
                                },
                                {"name": "REGENERATE_LOG_OVERRIDE", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="regenerate-log-override") | string }}\''},
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

            configure_dag_task >> collection_ecs_task

            # now add loaders for datasets
            # start with  postgres tasks
            for dataset in collection_datasets:

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
                    EXECUTION_ROLE_ARN = get_secrets("emr_execution_role", ENV)
                    S3_BUCKET = f"{ENV}-pd-batch-jobs-codepackage-bucket"
                    S3_LOG_BUCKET = f"{ENV}-pd-batch-jobs-logs-bucket"
                    LOAD_TYPE = get_secrets("load_type", ENV)
                    S3_SOURCE_DATA_PATH = f"{ENV}-collection-data"
                    S3_ENTRY_POINT = f"s3://{S3_BUCKET}/pkg/entry_script/run_main.py"
                    S3_WHEEL_FILE = f"s3://{S3_BUCKET}/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
                    S3_LOG_URI = f"s3://{S3_LOG_BUCKET}/"
                    S3_DEPENDENCIES_PATH = f"s3://{S3_BUCKET}/pkg/dependencies/dependencies.zip"
                    S3_DATA_PATH = f"s3://{S3_SOURCE_DATA_PATH}/"

                    assemble_emr_job = BashOperator(
                        task_id="assemble-emr-job",
                        bash_command=f"""
                        set -e
                        rm -f /tmp/{dataset}_job_run_id.txt
                        EMR_APPLICATION_ID="{{{{ task_instance.xcom_pull(task_ids='{dataset}-assemble-load-bake.get-emr-app-id') }}}}"
                        echo "Starting EMR job for {dataset}"
                        JOB_OUTPUT=$(aws emr-serverless start-job-run \\
                        --name "{dataset}-job" \\
                        --application-id $EMR_APPLICATION_ID \\
                        --execution-role-arn {EXECUTION_ROLE_ARN} \\
                        --job-driver '{{
                            "sparkSubmit": {{
                            "entryPoint": "{S3_ENTRY_POINT}",
                            "entryPointArguments": ["--load_type", "{LOAD_TYPE}", "--data_set", "{dataset}", "--path", "{S3_DATA_PATH}", "--env", "{ENV}"],
                            "sparkSubmitParameters": "--jars /usr/lib/spark/jars/postgresql-42.7.4.jar --py-files {S3_WHEEL_FILE},{S3_DEPENDENCIES_PATH} \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator \
--conf spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions"
                            }}
                        }}' \\
                        --configuration-overrides '{{
                            "monitoringConfiguration": {{
                            "s3MonitoringConfiguration": {{"logUri": "{S3_LOG_URI}"}}
                            }}
                        }}' \\
                        --region eu-west-2 --output json)
                        JOB_RUN_ID=$(echo "$JOB_OUTPUT" | jq -r '.jobRunId')
                        echo "$JOB_RUN_ID" > /tmp/{dataset}_job_run_id.txt
                        echo "Job submitted: $JOB_RUN_ID"
                        """,
                        do_xcom_push=False,
                        execution_timeout=timedelta(minutes=5),
                    )

                    def extract_job_id_wrapper(dataset_name=dataset, **context):
                        context["dataset"] = dataset_name
                        return extract_and_validate_job_id(**context)

                    extract_job_id = PythonOperator(task_id="extract-job-id", python_callable=extract_job_id_wrapper, execution_timeout=timedelta(minutes=2))

                    def wait_for_completion_wrapper(dataset_name=dataset, **context):
                        context["extract_task_id"] = f"{dataset_name}-assemble-load-bake.extract-job-id"
                        context["get_app_task_id"] = f"{dataset_name}-assemble-load-bake.get-emr-app-id"
                        return wait_for_emr_job_completion(**context)

                    wait_for_completion = PythonOperator(
                        task_id="wait-emr-completion", python_callable=wait_for_completion_wrapper, retries=0, execution_timeout=timedelta(minutes=53)
                    )

                    get_app_id >> assemble_emr_job >> extract_job_id >> wait_for_completion

                collection_ecs_task >> assemble_task_group

                if datasets_dict[dataset].get("typology") == "geography":
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
                    assemble_task_group >> tiles_builder_task
