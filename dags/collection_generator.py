from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRunTaskOperator,
)
from airflow.providers.slack.notifications.slack import send_slack_notification
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

failure_callbacks = []
if config["env"] == "production":
    failure_callbacks.append(send_slack_notification(text="The DAG {{ dag.dag_id }} failed", channel="#planning-data-alerts", username="Airflow"))

for collection, collection_datasets in collections.items():
    dag_id = f"{collection}-collection"

    with DAG(
        f"{collection}-collection",
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

            # Push values to XCom
            ti.xcom_push(key="memory", value=memory)
            ti.xcom_push(key="cpu", value=cpu)
            ti.xcom_push(key="transformed-jobs", value=transformed_jobs)
            ti.xcom_push(key="dataset-jobs", value=dataset_jobs)
            ti.xcom_push(key="transform-batch-size", value=transform_batch_size)
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

        get_batch_configs = PythonOperator(
            task_id=f"{collection}-get-transform-batch-configs",
            python_callable=get_transform_batch_configs,
            op_kwargs={"collection": collection, "collection_task_name": collection_task_name},
            dag=dag,
        )

        # Use expand to create mapped tasks
        transform_ecs_tasks = EcsRunTaskOperator.partial(
            task_id=f"{collection}-transform",
            dag=dag,
            execution_timeout=timedelta(minutes=1800),
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

        # now add loaders for datasets
        # start with  postgres tasks
        for dataset in collection_datasets:

            assemble_ecs_task = EcsRunTaskOperator(
                task_id=f"{collection}-{dataset}-assemble-and-bake",
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
                            "command": ["./bin/assemble.sh"],
                            "environment": [
                                {"name": "ENVIRONMENT", "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="env") | string }}\''},
                                {"name": "COLLECTION_NAME", "value": collection},
                                {"name": "DATASET_NAME", "value": dataset},
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

            transform_ecs_tasks >> assemble_ecs_task

            postgres_loader_task = EcsRunTaskOperator(
                task_id=f"{dataset}-postgres-loader",
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
                                    "value": '\'arn:aws:s3:::{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}'
                                    + f"/{collection}-collection/dataset/{dataset}.sqlite3'",
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
            assemble_ecs_task >> postgres_loader_task

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
                assemble_ecs_task >> tiles_builder_task
