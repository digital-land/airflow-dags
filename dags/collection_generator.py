import os
import json
import boto3

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from utils import dag_default_args, get_config, load_specification_datasets, setup_configure_dag_callable

# read config from file and environment
config = get_config()

# set some variables needed for ECS tasks,
ecs_cluster = f"{config['env']}-cluster"
collection_task_name = f"{config['env']}-mwaa-collection-task"

collections = load_specification_datasets()

for collection, datasets in collections.items():
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
            "incremental-loading-override": Param(default=False, type="boolean"),
        },
        render_template_as_native_obj=True,
        is_paused_upon_creation=False
    ) as dag:
        configure_dag_task = PythonOperator(
            task_id="configure-dag",
            python_callable=setup_configure_dag_callable(config, collection_task_name),
            dag=dag,
        )

        collection_ecs_task = EcsRunTaskOperator(
            task_id=f"{collection}-collection",
            dag=dag,
            execution_timeout=timedelta(minutes=1800),
            cluster=ecs_cluster,
            task_definition=collection_task_name,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": collection_task_name,
                        'cpu': '{{ task_instance.xcom_pull(task_ids="configure-dag", key="cpu") | int }}', 
                        'memory': '{{ task_instance.xcom_pull(task_ids="configure-dag", key="memory") | int }}', 
                        "environment": [
                            {"name": "ENVIRONMENT", "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"env\") | string }}'"},
                            {"name": "COLLECTION_NAME", "value": collection},
                            {
                                "name": "COLLECTION_DATASET_BUCKET_NAME",
                                "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"collection-dataset-bucket-name\") | string }}'"
                            },
                            {
                                "name": "HOISTED_COLLECTION_DATASET_BUCKET_NAME",
                                "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"collection-dataset-bucket-name\") | string }}'"
                            },
                            # {"name": "TRANSFORMED_JOBS", "value": str('{{ task_instance.xcom_pull(task_ids="configure-dag", key="transformed-jobs") | string }}')},
                            {"name": "TRANSFORMED_JOBS", "value":"'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"transformed-jobs\") | string }}'"},
                            {"name": "DATASET_JOBS", "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"dataset-jobs\") | string }}'"},
                            {"name": "INCREMENTAL_LOADING_OVERRIDE", "value": "'{{ \"yes\" if task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"incremental-loading-override\") else \"\" }}'"}
                        ],
                    },
                ]
            },
            network_configuration={
                "awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'
            },
            awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
            awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
            awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}',
            awslogs_fetch_interval=timedelta(seconds=1)
        )

        configure_dag_task >> collection_ecs_task
