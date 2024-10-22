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

from dags.utils import get_config, get_task_log_config, load_specification_datasets

# read config from file and environment
config = get_config()

# set some variables needed for ECS tasks,
ecs_cluster = f"{config['env']}-cluster"
collection_task_defn= f"{config['env']}-mwaa-collection-task"

# set some default_arrgs for all colllections
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "dagrun_timeout": timedelta(minutes=5),
}

# set task id for the initialisation task at the start
configure_dag_task_id = "configure-dag"


def configure_dag(**kwargs):
    """
    function which returns the relevant configuration details
    and stores them in xcoms for other tasks. this includes:
    - get and process params into correct formats
    - read in env variables
    - access options defined in the task definitions
    """
    aws_vpc_config = {
        "subnets": kwargs['conf'].get(section='custom', key='ecs_task_subnets').split(","),
        "securityGroups": kwargs['conf'].get(section='custom', key='ecs_task_security_groups').split(","),
        "assignPublicIp": "ENABLED",
    }

    # retrieve  and process parameters
    params = kwargs['params']

    memory = int(params.get('memory'))
    cpu = int(params.get('cpu'))
    transformed_jobs = str(kwargs['params'].get('transformed-jobs'))
    dataset_jobs = str(kwargs['params'].get('dataset-jobs'))

    # get ecs-task logging configuration
    ecs_client = boto3.client('ecs')
    collection_task_log_config = get_task_log_config(ecs_client, collection_task_defn)
    collection_task_log_config_options = collection_task_log_config['options']
    collection_task_log_group = str(collection_task_log_config_options.get('awslogs-group'))
    # add container name to prefix
    collection_task_log_stream_prefix = str(collection_task_log_config_options.get('awslogs-stream-prefix')) + f'/{collection_task_defn}'
    collection_task_log_region = str(collection_task_log_config_options.get('awslogs-region'))
    collection_dataset_bucket_name = kwargs['conf'].get(section='custom', key='collection_dataset_bucket_name')

    # Push values to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='env', value=config['env'])
    ti.xcom_push(key='aws_vpc_config', value=aws_vpc_config)
    ti.xcom_push(key='memory', value=memory)
    ti.xcom_push(key='cpu', value=cpu)
    ti.xcom_push(key='transformed-jobs', value=transformed_jobs)
    ti.xcom_push(key='dataset-jobs', value=dataset_jobs)
    ti.xcom_push(key='collection-task-log-group', value=collection_task_log_group)
    ti.xcom_push(key='collection-task-log-stream-prefix', value=collection_task_log_stream_prefix)
    ti.xcom_push(key='collection-task-log-region', value=collection_task_log_region)
    ti.xcom_push(key='collection-dataset-bucket-name', value=collection_dataset_bucket_name)


collections = load_specification_datasets()

for collection, datasets in collections:
    dag_id = f"{collection}-collection"

    with DAG(
        f"{collection}-collection",
        default_args=default_args,
        description=f"Collection task for the {collection} collection",
        schedule=None,
        catchup=False,
        params={
            "cpu": Param(default=8192, type="integer"),
            "memory": Param(default=32768, type="integer"),
            "transformed-jobs": Param(default=8, type="integer"),
            "dataset-jobs": Param(default=8, type="integer")
        },
        render_template_as_native_obj=True,
        is_paused_upon_creation=False
    ) as dag:
        convert_params_task = PythonOperator(
            task_id=configure_dag_task_id,
            python_callable=configure_dag,
            dag=dag,
        )

        collection_ecs_task = EcsRunTaskOperator(
            task_id=f"{collection}-collection",
            dag=dag,
            execution_timeout=timedelta(minutes=600),
            cluster=ecs_cluster,
            task_definition=collection_task_defn,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": collection_task_defn,
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
                            {"name": "DATASET_JOBS", "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"dataset-jobs\") | string }}'"}
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

        convert_params_task >> collection_ecs_task