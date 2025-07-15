"""
Module containing DAG to load a dataset into the Postgis database in RDS
"""
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

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.providers.slack.notifications.slack import send_slack_notification

from utils import dag_default_args,get_config, load_specification_datasets,get_dataset_collection, get_task_log_config,get_datasets,push_log_variables, push_vpc_config,get_collections_dict

config = get_config()
datasets_dict = load_specification_datasets()
collections_dict = get_collections_dict(datasets_dict.values())

# define some inputs
datasets = [dataset['dataset'] for dataset in datasets_dict.values() if dataset.get('typology', None) == 'geography' and dataset.get('collection', None) is not None] 
# append digital-land so data contained in there can be loaded

ecs_cluster = f"{config['env']}-cluster"
task_name = f"{config['env']}-tile-builder-task"
container_name = f"{config['env']}-tile-builder"
tiles_bucket_name = f"{config['env']}-tiles-data"

with DAG(
    "manual-tiles-loader",
    default_args=dag_default_args,
    description=f"A DAG which is not scheduled and can be mran manually to upload a dataset to the Postgis database in RDS without the need to run other processing",
    schedule=None,
    catchup=False,
    params={
        "cpu": Param(default=1024, type="integer"),
        "memory": Param(default=4096, type="integer"),
        "dataset": Param(type="string",enum=datasets),
    },
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
) as dag:
    
    def configure_dag(**kwargs):
        # get params from DAG params 
        params = kwargs['params']
        ti = kwargs['ti']
        # based on the dataset provided need to create an ARN
        # this isn't actually used to download the dataset but the bucket name 
        # and key are
        collection_dataset_bucket_name = kwargs['conf'].get(section='custom', key='collection_dataset_bucket_name')
        ti.xcom_push(key='collection-dataset-bucket-name', value=collection_dataset_bucket_name)
        
        dataset = str(params.get('dataset'))

        # getmemory and cpu from params
        memory = int(params.get('memory'))
        cpu = int(params.get('cpu'))
        ti.xcom_push(key='env', value=config['env'])
        ti.xcom_push(key='memory', value=memory)
        ti.xcom_push(key='cpu', value=cpu)
        ti.xcom_push(key='dataset', value=dataset)



        # push task log details
        push_log_variables(ti,task_definition_name=task_name,container_name=container_name,prefix='tiles-builder-task')
        # add vpc details
        push_vpc_config(ti, kwargs['conf'])

    configure_dag_task = PythonOperator(
        task_id="configure-dag",
        python_callable=configure_dag,
        dag=dag,
    )

    load_tiles_ecs_task = EcsRunTaskOperator(
        task_id=f"manual-load-dataset",
        dag=dag,
        execution_timeout=timedelta(minutes=1800),
        cluster=ecs_cluster,
        task_definition=task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": container_name,
                    'cpu': '{{ task_instance.xcom_pull(task_ids="configure-dag", key="cpu") | int }}', 
                    'memory': '{{ task_instance.xcom_pull(task_ids="configure-dag", key="memory") | int }}', 
                    "environment": [
                        {"name": "ENVIRONMENT", "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"env\") | string }}'"},
                        {
                            "name": "DATASET",
                            "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"dataset\") | string }}'"
                        },
                        {
                            "name": "READ_S3_BUCKET",
                            "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"collection-dataset-bucket-name\") | string }}'"
                        },
                        {
                            "name": "WRITE_S3_BUCKET",
                            "value": f"{tiles_bucket_name}"
                        },
                    ],
                },
            ]
        },
        network_configuration={
            "awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'
        },
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="tiles-builder-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="tiles-builder-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="tiles-builder-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1)
    )

    configure_dag_task >> load_tiles_ecs_task