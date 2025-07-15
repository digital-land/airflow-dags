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

from utils import dag_default_args,get_config, load_specification_datasets,get_dataset_collection, get_task_log_config, get_collections_dict

config = get_config()
datasets_dict = load_specification_datasets()
collections_dict = get_collections_dict(datasets_dict.values())

# define some inputs
datasets = [dataset['dataset'] for dataset in datasets_dict if dataset.get('collection', None) is not None] 
# append digital-land so data contained in there can be loaded
datasets.append('digital-land')

ecs_cluster = f"{config['env']}-cluster"
sqlite_injection_task_name = f"{config['env']}-sqlite-ingestion-task"
container_name = f"{config['env']}-sqlite-ingestion"

with DAG(
    "manual-postgres-loader",
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

        # based on the dataset provided need to create an ARN
        # this isn't actually used to download the dataset but the bucket name 
        # and key are
        collection_dataset_bucket_name = kwargs['conf'].get(section='custom', key='collection_dataset_bucket_name')
        dataset = str(params.get('dataset'))
        if dataset == 'digital-land':
            collection = 'digital-land-builder'
        else:
            collection = get_dataset_collection(collections_dict,dataset)

        s3_object_arn = f"arn:aws:s3:::{collection_dataset_bucket_name}/{collection}-collection/dataset/{dataset}.sqlite3"

        # getmemory and cpu from params
        memory = int(params.get('memory'))
        cpu = int(params.get('cpu'))
        ti = kwargs['ti']
        ti.xcom_push(key='env', value=config['env'])
        ti.xcom_push(key='memory', value=memory)
        ti.xcom_push(key='cpu', value=cpu)
        ti.xcom_push(key='dataset', value=dataset)
        ti.xcom_push(key='collection', value=collection)
        ti.xcom_push(key='s3-object-arn', value=s3_object_arn)

        # add vpc details
        aws_vpc_config = {
            "subnets": kwargs['conf'].get(section='custom', key='ecs_task_subnets').split(","),
            "securityGroups": kwargs['conf'].get(section='custom', key='ecs_task_security_groups').split(","),
            "assignPublicIp": "ENABLED",
        }
        ti.xcom_push(key='aws_vpc_config', value=aws_vpc_config)

        # need to get log group information
        ecs_client = boto3.client('ecs')
        sqlite_injection_task_log_config = get_task_log_config(ecs_client, sqlite_injection_task_name)
        sqlite_injection_task_log_config_options = sqlite_injection_task_log_config['options']
        sqlite_injection_task_log_group = str(sqlite_injection_task_log_config_options.get('awslogs-group'))
        sqlite_injection_task_log_stream_prefix = (str(sqlite_injection_task_log_config_options.get('awslogs-stream-prefix'))
                                             + f'/{container_name}')
        sqlite_injection_task_log_region = str(sqlite_injection_task_log_config_options.get('awslogs-region'))

        # all the info we need to feed into the ECS task
        ti.xcom_push(key='sqlite-injection-task-log-group', value=sqlite_injection_task_log_group)
        ti.xcom_push(key='sqlite-injection-task-log-stream-prefix', value=sqlite_injection_task_log_stream_prefix)
        ti.xcom_push(key='sqlite-injection-task-log-region', value=sqlite_injection_task_log_region)
        ti.xcom_push(key='collection-dataset-bucket-name', value=collection_dataset_bucket_name)

    configure_dag_task = PythonOperator(
        task_id="configure-dag",
        python_callable=configure_dag,
        dag=dag,
    )

    load_dataset_ecs_task = EcsRunTaskOperator(
        task_id=f"manual-load-dataset",
        dag=dag,
        execution_timeout=timedelta(minutes=1800),
        cluster=ecs_cluster,
        task_definition=sqlite_injection_task_name,
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
                            "name": "S3_OBJECT_ARN",
                            "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"s3-object-arn\") | string }}'"
                        },
                    ],
                },
            ]
        },
        network_configuration={
            "awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'
        },
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="sqlite-injection-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="sqlite-injection-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="sqlite-injection-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1)
    )

    configure_dag_task >> load_dataset_ecs_task