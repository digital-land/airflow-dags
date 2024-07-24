import os
import json

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)

cluster_name = "development-cluster"

collection_task = EcsRegisterTaskDefinitionOperator(
    task_id="collection-task",
    family="collection",
    container_definitions=[
        {
            "name": "collection-task",
            "image": "ubuntu:latest",# public.ecr.aws/l6z6v3j6/development-mwaa-dataset-collection-task:publish-image",
            "workingDirectory": "/usr/bin",
            "entryPoint": ["sh", "-c"],
            "command": ["ls"],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "airflow-development-mwaa-Task",
                    "awslogs-region": "eu-west-1",
                    "awslogs-stream-prefix": "ecs",
                },
            },
        },
    ],
    register_task_kwargs={
        "cpu": "1024",
        "executionRoleArn": "arn:aws:iam::955696714113:role/development-mwaa-execution-role",
        "memory": "8192",
        "networkMode": "awsvpc",
        "requiresCompatibilities": ["FARGATE"],
    },
)

my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "config.json")
with open(configuration_file_path) as file:
    configs = json.load(file)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "dagrun_timeout": timedelta(minutes=5),
}

for collection, datasets in configs.items():

    if collection != "central-activities-zone":
        continue

    dag_id = f"{collection}-collection"

    with DAG(
        f"{collection}-collector",
        default_args=DEFAULT_ARGS,
        description=f"Collection task for the {collection} collection",
        schedule=None,
    ) as dag:
        EcsRunTaskOperator(
            task_id="collection",
            dag=dag,
            execution_timeout=timedelta(minutes=10),
            # retries=3,
            # aws_conn_id="aws_default",
            cluster=cluster_name,
            task_definition=collection_task.output,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "collection-task",
                        "environment": [
                            {"name": "COLLECTION_NAME", "value": collection}
                        ],
                    },
                ]
            },
            network_configuration={
                "awsvpcConfiguration": {
                    "subnets": ["subnet-05a0d548ea8d901ab", "subnet-07252405b5369afd3"],
                    "securityGroups": ["sg-0fe390dd951829c75"],
                    "assignPublicIp": "ENABLED",
                }
            },
            awslogs_group="airflow-development-mwaa-Task",
            awslogs_region="eu-west-1",
            awslogs_stream_prefix="ecs",
            # awslogs_fetch_interval=timedelta(seconds=5)
        )
