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
            "image": "collection-task",
            #"workingDirectory": "/usr/bin",
            #"entryPoint": ["sh", "-c"],
            #"command": ["ls"],
            # "logConfiguration": {
            #     "logDriver": "awslogs",
            #     "options": {
            #         "awslogs-group": "airflow-development-mwaa-Task",
            #         "awslogs-region": "eu-west-1",
            #         "awslogs-stream-prefix": "ecs/test",
            #     },
            # },
        },
    ],
    register_task_kwargs={
        "cpu": "256",
        "memory": "512",
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
            execution_timeout=timedelta(minutes=5),
            # retries=3,
            # aws_conn_id="aws_default",
            cluster=cluster_name,
            task_definition=collection_task.output,
            launch_type="FARGATE",
            overrides={},
            # overrides={"containerOverrides": [
            #     {
            #         "name": "test",
            #         "command": ["python", "-c", "import time; for i in range(30): print(i); time.sleep(1)"],
            #     },
            # ]},
            network_configuration={
                "awsvpcConfiguration": {
                    "subnets": ["subnet-05a0d548ea8d901ab", "subnet-07252405b5369afd3"],
                    "securityGroups": ["sg-0fe390dd951829c75"],
                    "assignPublicIp": "ENABLED",
                }
            },
            awslogs_group="airflow-development-mwaa-Task",
            awslogs_region="eu-west-1",
            # awslogs_stream_prefix=f"ecs/test",
            # awslogs_fetch_interval=timedelta(seconds=5)
        )

    break # Just do the first one for now

    """
    with DAG(
        "Fargate",
        default_args=DEFAULT_ARGS,
        description="A test DAG to try out functionality",
        schedule=None,
    ) as dag:
        EcsRunTaskOperator(
            task_id="fargate-test",
            dag=dag,
            execution_timeout=timedelta(minutes=5),
            # retries=3,
            # aws_conn_id="aws_default",
            cluster=cluster_name,
            task_definition=register_task.output,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "hello",
                        "command": [
                            "echo",
                            "hello",
                        ],  # python", "-c", "import time; for i in range(30): print(i); time.sleep(1)"],
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
            # awslogs_stream_prefix=f"ecs/test",
            awslogs_fetch_interval=timedelta(seconds=5),
        )
    """