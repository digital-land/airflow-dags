import os
import json

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)

cluster_name = "development-cluster"
log_group = "airflow-development-mwaa-Task"
log_region = "eu-west-2"
collect_task_defn = "development-mwaa-collection-task"
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "dagrun_timeout": timedelta(minutes=5),
}

my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "config.json")
with open(configuration_file_path) as file:
    configs = json.load(file)


# def run_collection_task():
#     return EcsRegisterTaskDefinitionOperator(
#         task_id="collection-task",
#         family="collection",
#         container_definitions=[
#             {
#                 "name": "collection-task",
#                 "image": "public.ecr.aws/l6z6v3j6/development-mwaa-dataset-collection-task:publish-image",
#                 "logConfiguration": {
#                     "logDriver": "awslogs",
#                     "options": {
#                         "awslogs-create-group": "true",
#                         "awslogs-group": log_group,
#                         "awslogs-region": log_region,
#                         "awslogs-stream-prefix": "collector",
#                     },
#                 },
#             },
#         ],
#         register_task_kwargs={
#             "cpu": "1024",
#             "taskRoleArn": "arn:aws:iam::955696714113:role/development-mwaa-execution-role",
#             "executionRoleArn": "arn:aws:iam::955696714113:role/development-mwaa-execution-role",
#             "memory": "8192",
#             "networkMode": "awsvpc",
#             "requiresCompatibilities": ["FARGATE"],
#         },
#     )


for collection, datasets in configs.items():
    dag_id = f"{collection}-collection"

    with DAG(
        f"{collection}-collection",
        default_args=default_args,
        description=f"Collection task for the {collection} collection",
        schedule=None,
    ) as dag:
        EcsRunTaskOperator(
            task_id="collection",
            dag=dag,
            execution_timeout=timedelta(minutes=10),
            cluster=cluster_name,
            task_definition=collect_task_defn,
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
            # network_configuration={
            #     "awsvpcConfiguration": {
            #         "subnets": ["subnet-05a0d548ea8d901ab", "subnet-07252405b5369afd3"],
            #         "securityGroups": ["sg-0fe390dd951829c75"],
            #         "assignPublicIp": "ENABLED",
            #     }
            # },
            awslogs_group=log_group,
            awslogs_region=log_region,
            awslogs_stream_prefix="collector/collection-task",
            awslogs_fetch_interval=timedelta(seconds=5)
        )


# with DAG(
#     "EC2-Test",
#     default_args=default_args,
#     description="A test DAG to try out functionality",
#     schedule=None,
# ) as dag:
#     EcsRunTaskOperator(
#         task_id="ec2-test",
#         dag=dag,
#         execution_timeout=timedelta(minutes=5),
#         # retries=3,
#         # aws_conn_id="aws_default",
#         cluster=cluster_name,
#         task_definition="airflow-ecs-operator-test",  # register_task.output,#",
#         launch_type="EC2",
#         overrides={},
#         # overrides={"containerOverrides": [
#         #     {
#         #         "name": "test",
#         #         "command": ["python", "-c", "import time; for i in range(30): print(i); time.sleep(1)"],
#         #     },
#         # ]},
#         network_configuration={
#             "awsvpcConfiguration": {
#                 "subnets": ["subnet-05a0d548ea8d901ab", "subnet-07252405b5369afd3"],
#                 "securityGroups": ["sg-0fe390dd951829c75"],
#                 # "assignPublicIp": "ENABLED",
#             }
#         },
#         awslogs_group="airflow-development-mwaa-Task",
#         awslogs_region="eu-west-1",
#         # awslogs_stream_prefix=f"ec2/test",
#         # awslogs_fetch_interval=timedelta(seconds=5)
#     )


# with DAG(
#     "Fargate-Test",
#     default_args=default_args,
#     description="A test DAG to try out functionality",
#     schedule=None,
# ) as dag:    
#     test_task = EcsRegisterTaskDefinitionOperator(
#         task_id="register-test-task",
#         family="test",
#         container_definitions=[
#             {
#                 "name": "hello",
#                 "image": "ubuntu",
#                 "workingDirectory": "/usr/bin",
#                 "entryPoint": ["sh", "-c"],
#                 "command": ["ls"],
#                 "logConfiguration": {
#                     "logDriver": "awslogs",
#                     "options": {
#                         "awslogs-create-group": "true",
#                         "awslogs-group": log_group,
#                         "awslogs-region": log_region,
#                         "awslogs-stream-prefix": "ecs",
#                     },
#                 },
#             },
#         ],
#         register_task_kwargs={
#             "cpu": "256",
#             "taskRoleArn": "arn:aws:iam::955696714113:role/development-mwaa-execution-role",
#             "executionRoleArn": "arn:aws:iam::955696714113:role/development-mwaa-execution-role",
#             "memory": "512",
#             "networkMode": "awsvpc",
#             "requiresCompatibilities": ["FARGATE"],
#         },
#     )

#     EcsRunTaskOperator(
#         task_id="run-test-task",
#         dag=dag,
#         execution_timeout=timedelta(minutes=5),
#         # retries=3,
#         # aws_conn_id="aws_default",
#         cluster=cluster_name,
#         task_definition=test_task.output,
#         launch_type="FARGATE",
#         overrides={
#             "containerOverrides": [
#                 {
#                     "name": "hello",
#                     "command": ["uname"],
#                 },
#             ]
#         },
#         network_configuration={
#             "awsvpcConfiguration": {
#                 "subnets": ["subnet-05a0d548ea8d901ab", "subnet-07252405b5369afd3"],
#                 "securityGroups": ["sg-0fe390dd951829c75"],
#                 "assignPublicIp": "ENABLED",
#             }
#         },
#         awslogs_group=log_group,
#         awslogs_region=log_region,
#         awslogs_stream_prefix="ecs/hello",
#         awslogs_fetch_interval=timedelta(seconds=5),
#     )
