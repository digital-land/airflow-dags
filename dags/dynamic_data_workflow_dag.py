import os
import json

from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ecs import EcsRegisterTaskDefinitionOperator, EcsRunTaskOperator


cluster_name = 'development-cluster'

register_task = EcsRegisterTaskDefinitionOperator(
    task_id="hello",
    family="test",
    container_definitions=[
        {
            "name": "hello",
            "image": "ubuntu",
            "workingDirectory": "/usr/bin",
            "entryPoint": ["sh", "-c"],
            "command": ["ls"],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "log_group",
                    "awslogs-region": "eu-west-1",
                    "awslogs-create-group": "true",
                    "awslogs-stream-prefix": "ecs",
                },
            },
        },
    ],
    register_task_kwargs={
        "cpu": "256",
        "memory": "512",
        "networkMode": "awsvpc",
    },
)

my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "config.json")
with open(configuration_file_path) as file:
    configs = json.load(file)


for collection,datasets in configs.items():
    dag_id = f"dynamic-generated-dag-{collection}"

    @dag(dag_id=dag_id, start_date=datetime.today())
    def collection_workflow():

        @task
        def run_collection_task(message):
            print(message)
        
        @task
        def load_dataset_into_postgres(message):
            print(message)



        # need an ECS task to run the collection, pipeline and dataset stages
        # need to get a list of updated datasets
        # add a dataset to the platform task should be mapped. We could 
        # take it from the config first mapping could come later
        # get 
        cluster_name = 'development-cluster'
        task_definition_name = 'development-collection-workflow'

        hello_task = EcsRunTaskOperator(
            task_id="hello",
            cluster=cluster_name,
            task_definition="hello",
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "hello",
                        "command": ["echo", "hello", "world"],
                    },
                ],
            },
            network_configuration={
                "awsvpcConfiguration": {
                    #"subnets": test_context[SUBNETS_KEY],
                    #"securityGroups": test_context[SECURITY_GROUPS_KEY],
                    "assignPublicIp": "ENABLED",
                },
            },
        )
        # collection_task = EcsRunTaskOperator(
        #     task_id="hello_world",
        #     cluster=cluster_name,
        #     task_definition=task_definition_name,
        #     launch_type="FARGATE",
        #     network_configuration={
        #         "awsvpcConfiguration": {
        #             "subnets": test_context[SUBNETS_KEY],
        #             "securityGroups": test_context[SECURITY_GROUPS_KEY],
        #             "assignPublicIp": "ENABLED",
        #         },
        #     },
        # )
        
        
        collection_task = run_collection_task(collection)

        for dataset in datasets:
            dataset_task = load_dataset_into_postgres(dataset)
            collection_task >> hello_task >> dataset_task

    collection_workflow()
