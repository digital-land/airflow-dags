import os
import json

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ecs import EcsRegisterTaskDefinitionOperator, EcsRunTaskOperator

cluster_name = 'development-cluster'

register_task = EcsRegisterTaskDefinitionOperator(
    task_id="register-task",
    family="test",
    container_definitions=[
        {
            "name": "hello",
            "image": "ubuntu",
            "workingDirectory": "/usr/bin",
            "entryPoint": ["sh", "-c"],
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
        "requiresCompatibilities": [
            "FARGATE"
        ],
    },
)

my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "config.json")
with open(configuration_file_path) as file:
    configs = json.load(file)


for collection,datasets in configs.items():
    dag_id = f"dynamic-generated-dag-{collection}"
    break

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

        """
        hello_task = EcsRunTaskOperator(
            task_id=f"{collection}-collector",
            cluster="development-cluster",
            task_definition=register_task.output,
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
                    "subnets": ["subnet-05a0d548ea8d901ab", "subnet-07252405b5369afd3"],
                    "securityGroups": ["sg-0fe390dd951829c75"],
                    "assignPublicIp": "ENABLED",
                },
            },
            awslogs_group="airflow-development-mwaa-Task"
            #awslogs_region=aws_region,
            #awslogs_stream_prefix=f"ecs/{container_name}",            
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
        """        
        
        collection_task = run_collection_task(collection)

        for dataset in datasets:
            dataset_task = load_dataset_into_postgres(dataset)
            # hello_task >> 
            collection_task >> dataset_task

    collection_workflow()



DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "dagrun_timeout":timedelta(minutes=5),
}

with DAG(
    "ECS-Test",
    default_args=DEFAULT_ARGS,
    description="A test DAG to try out functionality",
    schedule=None,
) as dag:
    EcsRunTaskOperator(
        task_id="ecs-test",
        dag=dag,
        execution_timeout=timedelta(minutes=5),
        #retries=3,
        #aws_conn_id="aws_default",
        cluster=cluster_name,
        task_definition="airflow-ecs-operator-test",#register_task.output,#",
        launch_type="EC2",#"FARGATE",
        overrides={},
        # overrides={"containerOverrides": [
        #     {
        #         "name": "test",
        #         "command": ["python", "-c", "import time; for i in range(30): print(i); time.sleep(1)"],
        #     },
        # ]},
        #network_configuration={
        #    "awsvpcConfiguration": {
        #        "subnets": ["subnet-05a0d548ea8d901ab", "subnet-07252405b5369afd3"],
        #        "securityGroups": ["sg-0fe390dd951829c75"],
        #        "assignPublicIp": "ENABLED",
        #    }
        #},
        awslogs_group="airflow-development-mwaa-Task",
        awslogs_region="eu-west-1",
        #awslogs_stream_prefix=f"ecs/test",
        #awslogs_fetch_interval=timedelta(seconds=5)
    )

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
        #retries=3,
        #aws_conn_id="aws_default",
        cluster=cluster_name,
        task_definition=register_task.output,
        launch_type="FARGATE",
        overrides={"containerOverrides": [
             {
                 "name": "hello",
                 "command": ["python", "-c", "import time; for i in range(30): print(i); time.sleep(1)"],
             },        
         ]},
        network_configuration={
           "awsvpcConfiguration": {
               "subnets": ["subnet-05a0d548ea8d901ab", "subnet-07252405b5369afd3"],
               "securityGroups": ["sg-0fe390dd951829c75"],
               "assignPublicIp": "ENABLED",
           }
        },
        awslogs_group="airflow-development-mwaa-Task",
        awslogs_region="eu-west-1",
        #awslogs_stream_prefix=f"ecs/test",
        awslogs_fetch_interval=timedelta(seconds=5)
    )    