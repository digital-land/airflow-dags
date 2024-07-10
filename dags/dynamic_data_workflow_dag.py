import os
import json

from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.docker.operators.docker import DockerOperator



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
            collection_task >> dataset_task

    collection_workflow()

@dag(dag_id="hello-world")
def hello():

    @task()
    def t1():
        pass

    hello_task = DockerOperator(
        task_id='hello',
        image='hello-world',
        container_name='hello-world',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        #docker_url='tcp://docker-proxy:2375',
        network_mode='bridge',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
        # 'SOMETHING': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        })
    
    t1() >> hello_task