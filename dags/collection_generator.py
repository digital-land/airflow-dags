import os
import json

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.models.param import Param

#TO-DO generate name from env
cluster_name = "development-cluster"
# This is the same for all tasks so can be an environment variable
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


for collection, datasets in configs.items():
    dag_id = f"{collection}-collection"

    with DAG(
        f"{collection}-collection",
        default_args=default_args,
        description=f"Collection task for the {collection} collection",
        schedule=None,
        params={
            "cpu": Param(default=8192, type="integer"),
            "memory": Param(default=32768, type="integer"),
            "timeout": Param(default=600, type="integer"),
            "transformed-jobs":Param(default=8, type="integer", minimum=0),
            "dataset-jobs":Param(default=8, type="integer", minimum=0)
        },
        render_template_as_native_obj=True
    ) as dag:
        EcsRunTaskOperator(
            task_id=f"{collection}-collection",
            dag=dag,
            execution_timeout=timedelta(minutes= '{{ params.timeout | int }}'),
            cluster=cluster_name,
            task_definition="development-mwaa-collection-task",
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "development-mwaa-collection-task",
                        'cpu': dag.params['cpu'], 
                        'memory': dag.params['memory'], 
                        "environment": [
                            {"name": "COLLECTION_NAME", "value": collection},
                            {"name": "TRANSFORMED_JOBS", "value": dag.params['transformed-jobs']},
                            {"name": "DATASET_JOBS", "value": dag.params['dataset-jobs']}
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
            awslogs_region="eu-west-2",
            awslogs_stream_prefix="task/development-mwaa-collection-task",
            awslogs_fetch_interval=timedelta(seconds=1)
        )