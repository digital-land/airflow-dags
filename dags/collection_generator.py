import os
import json

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param


class CustomEcsRunTaskOperator(EcsRunTaskOperator):
    # Add execution_timeout to template_fields so it can be templated
    template_fields = EcsRunTaskOperator.template_fields + ('execution_timeout',)


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

def get_configuration(**kwargs):
    """
    function which returns the relevant configuration details
    and stores them in xcoms for other tasks. this includes:
    - get and process params into correct formats
    - read in env variables
    - access options defined in the task definitions
    """
    # retrieve  and process parameters
    params = kwargs['params']

    timeout = timedelta(minutes=int(params.get('timeout')))
    memory = int(params.get('memory'))
    cpu = int(params.get('cpu'))
    transformed_jobs = str(kwargs['params'].get('transformed-jobs'))
    dataset_jobs = str(kwargs['params'].get('dataset-jobs'))
    
    # Push values to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='timeout', value=timeout)
    ti.xcom_push(key='memory', value=memory)
    ti.xcom_push(key='cpu', value=cpu)
    ti.xcom_push(key='transformed-jobs',value=transformed_jobs)
    ti.xcom_push(key='dataset-jobs',value=dataset_jobs)
    


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
            "timeout": Param(default=10, type="integer"),
            "transformed-jobs":Param(default='8', type="string"),
            "dataset-jobs":Param(default='8', type="string")
        },
        render_template_as_native_obj=True
    ) as dag:
        convert_params_task = PythonOperator(
            task_id='get-configuration',
            python_callable=get_configuration,
            provide_context=True,
            dag=dag,
        )

        collection_ecs_task = CustomEcsRunTaskOperator(
            task_id=f"{collection}-collection",
            dag=dag,
            execution_timeout='{{ task_instance.xcom_pull(task_ids="convert_params", key="timeout") }}',
            cluster=cluster_name,
            task_definition="development-mwaa-collection-task",
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "development-mwaa-collection-task",
                        'cpu': '{{ task_instance.xcom_pull(task_ids="convert_params", key="cpu") }}', 
                        'memory': '{{ task_instance.xcom_pull(task_ids="convert_params", key="memory") }}', 
                        "environment": [
                            {"name": "COLLECTION_NAME", "value": collection},
                            {"name": "TRANSFORMED_JOBS", "value": '{{ task_instance.xcom_pull(task_ids="convert_params", key="transformed-jobs") }}'},
                            {"name": "DATASET_JOBS", "value": '{{ task_instance.xcom_pull(task_ids="convert_params", key="dataset-jobs") }}'}
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

        convert_params_task >> collection_ecs_task