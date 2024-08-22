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

from utills import get_config, get_task_log_config

# read config from file and environment
my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "config.json")
config  = get_config(configuration_file_path) 

# set some variablles needed for ECS tasks, 
# TODO get from environment in the future
ecs_cluster = f"{config['env']}-cluster"
collection_task_defn= f"{config['env']}-mwaa-collection-task"
aws_vpc_config={
    "subnets": ["subnet-05a0d548ea8d901ab", "subnet-07252405b5369afd3"],
    "securityGroups": ["sg-0fe390dd951829c75"],
    "assignPublicIp": "ENABLED",
}

# set some default_arrgs for all colllections
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "dagrun_timeout": timedelta(minutes=5),
}

# set task id for the initialisation task at the start
configure_dag_task_id = "configure-dag"

def configure_dag(**kwargs):
    """
    function which returns the relevant configuration details
    and stores them in xcoms for other tasks. this includes:
    - get and process params into correct formats
    - read in env variables
    - access options defined in the task definitions
    """
    # retrieve  and process parameters
    params = kwargs['params']

    memory = int(params.get('memory'))
    cpu = int(params.get('cpu'))
    transformed_jobs = str(kwargs['params'].get('transformed-jobs'))
    dataset_jobs = str(kwargs['params'].get('dataset-jobs'))
    

    # get ecs-task logging configuration
    collection_task_log_config = get_task_log_config(collection_task_defn)
    collection_task_log_config = collection_task_log_config.get('logConfiguration',{})
    collection_task_log_group = collection_task_log_config.get('options', {}).get('awslogs-group')
    collection_task_log_stream_prefix = collection_task_log_config.get('options', {}).get('awslogs-stream-prefix')
    collection_task_log_region = collection_task_log_config.get('options', {}).get('awslogs-region')


    # Push values to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='memory', value=memory)
    ti.xcom_push(key='cpu', value=cpu)
    ti.xcom_push(key='transformed-jobs',value=transformed_jobs)
    ti.xcom_push(key='dataset-jobs',value=dataset_jobs)
    ti.xcom_push(key='collection-task-log-group', value=collection_task_log_group)
    ti.xcom_push(key='collection-task-log-stream-prefix',value=collection_task_log_stream_prefix)
    ti.xcom_push(key='collection-task-log-region',value=collection_task_log_region)


for collection, datasets in config['collections'].items():
    dag_id = f"{collection}-collection"

    with DAG(
        f"{collection}-collection",
        default_args=default_args,
        description=f"Collection task for the {collection} collection",
        schedule=None,
        params={
            "cpu": Param(default=8192, type="integer"),
            "memory": Param(default=32768, type="integer"),
            "transformed-jobs":Param(default='8', type="string"),
            "dataset-jobs":Param(default='8', type="string")
        },
        render_template_as_native_obj=True
    ) as dag:
        convert_params_task = PythonOperator(
            task_id=configure_dag_task_id,
            python_callable=configure_dag,
            provide_context=True,
            dag=dag,
        )

        collection_ecs_task = EcsRunTaskOperator(
            task_id=f"{collection}-collection",
            dag=dag,
            execution_timeout=timedelta(minutes=600),
            cluster=ecs_cluster,
            task_definition=collection_task_defn,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": collection_task_defn,
                        'cpu': '{{ task_instance.xcom_pull(task_ids="configure-dag", key="cpu") | int }}', 
                        'memory': '{{ task_instance.xcom_pull(task_ids="configure-dag", key="memory") | int }}', 
                        "environment": [
                            {"name": "COLLECTION_NAME", "value": collection},
                            {"name": "TRANSFORMED_JOBS", "value": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="transformed-jobs") | string }}'},
                            {"name": "DATASET_JOBS", "value": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="dataset-jobs") | string }}'}
                        ],
                    },
                ]
            },
            network_configuration={
                "awsvpcConfiguration": aws_vpc_config
            },
            # awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
            # awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
            # awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}/' + f'{collection_task_defn}',
            awslogs_group='airflow-development-mwaa-Task',
            awslogs_region='eu-west-2',
            awslogs_stream_prefix='task/development-mwaa-collection-task',
            awslogs_fetch_interval=timedelta(seconds=1)
        )

        convert_params_task >> collection_ecs_task