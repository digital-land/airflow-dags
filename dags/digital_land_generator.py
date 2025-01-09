from datetime import timedelta

from airflow import DAG

from utils import dag_default_args, get_config, setup_configure_dag_callable
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# read config from file and environment
config = get_config()

# set some variables needed for ECS tasks,
ecs_cluster = f"{config['env']}-cluster"
task_definition_name = f"{config['env']}-mwaa-digital-land-builder-task"

with DAG(
    f"digital-land-builder",
    default_args=dag_default_args,
    description=f"Data package builder task for the digital land database",
    schedule=None,
    catchup=False,
    params={
        "cpu": Param(default=8192, type="integer"),
        "memory": Param(default=32768, type="integer"),
        "build-performance-db": Param(default=True, type="boolean"),
    },
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
) as dag:
    configure_dag_task = PythonOperator(
        task_id="configure-dag",
        python_callable=setup_configure_dag_callable(config, task_definition_name),
        dag=dag,
    )

    builder_ecs_task = EcsRunTaskOperator(
        task_id=f"build-digital-land",
        dag=dag,
        execution_timeout=timedelta(minutes=600),
        cluster=ecs_cluster,
        task_definition=task_definition_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": task_definition_name,
                    "cpu": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="cpu") | int }}',
                    "memory": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="memory") | int }}',
                    "environment": [
                        {
                            "name": "ENVIRONMENT",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="env") | string }}\'',
                        },
                        {
                            "name": "READ_S3_BUCKET",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}\'',
                        },
                        {
                            "name": "WRITE_S3_BUCKET",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-dataset-bucket-name") | string }}\'',
                        },
                        {
                            "name": "BUILD_PERF_DB",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="configure-dag", key="build-performance-db") | string }}\'',
                        },
                    ],
                },
            ]
        },
        network_configuration={
            "awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'
        },
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1),
    )

    configure_dag_task >> builder_ecs_task
