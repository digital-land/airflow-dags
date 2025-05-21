from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from utils import dag_default_args, get_config, setup_configure_dag_callable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

config = get_config()
ecs_cluster = f"{config['env']}-cluster"
collection_task_name = f"{config['env']}-mwaa-collection-task"

with DAG(
    dag_id="build-digital-land-builder",
    default_args=dag_default_args,
    description="Run digital-land-builder and upload files to S3",
    schedule=None,
    catchup=False,
    params={
        "cpu": Param(default=8192, type="integer"),
        "memory": Param(default=32768, type="integer"),
    },
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
) as dag:

    configure_dag_task = PythonOperator(
        task_id="configure-dag",
        python_callable=setup_configure_dag_callable(config, collection_task_name),
        dag=dag,
    )

    build_digital_land_builder = EcsRunTaskOperator(
        task_id="build-digital-land-builder",
        dag=dag,
        execution_timeout=timedelta(minutes=60),
        cluster=ecs_cluster,
        task_definition=collection_task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": collection_task_name,
                    "command": ["./build-digtial-land-builder.sh"],
                    "environment": [
                        {"name": "ENVIRONMENT", "value": config["env"]},
                    ],
                }
            ]
        },
        network_configuration={
            "awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'
        },
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1)
    )

    configure_dag_task >> build_digital_land_builder
