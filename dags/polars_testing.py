
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from utils import dag_default_args, get_config, get_transform_batch_configs, push_log_variables, push_vpc_config

config = get_config()

ecs_cluster = f"{config['env']}-cluster"
collection_task_name = f"{config['env']}-mwaa-collection-task"

failure_callbacks = []
if config["env"] == "production":
    failure_callbacks.append(send_slack_notification(text="The DAG {{ dag.dag_id }} failed", channel="#planning-data-alerts", username="Airflow"))

with DAG(
    "polars-testing",
    default_args=dag_default_args,
    description="Polars testing DAG for dataset transform step",
    schedule=None,
    catchup=False,
    params={
        "collection": Param(default="ancient-woodland", type="string"),
        "cpu": Param(default=8192, type="integer"),
        "memory": Param(default=32768, type="integer"),
        "transform-batch-size": Param(default=200, type="integer"),
    },
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
    on_failure_callback=failure_callbacks,
) as dag:

    def configure_dag(**kwargs):
        ti = kwargs["ti"]
        params = kwargs["params"]

        ti.xcom_push(key="env", value=config["env"])
        ti.xcom_push(key="collection", value=str(params.get("collection", "ancient-woodland")))
        ti.xcom_push(key="cpu", value=int(params.get("cpu")))
        ti.xcom_push(key="memory", value=int(params.get("memory")))
        ti.xcom_push(key="transform-batch-size", value=int(params.get("transform-batch-size")))

        collection_dataset_bucket_name = kwargs["conf"].get(section="custom", key="collection_dataset_bucket_name")
        ti.xcom_push(key="collection-dataset-bucket-name", value=collection_dataset_bucket_name)

        push_log_variables(ti, task_definition_name=collection_task_name, container_name=collection_task_name, prefix="collection-task")
        push_vpc_config(ti, kwargs["conf"])

    configure_dag_task = PythonOperator(
        task_id="configure-dag",
        python_callable=configure_dag,
        dag=dag,
    )

    def get_batch_configs_wrapper(**kwargs):
        ti = kwargs["ti"]
        collection = ti.xcom_pull(task_ids="configure-dag", key="collection")
        return get_transform_batch_configs(collection=collection, collection_task_name=collection_task_name, dataset=collection, **kwargs)

    get_batch_configs = PythonOperator(
        task_id="get-transform-batch-configs",
        python_callable=get_batch_configs_wrapper,
        dag=dag,
    )

    transform_ecs_tasks = EcsRunTaskOperator.partial(
        task_id="transform",
        dag=dag,
        execution_timeout=timedelta(minutes=180),
        cluster=ecs_cluster,
        task_definition=collection_task_name,
        launch_type="FARGATE",
        network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'},
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1),
    ).expand(overrides=get_batch_configs.output)

    configure_dag_task >> get_batch_configs >> transform_ecs_tasks
