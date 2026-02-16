from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRunTaskOperator,
)
from utils import (
    dag_default_args,
    get_collections_dict,
    get_config,
    get_transform_batch_configs,
    load_specification_datasets,
    push_log_variables,
    push_vpc_config,
)

# read config from file and environment
config = get_config()

# set some variables needed for ECS tasks
ecs_cluster = f"{config['env']}-cluster"
collection_task_name = f"{config['env']}-mwaa-collection-task"

datasets_dict = load_specification_datasets()
collections = get_collections_dict(datasets_dict.values())

with DAG(
    "polars-testing",
    default_args=dag_default_args,
    description="Testing DAG for transform tasks - isolated for polars testing",
    schedule=None,
    catchup=False,
    params={
        "collection": Param(
            default="ancient-woodland-collection",
            type="string",
            description="Collection to test",
        ),
        "cpu": Param(default=8192, type="integer"),
        "memory": Param(default=32768, type="integer"),
        "transformed-jobs": Param(default=8, type="integer"),
        "dataset-jobs": Param(default=8, type="integer"),
        "transform-batch-size": Param(default=200, type="integer"),
        "incremental-loading-override": Param(default=False, type="boolean"),
        "regenerate-log-override": Param(default=False, type="boolean"),
    },
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
) as dag:

    def configure_dag(**kwargs):
        ti = kwargs["ti"]

        # add env from config
        ti.xcom_push(key="env", value=config["env"])

        # add DAG parameters
        params = kwargs["params"]

        # Get collection from params (used for both collection and dataset)
        collection = str(params.get("collection"))
        ti.xcom_push(key="collection", value=collection)

        memory = int(params.get("memory"))
        cpu = int(params.get("cpu"))
        transformed_jobs = str(kwargs["params"].get("transformed-jobs"))
        dataset_jobs = str(kwargs["params"].get("dataset-jobs"))
        transform_batch_size = int(kwargs["params"].get("transform-batch-size"))
        incremental_loading_override = bool(
            kwargs["params"].get("incremental-loading-override")
        )
        regenerate_log_override = bool(kwargs["params"].get("regenerate-log-override"))

        # Push values to XCom
        ti.xcom_push(key="memory", value=memory)
        ti.xcom_push(key="cpu", value=cpu)
        ti.xcom_push(key="transformed-jobs", value=transformed_jobs)
        ti.xcom_push(key="dataset-jobs", value=dataset_jobs)
        ti.xcom_push(key="transform-batch-size", value=transform_batch_size)
        ti.xcom_push(
            key="incremental-loading-override", value=incremental_loading_override
        )
        ti.xcom_push(key="regenerate-log-override", value=regenerate_log_override)

        # add collection_data bucket
        collection_dataset_bucket_name = kwargs["conf"].get(
            section="custom", key="collection_dataset_bucket_name"
        )
        ti.xcom_push(
            key="collection-dataset-bucket-name", value=collection_dataset_bucket_name
        )

        # push collection-task log variables
        push_log_variables(
            ti,
            task_definition_name=collection_task_name,
            container_name=collection_task_name,
            prefix="collection-task",
        )

        # push aws vpc config
        push_vpc_config(ti, kwargs["conf"])

    configure_dag_task = PythonOperator(
        task_id="configure-dag",
        python_callable=configure_dag,
        dag=dag,
    )

    # Create a single transform task for the specified dataset
    get_batch_configs = PythonOperator(
        task_id="get-transform-batch-configs",
        python_callable=get_transform_batch_configs,
        op_kwargs={
            "collection": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection") }}',
            "collection_task_name": collection_task_name,
            "dataset": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection") }}',
        },
        dag=dag,
    )

    # Use expand to create mapped tasks for the transform operation
    transform_ecs_task = EcsRunTaskOperator.partial(
        task_id="transform",
        dag=dag,
        execution_timeout=timedelta(minutes=180),
        cluster=ecs_cluster,
        task_definition=collection_task_name,
        launch_type="FARGATE",
        network_configuration={
            "awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'
        },
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="collection-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1),
    ).expand(overrides=get_batch_configs.output)

    configure_dag_task >> get_batch_configs >> transform_ecs_task
