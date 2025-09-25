from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from utils import dag_default_args, get_config, push_log_variables, push_vpc_config
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

config = get_config()
ecs_cluster = f"{config['env']}-cluster"
# digital-land-builder task definition name
digital_land_builder_task_name = f"{config['env']}-mwaa-digital-land-builder-task"
sqlite_injection_task_name = f"{config['env']}-sqlite-ingestion-task"
sqlite_injection_task_container_name = f"{config['env']}-sqlite-ingestion"

# reporting-task-definition name
reporting_task_name = f"{config['env']}-reporting-task"
reporting_task_container_name = f"{config['env']}-reporting"


failure_callbacks = []
if config['env'] == 'production':
    failure_callbacks.append(
        send_slack_notification(
            text="The DAG {{ dag.dag_id }} failed",
            channel="#planning-data-alerts",
            username="Airflow"
        )
    )

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
    on_failure_callback=failure_callbacks,
) as dag:
    
    def configure_dag(**kwargs):
        """
        function which returns the relevant configuration details
        and stores them in xcoms for other tasks. this includes:
        - get and process params into correct formats
        - read in env variables
        - access options defined in the task definitions
        """

        ti = kwargs['ti']

        # add env from config
        ti.xcom_push(key='env', value=config['env'])

        # add DAG parameters
        params = kwargs['params']

        memory = int(params.get('memory'))
        cpu = int(params.get('cpu'))
        transformed_jobs = str(kwargs['params'].get('transformed-jobs'))
        dataset_jobs = str(kwargs['params'].get('dataset-jobs'))
        incremental_loading_override = bool(kwargs['params'].get('incremental-loading-override'))
        regenerate_log_override = bool(kwargs['params'].get('regenerate-log-override'))

        # Push values to XCom
        ti.xcom_push(key='memory', value=memory)
        ti.xcom_push(key='cpu', value=cpu)
        ti.xcom_push(key='transformed-jobs', value=transformed_jobs)
        ti.xcom_push(key='dataset-jobs', value=dataset_jobs)
        ti.xcom_push(key='incremental-loading-override', value=incremental_loading_override)
        ti.xcom_push(key='regenerate-log-override', value=regenerate_log_override)


        # add collection_data bucket # add collection bucket name
        collection_dataset_bucket_name = kwargs['conf'].get(section='custom', key='collection_dataset_bucket_name')
        ti.xcom_push(key='collection-dataset-bucket-name', value=collection_dataset_bucket_name)

        # push collection-task log variables
        push_log_variables(ti,task_definition_name=digital_land_builder_task_name,container_name=digital_land_builder_task_name,prefix='collection-task')

        # push  sqlite_ingestion log variables
        push_log_variables(ti, task_definition_name=sqlite_injection_task_name,container_name=sqlite_injection_task_container_name,prefix='sqlite-ingestion-task')

        # push  sqlite_ingestion log variables
        push_log_variables(ti, task_definition_name=reporting_task_name,container_name=reporting_task_container_name,prefix='reporting-task')

        # push aws vpc config
        push_vpc_config(ti, kwargs['conf'])

    configure_dag_task = PythonOperator(
        task_id="configure-dag",
        python_callable=configure_dag,
        dag=dag,
    )

    build_digital_land_builder = EcsRunTaskOperator(
        task_id="build-digital-land-builder",
        dag=dag,
        execution_timeout=timedelta(minutes=1800),
        cluster=ecs_cluster,
        task_definition=digital_land_builder_task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": digital_land_builder_task_name,
                    "environment": [
                        {"name": "ENVIRONMENT", "value": config["env"]},
                        {
                                "name": "COLLECTION_DATASET_BUCKET_NAME",
                                "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"collection-dataset-bucket-name\") | string }}'"
                            },
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

    run_reporting_task = EcsRunTaskOperator(
        task_id="run-reporting-task",
        dag=dag,
        execution_timeout=timedelta(minutes=1800),
        cluster=ecs_cluster,
        task_definition=reporting_task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": f"{reporting_task_container_name}", 
                    "environment": [
                        {"name": "ENVIRONMENT", "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"env\") | string }}'"},
                        {
                            "name": "S3_OBJECT_ARN",
                            "value": "'arn:aws:s3:::{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"collection-dataset-bucket-name\") | string }}/digital-land-builder/dataset/digital-land.sqlite3'"
                        },
                    ],
                },
            ]
        },
        network_configuration={
            "awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'
        },
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="reporting-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="reporting-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="reporting-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1)

    )

    build_digital_land_builder >> run_reporting_task

    # now we want to load the digital land db into postgres using the sqlite innjection task
    postgres_loader_task = EcsRunTaskOperator(
        task_id=f"digital-land-postgres-loader",
        dag=dag,
        execution_timeout=timedelta(minutes=1800),
        cluster=ecs_cluster,
        task_definition=sqlite_injection_task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": f"{sqlite_injection_task_container_name}", 
                    "environment": [
                        {"name": "ENVIRONMENT", "value": "'{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"env\") | string }}'"},
                        {
                            "name": "S3_OBJECT_ARN",
                            "value": "'arn:aws:s3:::{{ task_instance.xcom_pull(task_ids=\"configure-dag\", key=\"collection-dataset-bucket-name\") | string }}/digital-land-builder/dataset/digital-land.sqlite3'"
                        },
                    ],
                },
            ]
        },
        network_configuration={
            "awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="configure-dag", key="aws_vpc_config") }}'
        },
        awslogs_group='{{ task_instance.xcom_pull(task_ids="configure-dag", key="sqlite-ingestion-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="configure-dag", key="sqlite-ingestion-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="configure-dag", key="sqlite-ingestion-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=1)
    )

    build_digital_land_builder >> postgres_loader_task