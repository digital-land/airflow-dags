"""
Module containing a DAG to remove a retired dataset's user-facing data from the platform.

The dataset's `environment` value in the specification is changed first so that it no longer
includes this environment (or the dataset is removed from the specification); this DAG then cleans
up what it already published. Raw collected files are kept, so the dataset can be rebuilt by
adding the environment back.
"""

import logging
from datetime import timedelta

import boto3
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from utils import (
    dag_default_args,
    get_config,
    is_dataset_available,
    load_specification_datasets,
    push_log_variables,
    push_vpc_config,
)

logger = logging.getLogger(__name__)

config = get_config()

ecs_cluster = f"{config['env']}-cluster"
collection_task_name = f"{config['env']}-mwaa-collection-task"
postgres_task_name = f"{config['env']}-sqlite-ingestion-task"
postgres_container_name = f"{config['env']}-sqlite-ingestion"
datasette_task_name = f"{config['env']}-efs-sync-task"
datasette_container_name = f"{config['env']}-efs-sync"

DAG_DOC = """
### Retire a dataset

Removes the user-facing data a dataset has left on the platform once it is no longer built in this environment.
Raw collected files (resources, logs, transformed data) are kept, so the dataset can be rebuilt by adding the environment back.

**Before running**
1. End-date the dataset's sources and endpoints in config. Do not delete the rows.
2. Change the dataset's `environment` value in the specification so it no longer includes this environment, or remove the dataset from the specification entirely.

**Parameters**
- `dataset`: the dataset to retire, as named in the specification.
- `dry_run`: on by default. Lists what would be removed and removes nothing. Only untick it after checking a dry run's output.

**What each task does**
- `guard`: fails if the specification still builds the dataset in this environment.
- `discover`: lists the dataset's files: the public downloads under `dataset/` and the built files in its collection's `dataset/` folder.
- `files`: runs the collection task's `bin/retire.sh`, which removes those files, or only lists them on a dry run.
- `postgres`: runs the Postgres loader's `retire.sh`, which removes the dataset's rows from `entity`, `old_entity` and `entity_subdivided`, or only counts them on a dry run.
- `datasette`: runs the datasette sync's `retire.sh`, which takes the dataset's database out of datasette and deletes its files, or only lists them on a dry run.

Removed files can be restored from the bucket's previous versions for 7 days.
Removed Postgres rows can be loaded again with `manual-postgres-loader`, once the dataset's `.sqlite3` file has been restored.
Datasette serves the database again once its `.sqlite3` file is next uploaded to the collection data bucket, which triggers the datasette sync.
The dataset's tiles are not removed yet.

"""


def check_dataset_can_be_retired(dataset, datasets_dict, env):
    """
    Refuse to retire a dataset Airflow still builds in env. Otherwise the next run would publish it all
    again. Uses the same rule as the DAG scheduling (the dataset's `environment` value), so an
    end-dated dataset whose `environment` still includes env is refused too. A dataset missing from
    the specification is built nowhere.
    """
    if is_dataset_available(datasets_dict.get(dataset, {}), env):
        raise AirflowFailException(
            f"{dataset}'s environment in the specification still includes {env}. Change its environment value or remove it from the specification first, then run this DAG again."
        )

    if dataset in datasets_dict:
        logger.info(f"{dataset} is in the specification but its environment does not include {env}")
    else:
        logger.info(f"{dataset} is not in the specification")


def find_dataset_files(s3_client, bucket, dataset):
    """
    Every user-facing file the dataset left in the collection data bucket: the public downloads
    under dataset/ and the files in its collection's dataset/ folder. Each collection's folder is
    listed directly, as a dataset removed from the specification no longer says which collection
    it belonged to, and searching the whole bucket would list every raw file too.
    """
    paginator = s3_client.get_paginator("list_objects_v2")

    prefixes = [f"dataset/{dataset}."]
    for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            if common_prefix["Prefix"].endswith("-collection/"):
                prefixes.append(f"{common_prefix['Prefix']}dataset/{dataset}.")

    files = []
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            files.extend({"key": item["Key"], "size": item["Size"]} for item in page.get("Contents", []))
    return files


def collection_to_retire(dataset, collections):
    """
    The collection whose dataset/ folder holds the dataset's files, without the -collection suffix, which
    the collection task adds itself. With no collection folder only the public downloads are left, so the
    dataset's own name stands in: discover has already looked in every collection folder, so the prefix
    it gives matches nothing.
    """
    if len(collections) > 1:
        raise AirflowFailException(f"{dataset} has files in more than one collection: {collections}. This DAG removes one collection's files, so remove these by hand.")
    if not collections:
        return dataset
    return collections[0].removesuffix("-collection")


def dry_run_value(dry_run):
    """DRY_RUN for the collection task, which only removes files when it is exactly "false"."""
    return "false" if dry_run is False else "true"


with DAG(
    "retire-dataset",
    default_args=dag_default_args,
    description="A manually run DAG which removes the user-facing data of a dataset no longer built in this environment.",
    doc_md=DAG_DOC,
    schedule=None,
    catchup=False,
    params={
        "dataset": Param(
            type="string",
            pattern="^[a-z0-9-]+$",
            description="The dataset to retire, as named in the specification",
        ),
        "dry_run": Param(
            default=True,
            type="boolean",
            description="List what would be removed without removing anything",
        ),
    },
    render_template_as_native_obj=True,
    is_paused_upon_creation=False,
) as dag:

    def guard(**kwargs):
        # Read the specification now rather than when the DAG was parsed, as the dataset's
        # environment value may have only just been changed
        check_dataset_can_be_retired(kwargs["params"]["dataset"], load_specification_datasets(), config["env"])

    def discover(**kwargs):
        dataset = kwargs["params"]["dataset"]
        bucket = kwargs["conf"].get(section="custom", key="collection_dataset_bucket_name")

        files = find_dataset_files(boto3.client("s3"), bucket, dataset)
        collections = sorted({file["key"].split("/")[0] for file in files if file["key"].split("/")[0].endswith("-collection")})

        if not files:
            logger.info(f"no user-facing files found for {dataset} in {bucket}")
        for file in files:
            logger.info(f"{file['size']:>15,} bytes  s3://{bucket}/{file['key']}")

        # Fails if the files are in more than one collection, after listing them above
        collection = collection_to_retire(dataset, collections)

        ti = kwargs["ti"]
        ti.xcom_push(key="files", value=files)
        ti.xcom_push(key="collections", value=collections)
        # Everything the files task passes to the collection task
        ti.xcom_push(key="collection", value=collection)
        ti.xcom_push(key="collection-dataset-bucket-name", value=bucket)
        ti.xcom_push(key="dry-run", value=dry_run_value(kwargs["params"]["dry_run"]))
        push_vpc_config(ti, kwargs["conf"])
        push_log_variables(
            ti,
            task_definition_name=collection_task_name,
            container_name=collection_task_name,
            prefix="collection-task",
        )
        push_log_variables(
            ti,
            task_definition_name=postgres_task_name,
            container_name=postgres_container_name,
            prefix="postgres-task",
        )
        push_log_variables(
            ti,
            task_definition_name=datasette_task_name,
            container_name=datasette_container_name,
            prefix="datasette-task",
        )

    guard_task = PythonOperator(task_id="guard", python_callable=guard)
    discover_task = PythonOperator(task_id="discover", python_callable=discover)

    # Runs the collection task's retire mode, which lists the files and, unless this is a dry run, removes them
    files_task = EcsRunTaskOperator(
        task_id="files",
        execution_timeout=timedelta(minutes=30),
        cluster=ecs_cluster,
        task_definition=collection_task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": collection_task_name,
                    "command": ["./bin/retire.sh"],
                    "environment": [
                        {"name": "DATASET_NAME", "value": "'{{ params.dataset }}'"},
                        {
                            "name": "COLLECTION_NAME",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="discover", key="collection") }}\'',
                        },
                        {
                            "name": "COLLECTION_DATASET_BUCKET_NAME",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="discover", key="collection-dataset-bucket-name") }}\'',
                        },
                        {
                            "name": "DRY_RUN",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="discover", key="dry-run") }}\'',
                        },
                    ],
                },
            ]
        },
        network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="discover", key="aws_vpc_config") }}'},
        awslogs_group='{{ task_instance.xcom_pull(task_ids="discover", key="collection-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="discover", key="collection-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="discover", key="collection-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=10),
    )

    # Runs the Postgres loader's retire mode, which counts the dataset's rows and, unless this is a dry run, removes them
    postgres_task = EcsRunTaskOperator(
        task_id="postgres",
        execution_timeout=timedelta(minutes=30),
        cluster=ecs_cluster,
        task_definition=postgres_task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": postgres_container_name,
                    "command": ["./retire.sh"],
                    "environment": [
                        {"name": "DATASET_NAME", "value": "'{{ params.dataset }}'"},
                        {
                            "name": "DRY_RUN",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="discover", key="dry-run") }}\'',
                        },
                    ],
                },
            ]
        },
        network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="discover", key="aws_vpc_config") }}'},
        awslogs_group='{{ task_instance.xcom_pull(task_ids="discover", key="postgres-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="discover", key="postgres-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="discover", key="postgres-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=10),
    )

    # Runs the datasette sync's retire mode, which lists the dataset's database files and, unless this is a dry run,
    # takes the database out of datasette and removes them
    datasette_task = EcsRunTaskOperator(
        task_id="datasette",
        execution_timeout=timedelta(minutes=30),
        cluster=ecs_cluster,
        task_definition=datasette_task_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": datasette_container_name,
                    "command": ["./retire.sh"],
                    "environment": [
                        {"name": "DATASET_NAME", "value": "'{{ params.dataset }}'"},
                        {
                            "name": "DRY_RUN",
                            "value": '\'{{ task_instance.xcom_pull(task_ids="discover", key="dry-run") }}\'',
                        },
                    ],
                },
            ]
        },
        network_configuration={"awsvpcConfiguration": '{{ task_instance.xcom_pull(task_ids="discover", key="aws_vpc_config") }}'},
        awslogs_group='{{ task_instance.xcom_pull(task_ids="discover", key="datasette-task-log-group") }}',
        awslogs_region='{{ task_instance.xcom_pull(task_ids="discover", key="datasette-task-log-region") }}',
        awslogs_stream_prefix='{{ task_instance.xcom_pull(task_ids="discover", key="datasette-task-log-stream-prefix") }}',
        awslogs_fetch_interval=timedelta(seconds=10),
    )

    guard_task >> discover_task >> [files_task, postgres_task, datasette_task]
