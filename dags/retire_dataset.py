"""
Module containing a DAG to remove a retired dataset's user-facing data from the platform.

The dataset's `environment` value in the specification is changed first so that it no longer
includes this environment (or the dataset is removed from the specification); this DAG then cleans
up what it already published. Raw collected files are kept, so the dataset can be rebuilt by
adding the environment back.
"""

import logging

import boto3
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from utils import dag_default_args, get_config, is_dataset_available, load_specification_datasets

logger = logging.getLogger(__name__)

config = get_config()


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


with DAG(
    "retire-dataset",
    default_args=dag_default_args,
    description="A manually run DAG which removes the user-facing data of a dataset no longer built in this environment.",
    schedule=None,
    catchup=False,
    params={
        "dataset": Param(type="string", pattern="^[a-z0-9-]+$", description="The dataset to retire, as named in the specification"),
        "dry_run": Param(default=True, type="boolean", description="List what would be removed without removing anything"),
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
        if len(collections) > 1:
            logger.warning(f"{dataset} has files in more than one collection: {collections}")

        ti = kwargs["ti"]
        ti.xcom_push(key="files", value=files)
        ti.xcom_push(key="collections", value=collections)

    guard_task = PythonOperator(task_id="guard", python_callable=guard)
    discover_task = PythonOperator(task_id="discover", python_callable=discover)

    guard_task >> discover_task
