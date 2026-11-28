import csv
import json
import math
import os
import urllib.request
from datetime import datetime, timedelta

import boto3
from collection_config import get_collection_dag_config

# Run every EMR Serverless job on /usr/bin/python3.11, which the emr-7.9.0 image ships
# alongside the default python3 (3.9). Shared so the jobs can't drift apart.
PYSPARK_PYTHON_CONF = (
    "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/usr/bin/python3.11 "
    "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11 "
    "--conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3.11"
)

CONFIG_DATASET_KEY = "config/pipeline/dataset.csv"

SPECIFICATION_DATASET_KEY = "specification/dataset.csv"
SPECIFICATION_DATASET_PUBLIC_URL = (
    "https://files.planning.data.gov.uk/specification/dataset.csv"
)

# Some useful default args for all DAGs
dag_default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "dagrun_timeout": timedelta(minutes=5),
}


def get_config(path=None):
    if path is None:
        my_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(my_dir, "config.json")
    with open(path) as file:
        config = json.load(file)
    return config


SPECIFICATION_DATASET_KEY = "specification/dataset.csv"
SPECIFICATION_DATASET_PUBLIC_URL = (
    "https://files.planning.data.gov.uk/specification/dataset.csv"
)


def load_specification_datasets(bucket_name=None):
    """
    Read the specification's dataset.csv from the collection dataset bucket, keyed by dataset.

    This is the copy of the specification the rest of the pipeline runs on. The bucket name
    defaults to the custom.collection_dataset_bucket_name Airflow config value.
    """
    if bucket_name is None:
        from airflow.configuration import conf

        bucket_name = conf.get(
            section="custom", key="collection_dataset_bucket_name", fallback=None
        )

    if bucket_name:
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket_name, Key=SPECIFICATION_DATASET_KEY)
        content = response["Body"].read().decode("utf-8")
    else:
        with urllib.request.urlopen(SPECIFICATION_DATASET_PUBLIC_URL) as response:
            content = response.read().decode("utf-8")

    return {row["dataset"]: row for row in csv.DictReader(content.splitlines())}


def get_collections_dict(datasets):
    """
    Given a list of datasets, return a dictionary where the keys are the collection names
    and the values are lists of datasets in that collection.
    """
    collections_dict = {}
    for dataset in datasets:
        collection = dataset.get("collection", None)
        dataset_name = dataset.get("dataset", None)
        if collection and dataset_name:
            if collection in collections_dict:
                collections_dict[collection].append(dataset_name)
            else:
                collections_dict[collection] = [dataset_name]
    return collections_dict


def is_dataset_available(dataset, env):
    """
    Given a dataset row (with an 'environment' value of '', 'development',
    'staging' or 'production') and an environment name, return True if the
    dataset should be available in that environment.
    """
    environment = dataset.get("environment", "")

    if environment == "production":
        return True

    if environment == "staging":
        return env in ("development", "staging")

    if environment == "development":
        return env == "development"

    return False


def filter_collections_for_env(collections_dict, datasets_dict, env):
    """
    Given a dict of collection -> [dataset names] and a dict of dataset name -> dataset row,
    return a new dict containing only:
      - collections where at least one dataset is available in env
      - and for those collections, only the datasets that are themselves available in env
    """
    filtered = {}
    for collection, dataset_names in collections_dict.items():
        available_datasets = [
            name
            for name in dataset_names
            if is_dataset_available(datasets_dict[name], env)
        ]
        if available_datasets:
            filtered[collection] = available_datasets
    return filtered


def get_task_log_config(ecs_client, task_definition_family):
    """
    returns the log configuration of a task definition stored in aws
    assumes the local environment is set up to access aws
    """

    # Describe the task definition
    response = ecs_client.describe_task_definition(
        taskDefinition=task_definition_family
    )

    # Extract the log configuration from the container definitions
    log_config = response["taskDefinition"]["containerDefinitions"][0].get(
        "logConfiguration", {}
    )

    return log_config


def push_vpc_config(ti, conf):
    """
    Pushes the VPC configuration details to XComs for use in other tasks.
    """
    aws_vpc_config = {
        "subnets": conf.get(section="custom", key="ecs_task_subnets").split(","),
        "securityGroups": conf.get(
            section="custom", key="ecs_task_security_groups"
        ).split(","),
        "assignPublicIp": "ENABLED",
    }

    ti.xcom_push(key="aws_vpc_config", value=aws_vpc_config)


def push_log_variables(ti, task_definition_name, container_name, prefix):
    """
    Given an ECS task definition name and container name can push
    the log configuration details to XComs for use in other tasks.
    """
    # get ecs-task logging configuration
    ecs_client = boto3.client("ecs")
    task_log_config = get_task_log_config(ecs_client, task_definition_name)
    task_log_config_options = task_log_config["options"]
    task_log_group = str(task_log_config_options.get("awslogs-group"))
    # add container name to prefix
    task_log_stream_prefix = (
        str(task_log_config_options.get("awslogs-stream-prefix")) + f"/{container_name}"
    )
    task_log_region = str(task_log_config_options.get("awslogs-region"))

    ti.xcom_push(key=f"{prefix}-log-group", value=task_log_group)
    ti.xcom_push(key=f"{prefix}-log-stream-prefix", value=task_log_stream_prefix)
    ti.xcom_push(key=f"{prefix}-log-region", value=task_log_region)


def setup_configure_dag_callable(config, task_definition_name):
    def configure_dag(**kwargs):
        """
        function which returns the relevant configuration details
        and stores them in xcoms for other tasks. this includes:
        - get and process params into correct formats
        - read in env variables
        - access options defined in the task definitions
        """

        ti = kwargs["ti"]

        # add env from config
        ti.xcom_push(key="env", value=config["env"])

        # add DAG parameters
        params = kwargs["params"]

        memory = int(params.get("memory"))
        cpu = int(params.get("cpu"))
        transformed_jobs = str(kwargs["params"].get("transformed-jobs"))
        dataset_jobs = str(kwargs["params"].get("dataset-jobs"))
        incremental_loading_override = bool(
            kwargs["params"].get("incremental-loading-override")
        )
        regenerate_log_override = bool(kwargs["params"].get("regenerate-log-override"))

        # Push values to XCom
        ti.xcom_push(key="memory", value=memory)
        ti.xcom_push(key="cpu", value=cpu)
        ti.xcom_push(key="transformed-jobs", value=transformed_jobs)
        ti.xcom_push(key="dataset-jobs", value=dataset_jobs)
        ti.xcom_push(
            key="incremental-loading-override", value=incremental_loading_override
        )
        ti.xcom_push(key="regenerate-log-override", value=regenerate_log_override)

        # add collection_data bucket # add collection bucket name
        collection_dataset_bucket_name = kwargs["conf"].get(
            section="custom", key="collection_dataset_bucket_name"
        )
        ti.xcom_push(
            key="collection-dataset-bucket-name", value=collection_dataset_bucket_name
        )

        # push collection-task log variables
        push_log_variables(
            ti,
            task_definition_name=task_definition_name,
            container_name=task_definition_name,
            prefix="collection-task",
        )
        # push aws vpc config
        push_vpc_config(ti, kwargs["conf"])

    return configure_dag


def get_dataset_collection(collections_dict, dataset):
    """
    Given a dictionary of collections and datasets, return the collection name for a given dataset.
    If the dataset is not found, return None.
    """
    for collection, datasets in collections_dict.items():
        if dataset in datasets:
            return collection
    return None


def get_datasets(collections_dict):
    """
    Given a dictionary of collections and datasets, return the list of datasets for a given collection.
    If the collection is not found, return an empty list.
    """
    all_datasets = []
    for collection, datasets in collections_dict.items():
        all_datasets.extend(datasets)

    return all_datasets


def sort_collections_dict(collections_dict):
    """
    Given a dictionary of collections and datasets, return a sorted list of collections.
    The sorting is done based on the collection name.
    """
    priority = [
        "tree-preservation-order",
        "transport-access-node",
        "flood-risk-zone",
        "listed-building",
        "conservation-area",
    ]

    def sort_key(item):
        key, value = item
        if key in priority:
            return (0, priority.index(key))  # Priority group
        return (1, value)

    sorted_collections = dict(sorted(collections_dict.items(), key=sort_key))
    return sorted_collections


def get_transform_batch_configs(ti, collection, collection_task_name, dataset):
    """
    Calculate the number of batches needed for transform based on
    the total number of resources and the batch size for a specific dataset.
    Returns a list of ECS overrides for dynamic task mapping.

    Args:
        ti: Airflow TaskInstance for pulling XCom values
        collection: Name of the collection being processed
        collection_task_name: ECS task definition name
        dataset: Name of the dataset being processed

    Returns:
        List of ECS override dictionaries for each batch
    """
    # Pull all required values from XCom
    batch_size = ti.xcom_pull(task_ids="configure-dag", key="transform-batch-size")
    batch_size = get_collection_dag_config(
        collection
    ).transform_batch_size_by_dataset.get(dataset, batch_size)
    collection_dataset_bucket_name = ti.xcom_pull(
        task_ids="configure-dag", key="collection-dataset-bucket-name"
    )
    cpu = ti.xcom_pull(task_ids="configure-dag", key="cpu")
    memory = ti.xcom_pull(task_ids="configure-dag", key="memory")
    env = ti.xcom_pull(task_ids="configure-dag", key="env")
    transformed_jobs = ti.xcom_pull(task_ids="configure-dag", key="transformed-jobs")
    dataset_jobs = ti.xcom_pull(task_ids="configure-dag", key="dataset-jobs")
    incremental_loading_override = ti.xcom_pull(
        task_ids="configure-dag", key="incremental-loading-override"
    )
    regenerate_log_override = ti.xcom_pull(
        task_ids="configure-dag", key="regenerate-log-override"
    )
    force_reprocessing = ti.xcom_pull(
        task_ids="configure-dag",
        key="force-reprocessing",
    )

    # Get the selected dependency branches from XCom
    digital_land_python_ref = (
        ti.xcom_pull(task_ids="configure-dag", key="digital-land-python-ref") or ""
    )

    makerules_url = ti.xcom_pull(task_ids="configure-dag", key="makerules-url") or ""

    # Get the resource count from the state file
    s3 = boto3.client("s3")

    # Read the state.json file to get the transform_count_by_dataset
    state_key = f"{collection}-collection/state.json"
    response = s3.get_object(Bucket=collection_dataset_bucket_name, Key=state_key)
    state_content = response["Body"].read().decode("utf-8")
    state_data = json.loads(state_content)

    # Get transform_count_by_dataset mapping and extract count for this dataset
    transform_count_by_dataset = state_data.get("transform_count_by_dataset", {})
    total_resources = transform_count_by_dataset.get(dataset, 0)
    print(f"Total resources to transform for {collection}/{dataset}: {total_resources}")
    print(f"Batch size: {batch_size}")

    # Calculate number of batches
    num_batches = math.ceil(total_resources / batch_size) if total_resources > 0 else 0
    print(f"Number of batches: {num_batches}")

    # Build overrides for each batch
    overrides_list = []
    for i in range(num_batches):
        offset = i * batch_size
        limit = batch_size

        override = {
            "containerOverrides": [
                {
                    "name": collection_task_name,
                    "cpu": cpu,
                    "memory": memory,
                    "command": ["./bin/transform.sh"],
                    "environment": [
                        {"name": "ENVIRONMENT", "value": str(env)},
                        {"name": "COLLECTION_NAME", "value": collection},
                        {"name": "DATASET", "value": dataset},
                        {
                            "name": "COLLECTION_DATASET_BUCKET_NAME",
                            "value": str(collection_dataset_bucket_name),
                        },
                        {
                            "name": "HOISTED_COLLECTION_DATASET_BUCKET_NAME",
                            "value": str(collection_dataset_bucket_name),
                        },
                        {"name": "TRANSFORMED_JOBS", "value": str(transformed_jobs)},
                        {"name": "DATASET_JOBS", "value": str(dataset_jobs)},
                        {
                            "name": "INCREMENTAL_LOADING_OVERRIDE",
                            "value": str(incremental_loading_override),
                        },
                        {
                            "name": "REGENERATE_LOG_OVERRIDE",
                            "value": str(regenerate_log_override),
                        },
                        {"name": "REPROCESS", "value": force_reprocessing},
                        {
                            "name": "DIGITAL_LAND_PYTHON_REF",
                            "value": digital_land_python_ref,
                        },
                        {"name": "MAKERULES_URL", "value": makerules_url},
                        {"name": "TRANSFORMATION_LIMIT", "value": str(limit)},
                        {"name": "TRANSFORMATION_OFFSET", "value": str(offset)},
                    ],
                },
            ]
        }
        overrides_list.append(override)

    print(f"Created {len(overrides_list)} batch configurations for {dataset}")
    return overrides_list
