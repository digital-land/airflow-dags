import csv
import json
import os
import tempfile
import urllib
from datetime import datetime, timedelta
from pathlib import Path

import boto3

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


# TBD no idea why this isn't in the config rather than directly loading stuff from the spec
def load_specification_datasets():
    with tempfile.TemporaryDirectory() as tmpdir:

        dataset_spec_url = "https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv"
        dataset_spec_path = Path(tmpdir) / "dataset.csv"
        urllib.request.urlretrieve(dataset_spec_url, dataset_spec_path)

        datasets_dict = {}

        with open(dataset_spec_path, newline="") as f:
            dictreader = csv.DictReader(f)
            for row in dictreader:
                datasets_dict[row["dataset"]] = row

        return datasets_dict


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


def get_task_log_config(ecs_client, task_definition_family):
    """
    returns the log configuration of a task definition stored in aws
    assumes the local environment is set up to access aws
    """

    # Describe the task definition
    response = ecs_client.describe_task_definition(taskDefinition=task_definition_family)

    # Extract the log configuration from the container definitions
    log_config = response["taskDefinition"]["containerDefinitions"][0].get("logConfiguration", {})

    return log_config


def push_vpc_config(ti, conf):
    """
    Pushes the VPC configuration details to XComs for use in other tasks.
    """
    aws_vpc_config = {
        "subnets": conf.get(section="custom", key="ecs_task_subnets").split(","),
        "securityGroups": conf.get(section="custom", key="ecs_task_security_groups").split(","),
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
    task_log_stream_prefix = str(task_log_config_options.get("awslogs-stream-prefix")) + f"/{container_name}"
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
        incremental_loading_override = bool(kwargs["params"].get("incremental-loading-override"))
        regenerate_log_override = bool(kwargs["params"].get("regenerate-log-override"))

        # Push values to XCom
        ti.xcom_push(key="memory", value=memory)
        ti.xcom_push(key="cpu", value=cpu)
        ti.xcom_push(key="transformed-jobs", value=transformed_jobs)
        ti.xcom_push(key="dataset-jobs", value=dataset_jobs)
        ti.xcom_push(key="incremental-loading-override", value=incremental_loading_override)
        ti.xcom_push(key="regenerate-log-override", value=regenerate_log_override)

        # add collection_data bucket # add collection bucket name
        collection_dataset_bucket_name = kwargs["conf"].get(section="custom", key="collection_dataset_bucket_name")
        ti.xcom_push(key="collection-dataset-bucket-name", value=collection_dataset_bucket_name)

        # push collection-task log variables
        push_log_variables(ti, task_definition_name=task_definition_name, container_name=task_definition_name, prefix="collection-task")
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
    priority = ["tree-preservation-order", "transport-access-node", "flood-risk-zone", "listed-building", "conservation-area"]

    def sort_key(item):
        key, value = item
        if key in priority:
            return (0, priority.index(key))  # Priority group
        return (1, value)

    sorted_collections = dict(sorted(collections_dict.items(), key=sort_key))
    return sorted_collections
