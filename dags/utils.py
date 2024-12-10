import csv
import json
import os
import tempfile
import urllib
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.slack.notifications.slack import send_slack_notification

import boto3
import logging

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


def load_specification_datasets():
    with tempfile.TemporaryDirectory() as tmpdir:

        dataset_spec_url = 'https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv'
        dataset_spec_path = Path(tmpdir) / 'dataset.csv'
        urllib.request.urlretrieve(dataset_spec_url, dataset_spec_path)

        collections_dict = {}

        with open(dataset_spec_path, newline="") as f:
            dictreader = csv.DictReader(f)
            for row in dictreader:
                collection = row.get('collection', None)

                dataset = row.get('dataset', None)
                if collection and dataset:
                    if collection in collections_dict:
                        collections_dict[collection].append(dataset)
                    else:
                        collections_dict[collection] = [dataset]

        return collections_dict


def get_task_log_config(ecs_client, task_definition_family):
    """
    returns the log configuration of a task definition stored in aws
    assumes the local environment is set up to access aws
    """
    
    # Describe the task definition
    response = ecs_client.describe_task_definition(taskDefinition=task_definition_family)
    
    # Extract the log configuration from the container definitions
    log_config = response['taskDefinition']['containerDefinitions'][0].get('logConfiguration', {})
    
    return log_config


def setup_configure_dag_callable(config, task_definition_name):
    def configure_dag(**kwargs):
        """
        function which returns the relevant configuration details
        and stores them in xcoms for other tasks. this includes:
        - get and process params into correct formats
        - read in env variables
        - access options defined in the task definitions
        """
        aws_vpc_config = {
            "subnets": kwargs['conf'].get(section='custom', key='ecs_task_subnets').split(","),
            "securityGroups": kwargs['conf'].get(section='custom', key='ecs_task_security_groups').split(","),
            "assignPublicIp": "ENABLED",
        }

        # retrieve and process parameters
        params = kwargs['params']

        memory = int(params.get('memory'))
        cpu = int(params.get('cpu'))
        transformed_jobs = str(kwargs['params'].get('transformed-jobs'))
        dataset_jobs = str(kwargs['params'].get('dataset-jobs'))
        incremental_loading_override = bool(kwargs['params'].get('incremental-loading-override'))

        # get ecs-task logging configuration
        ecs_client = boto3.client('ecs')
        collection_task_log_config = get_task_log_config(ecs_client, task_definition_name)
        collection_task_log_config_options = collection_task_log_config['options']
        collection_task_log_group = str(collection_task_log_config_options.get('awslogs-group'))
        # add container name to prefix
        collection_task_log_stream_prefix = (str(collection_task_log_config_options.get('awslogs-stream-prefix'))
                                             + f'/{task_definition_name}')
        collection_task_log_region = str(collection_task_log_config_options.get('awslogs-region'))
        collection_dataset_bucket_name = kwargs['conf'].get(section='custom', key='collection_dataset_bucket_name')

        # Push values to XCom
        ti = kwargs['ti']
        ti.xcom_push(key='env', value=config['env'])
        ti.xcom_push(key='aws_vpc_config', value=aws_vpc_config)
        ti.xcom_push(key='memory', value=memory)
        ti.xcom_push(key='cpu', value=cpu)
        ti.xcom_push(key='transformed-jobs', value=transformed_jobs)
        ti.xcom_push(key='dataset-jobs', value=dataset_jobs)
        ti.xcom_push(key='collection-task-log-group', value=collection_task_log_group)
        ti.xcom_push(key='collection-task-log-stream-prefix', value=collection_task_log_stream_prefix)
        ti.xcom_push(key='collection-task-log-region', value=collection_task_log_region)
        ti.xcom_push(key='collection-dataset-bucket-name', value=collection_dataset_bucket_name)
        ti.xcom_push(key='incremental-loading-override', value=incremental_loading_override)

    return configure_dag
