import csv
import json
import os
import tempfile
import urllib
from pathlib import Path

import boto3
import logging


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


def get_task_log_config(ecs_client,task_definition_family):
    """
    returns the log configuration of a task definition stored in aws
    assumes the local environment is set up to access aws
    """
    
    # Describe the task definition
    response = ecs_client.describe_task_definition(taskDefinition=task_definition_family)
    
    # Extract the log configuration from the container definitions
    log_config = response['taskDefinition']['containerDefinitions'][0].get('logConfiguration',{})
    
    return log_config