import csv
import json
import tempfile
import urllib.request
from pathlib import Path
from typing import List

import click

from collection_schema import CollectionConfig, CollectionSelection

collection_config = CollectionConfig(
    development=[
        'ancient-woodland',
        'organisation',
        'title-boundary',
        'article-4-direction',
        'central-activities-zone'
    ],
    staging=CollectionSelection.all,
    production=CollectionSelection.none
)


def collection_enabled(collection, env):
    env_collection_config = collection_config.for_env(env)
    return env_collection_config == CollectionSelection.all or collection in env_collection_config


@click.command()
@click.option(
    "--output-path",
    type=click.Path(),
    default=Path("dags/config.json"),
    help="Path to where the json configuration file should go",
)
@click.option(
    "--env",
    type=str,
    default="development",
    help="environment that the json is being created for. If development then a subset of collections are used",
)
def make_dag_config(output_path: Path, env: str):
    config_dict = {'env': env}

    with tempfile.TemporaryDirectory() as tmpdir:
        dataset_spec_url = 'https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv'
        spec_dataset_path = Path(tmpdir) / 'dataset.csv'
        urllib.request.urlretrieve(dataset_spec_url, Path(tmpdir) / 'dataset.csv')

        collections_dict = {}

        with open(spec_dataset_path, newline="") as f:
            dictreader = csv.DictReader(f)
            for row in dictreader:
                collection = row.get('collection', None)

                if collection_enabled(collection, env):
                    dataset = row.get('dataset', None)
                    if collection and dataset:
                        if config_dict.get(collection,None):
                            collections_dict[collection].append(dataset)
                        else:
                            collections_dict[collection] = [dataset]

        config_dict['collections'] = collections_dict
        with open(output_path, 'w') as f:
            json.dump(config_dict, f, indent=4)


if __name__ == "__main__":
    make_dag_config()