import csv
import json
import tempfile
import urllib.request
from pathlib import Path

import click

from collection_schema import Environments, CollectionSelection, CollectionConfig

collection_config = Environments(
    development=CollectionConfig(
        selection=CollectionSelection.explicit,
        collections=[
            'ancient-woodland',
            'organisation',
            'title-boundary',
            'article-4-direction',
            'central-activities-zone'
        ]
        # schedule='0 10 * * *'  # Daily at 10 AM
    ),
    staging=CollectionConfig(
        selection=CollectionSelection.all
    ),
    production=CollectionConfig(
        selection=CollectionSelection.none
    )
)


def collection_enabled(collection, env):
    env_collection_config = collection_config.for_env(env)
    return (env_collection_config.selection == CollectionSelection.all
            or collection in env_collection_config.collections)


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
    env_collection_config = collection_config.for_env(env)

    config_dict = {
        'env': env,
        'schedule': env_collection_config.schedule   
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        dataset_spec_url = 'https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv'
        dataset_spec_path = Path(tmpdir) / 'dataset.csv'
        urllib.request.urlretrieve(dataset_spec_url, dataset_spec_path)

        collections_dict = {}

        with open(dataset_spec_path, newline="") as f:
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