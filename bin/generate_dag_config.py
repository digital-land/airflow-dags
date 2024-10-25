import json
import os
import sys
from pathlib import Path

import click

# Allows read of collection_schema within dags directory
sys.path.append(f"{os.getcwd()}")

from dags.collection_schema import Environments, CollectionSelection, ScheduledCollectionConfig

scheduled_collection_config = Environments(
    development=ScheduledCollectionConfig(
        selection=CollectionSelection.explicit,
        collections=[
            'ancient-woodland',
            'organisation'
        ],
        schedule='0 0 * * *',  # time is UTC
        max_active_tasks=50
    ),
    staging=ScheduledCollectionConfig(
        selection=CollectionSelection.explicit,
        collections=[
            'ancient-woodland',
            'organisation'
        ],
        schedule='0 0 * * *'
    ),
    production=ScheduledCollectionConfig(
        selection=CollectionSelection.explicit,
        collections=[
            'ancient-woodland'
        ],
        schedule='0 0 * * *'
    )
)


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
    env_collection_config = scheduled_collection_config.for_env(env)

    config_dict = {
        'env': env
    }

    # Only add 'schedule' if it exists and is not None
    if env_collection_config.schedule:
        config_dict['schedule'] = env_collection_config.schedule

    if env_collection_config.max_active_tasks:
        config_dict['max_active_tasks'] = env_collection_config.max_active_tasks

    config_dict['collection_selection'] = env_collection_config.selection
    config_dict['collections'] = env_collection_config.collections

    # with tempfile.TemporaryDirectory() as tmpdir:
    #     dataset_spec_url = 'https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv'
    #     dataset_spec_path = Path(tmpdir) / 'dataset.csv'
    #     urllib.request.urlretrieve(dataset_spec_url, dataset_spec_path)
    #
    #     collections_dict = {}
    #
    #     with open(dataset_spec_path, newline="") as f:
    #         dictreader = csv.DictReader(f)
    #         for row in dictreader:
    #             collection = row.get('collection', None)
    #
    #             dataset = row.get('dataset', None)
    #             if collection and dataset:
    #                 if config_dict.get(collection,None):
    #                     collections_dict[collection].append(dataset)
    #                 else:
    #                     collections_dict[collection] = [dataset]

        # config_dict['scheduled_collections'] = collections_dict
    with open(output_path, 'w') as f:
        json.dump(config_dict, f, indent=4)


if __name__ == "__main__":
    make_dag_config()