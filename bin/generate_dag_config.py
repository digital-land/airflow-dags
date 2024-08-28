import tempfile
import urllib.request
import csv
import json
import click

from pathlib import Path

# TODO define and get from specification
DEVELOPMENT_COLLECTIONS = [
    'ancient-woodland',
    'organisation',
    'title-boundary',
    'article-4-direction',
    'central-activities-zone'
]

STAGING_COLLECTIONS = DEVELOPMENT_COLLECTIONS

#TODO get from specification
PACKAGES = [
    'organisation'
]

DEFAULTS = {
    'cpu':1024,
    'memory':2048
}


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
def make_colection_config(output_path:Path,env: str):
    config_dict = {}
    config_dict['env'] = env
    collections_dict = {}
    if env == 'development':
        restricted_collections = DEVELOPMENT_COLLECTIONS
    elif env == 'staging':
        restricted_collections = STAGING_COLLECTIONS
    else:
        restricted_collections = None

    with tempfile.TemporaryDirectory() as tmpdir:
        spec_dataset_path = Path(tmpdir) / 'dataset.csv'
        urllib.request.urlretrieve('https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv',Path(tmpdir) / 'dataset.csv')

        with open(spec_dataset_path,newline="") as f:
            dictreader = csv.DictReader(f)
            for row in dictreader:
                collection = row.get('collection',None)
                if restricted_collections is None or collection in restricted_collections:
                    dataset = row.get('dataset',None)
                    if collection and dataset:
                        if config_dict.get(collection,None):
                            collections_dict[collection].append(dataset)
                        else:
                            collections_dict[collection] = [dataset]

        config_dict['collections'] = collections_dict
        config_dict['packages'] = PACKAGES
        with open(output_path,'w') as f:
            json.dump(config_dict,f,indent=4)

if __name__ == "__main__":
    make_colection_config()