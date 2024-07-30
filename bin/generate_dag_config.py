import tempfile
import urllib.request
import csv
import json
import click

from pathlib import Path

# variabkle to contain the local colections we want to set up limited 
DEVELOPMENT_COLLECTIONS = [
    'ancient-woodland'
]


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
    with tempfile.TemporaryDirectory() as tmpdir:
        spec_dataset_path = Path(tmpdir) / 'dataset.csv'
        urllib.request.urlretrieve('https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv',Path(tmpdir) / 'dataset.csv')

        with open(spec_dataset_path,newline="") as f:
            dictreader = csv.DictReader(f)
            for row in dictreader:
                collection = row.get('collection',None)
                if env != 'development' or collection in DEVELOPMENT_COLLECTIONS:
                    dataset = row.get('dataset',None)
                    if collection and dataset:
                        if config_dict.get(collection,None):
                            config_dict[collection].append(dataset)
                        else:
                            config_dict[collection] = [dataset]

        with open(output_path,'w') as f:
            json.dump(config_dict,f,indent=4)

if __name__ == "__main__":
    make_colection_config()