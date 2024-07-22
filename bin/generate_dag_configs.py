import tempfile
import urllib.request
import csv
import json

from pathlib import Path

config_dict = {"article-4-direction": ["dataset-1", "dataset-2"]}

config_dict = {}
with tempfile.TemporaryDirectory() as tmpdir:
    spec_dataset_path = Path(tmpdir) / "dataset.csv"
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv",
        Path(tmpdir) / "dataset.csv",
    )

    with open(spec_dataset_path, newline="") as f:
        dictreader = csv.DictReader(f)
        for row in dictreader:
            collection = row.get("collection", None)
            dataset = row.get("dataset", None)
            if collection and dataset:
                if config_dict.get(collection, None):
                    config_dict[collection].append(dataset)
                else:
                    config_dict[collection] = [dataset]

with open("dags/config.json", "w") as f:
    json.dump(config_dict, f, indent=4)
