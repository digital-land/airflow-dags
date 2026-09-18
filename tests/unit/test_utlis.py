from unittest.mock import Mock

import pytest
import requests

from dags.utils import filter_collections_for_env, get_datasette_db_hash, is_dataset_available, sort_collections_dict


def test_sort_collections_dict_moves_priority_keys_to_front():
    collections_dict = {"flood-risk-zone": ["dataset1", "dataset2"], "collection_b": ["dataset3"], "collection_c": ["dataset4", "dataset5", "dataset6"]}
    sorted_dict = sort_collections_dict(collections_dict)
    key_list = list(sorted_dict.keys())
    assert key_list[0] == "flood-risk-zone"


@pytest.mark.parametrize(
    "environment,env,expected",
    [
        ("production", "development", True),
        ("production", "staging", True),
        ("production", "production", True),
        ("staging", "development", True),
        ("staging", "staging", True),
        ("staging", "production", False),
        ("development", "development", True),
        ("development", "staging", False),
        ("development", "production", False),
        ("", "development", False),
        ("", "staging", False),
        ("", "production", False),
    ],
)
def test_is_dataset_available(environment, env, expected):
    dataset = {"environment": environment}
    assert is_dataset_available(dataset, env) == expected


def test_is_dataset_available_treats_missing_environment_as_unavailable():
    assert is_dataset_available({}, "development") is False


def test_filter_collections_for_env_drops_collections_with_no_available_datasets():
    collections_dict = {"future-collection": ["future-dataset"]}
    datasets_dict = {"future-dataset": {"environment": "development"}}

    assert filter_collections_for_env(collections_dict, datasets_dict, "staging") == {}


def test_filter_collections_for_env_keeps_collection_when_any_dataset_available():
    collections_dict = {"organisation": ["local-authority", "new-dataset"]}
    datasets_dict = {
        "local-authority": {"environment": "production"},
        "new-dataset": {"environment": "development"},
    }

    # collection DAG still exists in staging because local-authority is available there,
    # but only local-authority's task group is included
    assert filter_collections_for_env(collections_dict, datasets_dict, "staging") == {"organisation": ["local-authority"]}


def test_get_datasette_db_hash_returns_hash_for_matching_db(monkeypatch):
    resp = Mock(raise_for_status=Mock())
    resp.json = Mock(return_value=[{"name": "digital-land", "hash": "abc"}, {"name": "performance", "hash": "def"}])
    monkeypatch.setattr("requests.get", Mock(return_value=resp))
    assert get_datasette_db_hash("digital-land") == "abc"


def test_get_datasette_db_hash_returns_none_on_error(monkeypatch):
    monkeypatch.setattr("requests.get", Mock(side_effect=requests.RequestException))
    assert get_datasette_db_hash("digital-land") is None
