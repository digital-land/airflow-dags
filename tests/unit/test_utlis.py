from dags.utils import sort_collections_dict


def test_sort_collections_dict_moves_priority_keys_to_front():
    collections_dict = {"flood-risk-zone": ["dataset1", "dataset2"], "collection_b": ["dataset3"], "collection_c": ["dataset4", "dataset5", "dataset6"]}
    sorted_dict = sort_collections_dict(collections_dict)
    key_list = list(sorted_dict.keys())
    assert key_list[0] == "flood-risk-zone"
