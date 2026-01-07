"""
A dag of dags which is responsible for triggering individual dags in the correct order each night
Progress of the individual collection dags can be checked in each of the dags generated in
collection_generator.py
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from collection_schema import CollectionSelection
from utils import get_collections_dict, get_config, load_specification_datasets, sort_collections_dict

config = get_config()
dag_schedule = config.get("schedule", None)  # Use "None" as a fallback if "schedule" key is missing
dag_max_active_tasks = config.get("max_active_tasks")

# we want to use weightings to pioritise Dags in the schedule below this constant can be altered to change the weighting applied
DEFAULT_WEIGHTING = 10
CUSTOM_COLLECTION_DAG_WEIGHTING = {"tree-preservation-order": 90, "transport-access-node": 90, "flood-risk-zone": 90, "listed-building": 100, "conservation-area": 90}


def collection_selected(collection_name, configuration):
    return configuration["collection_selection"] == CollectionSelection.all or (
        configuration["collection_selection"] == CollectionSelection.explicit and collection_name in configuration["collections"]
    )


datasets_dict = load_specification_datasets()
collections = sort_collections_dict(get_collections_dict(datasets_dict.values()))

with DAG(
    dag_id="trigger-collection-dags-scheduled",
    description="A master DAG which runs all selected collection DAGs on a scheduled basis",
    schedule=dag_schedule,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=12,
    is_paused_upon_creation=False,
):
    organisation_collection_selected = collection_selected("organisation", config)
    collection_tasks = []

    if organisation_collection_selected:
        run_org_collection_dag = TriggerDagRunOperator(task_id="trigger-organisation-collection-dag", trigger_dag_id="organisation-collection", wait_for_completion=True)
        run_org_builder_dag = TriggerDagRunOperator(
            task_id="trigger-organisation-builder-dag", trigger_dag_id="organisation-builder", wait_for_completion=True, trigger_rule=TriggerRule.ALL_DONE
        )
        run_org_collection_dag >> run_org_builder_dag

    for collection, datasets in collections.items():
        if collection not in ["organisation", "document", "title-boundary", "planning-application"]:

            if collection_selected(collection, config):
                # Set custom CPU for listed-building collection
                conf = {"cpu": 16384, "transformed-jobs": 16} if collection == "listed-building" else {}

                collection_dag = TriggerDagRunOperator(
                    task_id=f"trigger-{collection}-collection-dag",
                    trigger_dag_id=f"{collection}-collection",
                    wait_for_completion=True,
                    trigger_rule=TriggerRule.ALL_DONE,
                    priority_weight=CUSTOM_COLLECTION_DAG_WEIGHTING.get(collection, DEFAULT_WEIGHTING),
                    conf=conf,
                )
                collection_tasks.append(collection_dag)
                if organisation_collection_selected:
                    run_org_builder_dag >> collection_dag

    dlb_dag = TriggerDagRunOperator(
        task_id="trigger-digital-land-builder-dag", trigger_dag_id="build-digital-land-builder", wait_for_completion=True, trigger_rule=TriggerRule.ALL_DONE
    )
    for task in collection_tasks:
        task >> dlb_dag

with DAG(
    dag_id="trigger-collection-dags-manual",
    description="A master DAG which runs all collection DAGs on a manual basis",
    schedule=None,
    catchup=False,
    # limited as a lot of tasks won't complete until new tasks are spawned
    # TODO could utilise pools to stop these dags clogging the system
    max_active_tasks=10,
    is_paused_upon_creation=False,
):

    collection_tasks = []
    run_org_collection_dag = TriggerDagRunOperator(task_id="trigger-organisation-collection-dag", trigger_dag_id="organisation-collection", wait_for_completion=True)

    run_org_builder_dag = TriggerDagRunOperator(task_id="trigger-organisation-builder-dag", trigger_dag_id="organisation-builder", wait_for_completion=True)

    run_org_collection_dag >> run_org_builder_dag

    for collection, datasets in collections.items():
        if collection not in ["organisation", "document", "title-boundary", "planning-application"]:

            # Set custom CPU for listed-building collection
            conf = {"cpu": 16384, "transformed-jobs": 16} if collection == "listed-building" else {}

            collection_dag = TriggerDagRunOperator(task_id=f"trigger-{collection}-collection-dag", trigger_dag_id=f"{collection}-collection", wait_for_completion=True, conf=conf)
            collection_tasks.append(collection_dag)

            run_org_builder_dag >> collection_dag

    dlb_dag = TriggerDagRunOperator(
        task_id="trigger-digital-land-builder-dag", trigger_dag_id="build-digital-land-builder", wait_for_completion=True, trigger_rule=TriggerRule.ALL_DONE
    )

    for task in collection_tasks:
        task >> dlb_dag
