"""
A dag of dags which is responsible for triggering individual dags in the correct order each night
Progress of the individual collection dags can be checked in each of the dags generated in
collection_generator.py
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from collection_config import DEFAULT_COLLECTION_CONFIG, collection_schedule_matches, get_collection_dag_config
from collection_schema import CollectionSelection
from utils import filter_collections_for_env, get_collections_dict, get_config, load_specification_datasets, sort_collections_dict

# title-boundary runs via its newer, EMR-based pipeline; every other collection still runs via
# the original collection DAG
NEW_COLLECTION_DAG_COLLECTIONS = {"title-boundary"}

# collections triggered by a DAG of their own rather than from either master DAG. They run on a
# cadence of their own and nothing downstream consumes their output during the nightly run, so
# making the nightly chain wait on them only delays everything after them.
DECOUPLED_COLLECTIONS = {"title-boundary"}


def trigger_dag_id_for(collection: str) -> str:
    if collection in NEW_COLLECTION_DAG_COLLECTIONS:
        return f"new-{collection}-collection"
    return f"{collection}-collection"


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
collections = get_collections_dict(datasets_dict.values())
collections = filter_collections_for_env(collections, datasets_dict, config["env"])
collections = sort_collections_dict(collections)


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
        if collection not in ["organisation"] and collection not in DECOUPLED_COLLECTIONS:

            if collection_selected(collection, config):
                # Set custom CPU for listed-building collection
                conf = {"cpu": 16384, "transformed-jobs": 16} if collection in ("listed-building", "tree-preservation-order") else {}

                collection_dag = TriggerDagRunOperator(
                    task_id=f"trigger-{collection}-collection-dag",
                    trigger_dag_id=trigger_dag_id_for(collection),
                    wait_for_completion=True,
                    trigger_rule=TriggerRule.ALL_DONE,
                    priority_weight=CUSTOM_COLLECTION_DAG_WEIGHTING.get(collection, DEFAULT_WEIGHTING),
                    conf=conf,
                )

                # the scheduler runs every day; collections with a non-default schedule_rrule
                # (e.g. title-boundary, monthly) only actually get triggered on a matching day
                entry_task = collection_dag
                collection_dag_config = get_collection_dag_config(collection)
                if collection_dag_config.schedule_rrule != DEFAULT_COLLECTION_CONFIG.schedule_rrule:
                    check_schedule = ShortCircuitOperator(
                        task_id=f"check-{collection}-schedule",
                        python_callable=collection_schedule_matches,
                        op_kwargs={"collection": collection},
                        trigger_rule=TriggerRule.ALL_DONE,
                        ignore_downstream_trigger_rules=False,
                    )
                    check_schedule >> collection_dag
                    entry_task = check_schedule

                collection_tasks.append(collection_dag)
                if organisation_collection_selected:
                    run_org_builder_dag >> entry_task

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
        if collection not in ["organisation"] and collection not in DECOUPLED_COLLECTIONS:

            # Set custom CPU for listed-building collection
            conf = {"cpu": 16384, "transformed-jobs": 16} if collection in ("listed-building", "tree-preservation-order") else {}

            collection_dag = TriggerDagRunOperator(
                task_id=f"trigger-{collection}-collection-dag", trigger_dag_id=trigger_dag_id_for(collection), wait_for_completion=True, conf=conf
            )
            collection_tasks.append(collection_dag)

            run_org_builder_dag >> collection_dag

    dlb_dag = TriggerDagRunOperator(
        task_id="trigger-digital-land-builder-dag", trigger_dag_id="build-digital-land-builder", wait_for_completion=True, trigger_rule=TriggerRule.ALL_DONE
    )

    for task in collection_tasks:
        task >> dlb_dag


# title-boundary is decoupled from both master DAGs. It is a monthly job, and having the nightly
# chain block on a three-hour plus run once a month delayed everything behind it for no benefit -
# nothing downstream reads its output during that run.

if collection_selected("title-boundary", config) and "title-boundary" in collections:
    with DAG(
        dag_id="trigger-title-boundary-monthly",
        description="Triggers the title-boundary collection on its own monthly schedule, independently of the nightly run",
        # Midnight is load-bearing: do not move this off 00:00.
        #
        # The cron only gets us to "every Monday" - cron cannot intersect day-of-month with
        # day-of-week, so collection_schedule_matches is what picks which Monday. That check
        # compares the rrule's occurrences against data_interval_end, and every occurrence
        # expands at RRULE_SERIES_START's time of day, which is 00:00.
        schedule="0 0 * * 1",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        is_paused_upon_creation=False,
    ):
        check_schedule = ShortCircuitOperator(
            task_id="check-title-boundary-schedule",
            python_callable=collection_schedule_matches,
            op_kwargs={"collection": "title-boundary"},
        )
        # deliberately does not wait for completion: nothing depends on the outcome, and waiting
        # is precisely what this DAG exists to stop doing
        trigger_title_boundary = TriggerDagRunOperator(
            task_id="trigger-title-boundary-collection-dag",
            trigger_dag_id=trigger_dag_id_for("title-boundary"),
            wait_for_completion=False,
        )
        check_schedule >> trigger_title_boundary
