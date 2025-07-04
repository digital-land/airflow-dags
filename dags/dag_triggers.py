"""
A dag of dags which is responsible for triggering individual dags in the correct order each night
Progress of the individual collection dags can be checked in each of the dags generated in
collection_generator.py
"""
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta

from utils import get_config, load_specification_datasets

from collection_schema import CollectionSelection

config = get_config()
dag_schedule = config.get("schedule", None)  # Use "None" as a fallback if "schedule" key is missing
dag_max_active_tasks = config.get("max_active_tasks")


def collection_selected(collection_name, configuration):
    return (configuration['collection_selection'] == CollectionSelection.all
            or (configuration['collection_selection'] == CollectionSelection.explicit
                and collection_name in configuration['collections'])
            )


with DAG(
        dag_id="trigger-collection-dags-scheduled",
        description=f"A master DAG which runs all selected collection DAGs on a scheduled basis",
        schedule=dag_schedule,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_tasks=7,
        is_paused_upon_creation=False
):
    organisation_collection_selected = collection_selected('organisation', config)
    collection_tasks = []

    if organisation_collection_selected:
        run_org_collection_dag = TriggerDagRunOperator(
            task_id='trigger-organisation-collection-dag',
            trigger_dag_id=f'organisation-collection',
            wait_for_completion=True
        )
        run_org_builder_dag = TriggerDagRunOperator(
            task_id='trigger-organisation-builder-dag',
            trigger_dag_id=f'organisation-builder',
            wait_for_completion=True,
            trigger_rule=TriggerRule.ALL_DONE
        )
        run_org_collection_dag >> run_org_builder_dag

    collections = load_specification_datasets()

    for collection, datasets in collections.items():
        if collection not in ['organisation','document','title-boundary']:
            if collection_selected(collection, config):
                collection_dag = TriggerDagRunOperator(
                    task_id=f'trigger-{collection}-collection-dag',
                    trigger_dag_id=f'{collection}-collection',
                    wait_for_completion=True,
                    trigger_rule=TriggerRule.ALL_DONE
                )
                collection_tasks.append(collection_dag)
                if organisation_collection_selected:
                    run_org_builder_dag >> collection_dag

    
    dlb_dag = TriggerDagRunOperator(
                    task_id='trigger-digital-land-builder-dag',
                    trigger_dag_id='build-digital-land-builder',
                    wait_for_completion=True,
                    trigger_rule=TriggerRule.ALL_DONE
                )
    for task in collection_tasks:
        task >> dlb_dag

with DAG(
        dag_id="trigger-collection-dags-manual",
        description=f"A master DAG which runs all collection DAGs on a manual basis",
        schedule=None,
        catchup=False,
        # limited as a lot of tasks won't complete until new tasks are spawned
        # TODO could utilise pools to stop these dags clogging the system
        max_active_tasks=7,
        is_paused_upon_creation=False
):

    collection_tasks = []
    run_org_collection_dag = TriggerDagRunOperator(
        task_id='trigger-organisation-collection-dag',
        trigger_dag_id=f'organisation-collection',
        wait_for_completion=True
    )

    run_org_builder_dag = TriggerDagRunOperator(
        task_id='trigger-organisation-builder-dag',
        trigger_dag_id=f'organisation-builder',
        wait_for_completion=True
    )

    run_org_collection_dag >> run_org_builder_dag

    collections = load_specification_datasets()

    for collection, datasets in collections.items():
        if collection not in ['organisation','document','title-boundary']:

            collection_dag = TriggerDagRunOperator(
                task_id=f'trigger-{collection}-collection-dag',
                trigger_dag_id=f'{collection}-collection',
                wait_for_completion=True
            )
            collection_tasks.append(collection_dag)

            run_org_builder_dag >> collection_dag
    
    dlb_dag = TriggerDagRunOperator(
                    task_id='trigger-digital-land-builder-dag',
                    trigger_dag_id='build-digital-land-builder',
                    wait_for_completion=True,
                    trigger_rule=TriggerRule.ALL_DONE
                )
    
    for task in collection_tasks:
        task >> dlb_dag
