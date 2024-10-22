"""
A dag of dags which is responsible for triggering individual dags in the correct order each night
Progress of the individual collection dags can be checked in each of the dags generated in
collection_generator.py
"""
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
        max_active_tasks=dag_max_active_tasks,
        is_paused_upon_creation=False
):
    organisation_collection_selected = collection_selected('organisation', config)

    if organisation_collection_selected:
        run_org_dag = TriggerDagRunOperator(
            task_id='trigger-organisation-collection-dag',
            trigger_dag_id=f'organisation-collection'
        )

    collections = load_specification_datasets()

    for collection, datasets in collections.items():
        if collection not in ['organisation']:
            if collection_selected(collection, config):
                collection_dag = TriggerDagRunOperator(
                    task_id=f'trigger-{collection}-collection-dag',
                    trigger_dag_id=f'{collection}-collection'
                )
                if organisation_collection_selected:
                    run_org_dag >> collection_dag


with DAG(
        dag_id="trigger-collection-dags-manual",
        description=f"A master DAG which runs all collection DAGs on a manual basis",
        catchup=False,
        max_active_tasks=dag_max_active_tasks,
        is_paused_upon_creation=False
):

    run_org_dag = TriggerDagRunOperator(
        task_id='trigger-organisation-collection-dag',
        trigger_dag_id=f'organisation-collection'
    )

    collections = load_specification_datasets()

    for collection, datasets in collections.items():
        if collection not in ['organisation']:

            collection_dag = TriggerDagRunOperator(
                task_id=f'trigger-{collection}-collection-dag',
                trigger_dag_id=f'{collection}-collection'
            )

            run_org_dag >> collection_dag
