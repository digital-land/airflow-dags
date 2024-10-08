"""
A dag of dags which is responsible for triggering individual dags in the correct order each night
Progress of the individual collection dags can be checked in each of the dags generated in
collection_generator.py
"""
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import os
import json

from datetime import datetime, timedelta

my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "config.json")

with open(configuration_file_path) as file:
    config = json.load(file)

dag_schedule = config.get("schedule", None)  # Use "None" as a fallback if "schedule" key is missing

with DAG(
        dag_id="trigger-collection-dags",
        description=f"A master DAG which runs all the processing we need each need to process all datasets and packages",
        schedule=dag_schedule,
        start_date=datetime(2024, 1, 1),
        catchup=False,
):
    run_org_dag = TriggerDagRunOperator(
        task_id='trigger-organisation-collection-dag',
        trigger_dag_id=f'organisation-collection',
        wait_for_completion=True
    )

    for collection, datasets in config['collections'].items():
        if collection not in ['organisation', 'title-boundary']:
            collection_dag = TriggerDagRunOperator(
                task_id=f'trigger-{collection}-collection-dag',
                trigger_dag_id=f'{collection}-collection',
                wait_for_completion=True
            )

            run_org_dag >> collection_dag
