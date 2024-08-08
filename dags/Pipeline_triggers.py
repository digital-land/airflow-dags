"""
A dag of dags which is responsible for triggering individual dags in the correct order each night
Progress of the individual collection dags can be checked in each of the dags generated in
collection_generator.py
"""
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="trigger-collection-dags",
    description=f"A master Dag which  runs all the pocessing we need each need to process all datasets and packages",
    schedule=None,
 ):
    run_org_dag = TriggerDagRunOperator(
        trigger_dag_id = f'organisation-collection',
        wait_for_completion = True
    )
    run_ancient_woodland_dag = TriggerDagRunOperator(
        trigger_dag_id = f'ancient-woodland-collection',
        wait_for_completion = True
    )
    # run_title_boundary_dag = TriggerDagRunOperator(
    #     trigger_dag_id = f'title-boundary-collection',
    #     wait_for_completion = True
    # )

    run_org_dag >> run_ancient_woodland_dag
    # run_org_dag >> run_title_boundary_dag

