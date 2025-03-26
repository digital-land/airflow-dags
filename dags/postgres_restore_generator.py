from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        "postgres-restore",
        description=f"Postgres restore job",
        schedule=None,
        catchup=False,
        is_paused_upon_creation=True,
) as dag:
    restore_postgres_task = BashOperator(
        task_id="postgres-restore",
        bash_command="scripts/postgres_restore.sh",
        dag=dag
    )