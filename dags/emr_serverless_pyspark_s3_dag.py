from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG to run only manually
with DAG(
    dag_id='s3_script_execution_dag',
    default_args=default_args,
    description='Run scripts from S3 location',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
) as dag:

    S3_SCRIPT_LOCATION = "s3://development-collection-data/emr-data-processing/src/"

    # Example task: list scripts in the S3 bucket
    list_s3_scripts = BashOperator(
        task_id='list_s3_scripts',
        bash_command=f'aws s3 ls {S3_SCRIPT_LOCATION}',
    )

    list_s3_scripts