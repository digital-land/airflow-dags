from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
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

# Define the DAG to run EMR Serverless PySpark job
with DAG(
    dag_id='emr_serverless_transport_access_node_dag',
    default_args=default_args,
    description='Run transport-access-node PySpark job on EMR Serverless',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
) as dag:

    # EMR Serverless configuration from Airflow Variables (set via UI)
    EMR_APPLICATION_ID = Variable.get("emr_application_id")
    EXECUTION_ROLE_ARN = Variable.get("emr_execution_role_arn")
    S3_BUCKET = Variable.get("s3_data_bucket", default_var="development-collection-data")
    
    # Dynamic job parameters from Airflow Variables
    LOAD_TYPE = Variable.get("load_type", default_var="full")
    DATA_SET = Variable.get("data_set", default_var="transport-access-node")
    
    # Construct S3 paths
    S3_ENTRY_POINT = f"s3://{S3_BUCKET}/emr-data-processing/src0/entry_script/run_main.py"
    S3_WHEEL_FILE = f"s3://{S3_BUCKET}/emr-data-processing/src0/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
    S3_LOG_URI = f"s3://{S3_BUCKET}/emr-data-processing/logs/"
    S3_DATA_PATH = f"s3://{S3_BUCKET}/"

    # Task to run EMR Serverless PySpark job
    run_transport_access_node_job = BashOperator(
        task_id='run_transport_access_node_job',
        bash_command=f'''aws emr-serverless start-job-run \\
  --name "{DATA_SET}-job" \\
  --application-id {EMR_APPLICATION_ID} \\
  --execution-role-arn {EXECUTION_ROLE_ARN} \\
  --job-driver '{{
    "sparkSubmit": {{
      "entryPoint": "{S3_ENTRY_POINT}",
      "entryPointArguments": ["--load_type", "{LOAD_TYPE}", "--data_set", "{DATA_SET}", "--path", "{S3_DATA_PATH}"],
      "sparkSubmitParameters": "--py-files {S3_WHEEL_FILE}"
    }}
  }}' \\
  --configuration-overrides '{{
    "monitoringConfiguration": {{
      "s3MonitoringConfiguration": {{
        "logUri": "{S3_LOG_URI}"
      }}
    }}
  }}' ''',
    )

    run_transport_access_node_job