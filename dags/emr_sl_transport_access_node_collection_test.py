from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import time
import json

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

def wait_for_emr_job_completion(**context):
    """Wait for EMR Serverless job to complete"""
    job_run_id = context['task_instance'].xcom_pull(task_ids='extract_job_id', key='job_run_id')
    
    if not job_run_id:
        raise ValueError("No job_run_id found from previous task")
    
    emr_client = boto3.client('emr-serverless', region_name='eu-west-2')
    application_id = Variable.get("emr_application_id")
    
    print(f"Monitoring EMR Serverless job: {job_run_id}")
    
    # Set timeout (5 minutes = 300 seconds)
    timeout_seconds = 300
    start_time = time.time()
    
    while True:
        # Check if we've exceeded timeout
        if time.time() - start_time > timeout_seconds:
            raise Exception(f"Job monitoring timed out after {timeout_seconds} seconds")
        
        try:
            response = emr_client.get_job_run(
                applicationId=application_id,
                jobRunId=job_run_id
            )
            
            job_state = response['jobRun']['state']
            print(f"Job state: {job_state}")
            
            if job_state == 'SUCCESS':
                print("Job completed successfully!")
                return job_run_id
            elif job_state == 'FAILED':
                print("Job failed - stopping DAG execution")
                raise Exception(f"EMR job failed with state: {job_state}")
            elif job_state in ['CANCELLED', 'CANCELLING']:
                print("Job was cancelled - stopping DAG execution")
                raise Exception(f"EMR job cancelled with state: {job_state}")
            elif job_state in ['PENDING', 'SCHEDULED', 'RUNNING', 'SUBMITTED']:
                print(f"Job still running, waiting 30 seconds...")
                time.sleep(30)
            else:
                print(f"Unknown job state: {job_state}, continuing to wait...")
                time.sleep(30)
                
        except Exception as e:
            if "timed out" in str(e) or "failed" in str(e) or "cancelled" in str(e):
                raise
            print(f"Error checking job status: {e}, retrying in 30 seconds...")
            time.sleep(30)

# Define the DAG to run EMR Serverless PySpark job
with DAG(
    dag_id='emr_sl_transport_access_node_collection_test',
    default_args=default_args,
    description='Run transport-access-node PySpark job on EMR Serverless',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
) as dag:

    # EMR Serverless configuration from Airflow Variables (set via UI)
    EMR_APPLICATION_ID = Variable.get("emr_application_id")
    EXECUTION_ROLE_ARN = Variable.get("emr_execution_role_secret")
    S3_BUCKET = Variable.get("s3_pyspark_jobs_codepackage", default_var="development-pyspark-jobs-codepackage")
    
    # Dynamic job parameters from Airflow Variables
    LOAD_TYPE = Variable.get("load_type", default_var="full")
    LOAD_TYPE="sample"
    DATA_SET = Variable.get("data_set", default_var="transport-access-node")
    ENV = Variable.get("env", default_var="dev") # dev, staging, prod
    S3_SOURCE_DATA_PATH = Variable.get("source_data_path", default_var="") # dev, staging, prod
    # Construct S3 paths
    S3_ENTRY_POINT = f"s3://{S3_BUCKET}/pkg/entry_script/run_main.py"
    S3_WHEEL_FILE = f"s3://{S3_BUCKET}/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
    S3_LOG_URI = f"s3://{S3_BUCKET}/logs/"
    S3_DEPENDENCIES_PATH = f"s3://{S3_BUCKET}/pkg/dependencies/dependencies.tar.gz"
    # Fix: Remove the "/data/" part from the path
    S3_DATA_PATH = f"s3://{S3_SOURCE_DATA_PATH}/"  # Changed from f"s3://{S3_BUCKET}/data/"

    # Task 1: Submit EMR Serverless job and capture job run ID
    submit_emr_job = BashOperator(
        task_id='submit_emr_job',
        bash_command=f'''
        JOB_OUTPUT=$(aws emr-serverless start-job-run \\
          --name "{DATA_SET}-job" \\
          --application-id {EMR_APPLICATION_ID} \\
          --execution-role-arn {EXECUTION_ROLE_ARN} \\
          --job-driver '{{
            "sparkSubmit": {{
              "entryPoint": "{S3_ENTRY_POINT}",
              "entryPointArguments": ["--load_type", "{LOAD_TYPE}", "--data_set", "{DATA_SET}", "--path", "{S3_DATA_PATH}", "--env", "{ENV}"],
              "sparkSubmitParameters": "--py-files {S3_WHEEL_FILE},{S3_DEPENDENCIES_PATH}"
            }}
          }}' \\
          --configuration-overrides '{{
            "monitoringConfiguration": {{
              "s3MonitoringConfiguration": {{
                "logUri": "{S3_LOG_URI}"
              }}
            }}
          }}' --output json)
        
        echo "EMR Job submitted successfully"
        echo "Job Output: $JOB_OUTPUT"
        
        # Extract job run ID
        JOB_RUN_ID=$(echo $JOB_OUTPUT | jq -r '.jobRunId')
        echo "Job Run ID: $JOB_RUN_ID"
        echo "$JOB_RUN_ID" > /tmp/job_run_id.txt
        ''',
        do_xcom_push=False
    )

    # Python task to extract job run ID and push to XCom
    extract_job_id = PythonOperator(
        task_id='extract_job_id',
        python_callable=lambda **context: context['task_instance'].xcom_push(
            key='job_run_id', 
            value=open('/tmp/job_run_id.txt').read().strip()
        )
    )

    # Task 2: Wait for EMR job completion
    wait_for_completion = PythonOperator(
        task_id='wait_for_emr_completion',
        python_callable=wait_for_emr_job_completion
    )

    # Set task dependencies
    submit_emr_job >> extract_job_id >> wait_for_completion