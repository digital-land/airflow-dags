from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from emr_dags_utils import get_datasets, get_secrets, extract_and_validate_job_id, wait_for_emr_job_completion
from datetime import datetime, timedelta
from aws_secrets_manager import get_secret_emr_compatible
import boto3
import time
import json
import logging
 
# List of datasets
datasets = get_datasets()

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

# DAG factory function
def create_dag(dag_id, dataset_name, schedule=None): #"0 17 * * *"
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        start_date=datetime(2025, 9, 24),
        catchup=False,
        tags=['dynamic'],
        dagrun_timeout=timedelta(minutes=60)  # Entire DAG must complete within 60 minutes
    ) as dag:
        # EMR Serverless configuration from AWS Secrets Manager key value pairs
        # TODO: need to refractor this method to make dynamic for different environments
        ENV = get_secrets("environment", "development") # development, staging, production
        EMR_APPLICATION_ID = get_secrets("emr_application_id",ENV)
        EXECUTION_ROLE_ARN = get_secrets("emr_execution_role",ENV)

        S3_BUCKET = f"{ENV}-pd-batch-jobs-codepackage-bucket"
        S3_LOG_BUCKET = f"{ENV}-pd-batch-jobs-logs-bucket"
        
        LOAD_TYPE = get_secrets("load_type",ENV) #sample, full and delta
        
        DATA_SET = dataset_name
        
        S3_SOURCE_DATA_PATH = f"{ENV}-collection-data"
        # S3 paths
        S3_ENTRY_POINT = f"s3://{S3_BUCKET}/pkg/entry_script/run_main.py"
        S3_WHEEL_FILE = f"s3://{S3_BUCKET}/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
        S3_LOG_URI = f"s3://{S3_LOG_BUCKET}/"
        S3_DEPENDENCIES_PATH = f"s3://{S3_BUCKET}/pkg/dependencies/dependencies.zip"
        S3_POSTGRESQL_JAR = f"s3://{S3_BUCKET}/pkg/jars/postgresql-42.7.4.jar"
        S3_SEDONA_JAR = f"s3://{S3_BUCKET}/pkg/jars/sedona-spark-shaded-3.5_2.12-1.8.0.jar"
        S3_SEDONA_GEOTOOLS_JAR = f"s3://{S3_BUCKET}/pkg/jars/geotools-wrapper-1.8.0-33.1.jar"  
        S3_DATA_PATH = f"s3://{S3_SOURCE_DATA_PATH}/"
        
        # Task 1: Submit EMR Serverless job and capture job run ID
        submit_emr_job = BashOperator(
            task_id='submit_emr_job',
            bash_command=f'''
            set -e  # Exit on any error
            
            echo "Starting EMR Serverless job submission..."
            echo "Application ID: {EMR_APPLICATION_ID}"
            echo "Dataset: {DATA_SET}"
            echo "Load Type: {LOAD_TYPE}"
            echo "Environment: {ENV}"
            
            # Submit EMR job and capture output
            JOB_OUTPUT=$(aws emr-serverless start-job-run \\
            --name "{DATA_SET}-job" \\
            --application-id {EMR_APPLICATION_ID} \\
            --execution-role-arn {EXECUTION_ROLE_ARN} \\
            --job-driver '{{
                "sparkSubmit": {{
                "entryPoint": "{S3_ENTRY_POINT}",
                "entryPointArguments": ["--load_type", "{LOAD_TYPE}", "--data_set", "{DATA_SET}", "--path", "{S3_DATA_PATH}", "--env", "{ENV}"],
                "sparkSubmitParameters": "--py-files {S3_WHEEL_FILE},{S3_DEPENDENCIES_PATH} \
                --jars {S3_POSTGRESQL_JAR},{S3_SEDONA_JAR},{S3_SEDONA_GEOTOOLS_JAR} \
                --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
                --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator \
                --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaSqlExtensions"
                }}
            }}' \\
            --configuration-overrides '{{
                "monitoringConfiguration": {{
                "s3MonitoringConfiguration": {{
                    "logUri": "{S3_LOG_URI}"
                }}
                }}
            }}' \\
            --region eu-west-2 \\
            --output json)
            
            # Check if job submission was successful
            if [ $? -ne 0 ]; then
            echo "ERROR: EMR job submission failed"
            exit 1
            fi
            
            echo "EMR Job submitted successfully"
            echo "Job Output: $JOB_OUTPUT"
            
            # Extract and validate job run ID
            JOB_RUN_ID=$(echo "$JOB_OUTPUT" | jq -r '.jobRunId')
            
            if [ "$JOB_RUN_ID" = "null" ] || [ -z "$JOB_RUN_ID" ]; then
            echo "ERROR: Failed to extract job run ID from output"
            echo "Job Output was: $JOB_OUTPUT"
            exit 1
            fi
            
            echo "Job Run ID: $JOB_RUN_ID"
            echo "$JOB_RUN_ID" > /tmp/job_run_id.txt
            
            # Verify the file was written correctly
            if [ ! -f /tmp/job_run_id.txt ]; then
            echo "ERROR: Failed to write job run ID to temp file"
            exit 1
            fi
            
            STORED_ID=$(cat /tmp/job_run_id.txt)
            if [ "$STORED_ID" != "$JOB_RUN_ID" ]; then
            echo "ERROR: Mismatch between extracted and stored job run ID"
            exit 1
            fi
            
            echo "Job submission completed successfully with ID: $JOB_RUN_ID"
            ''',
            do_xcom_push=False,
            execution_timeout=timedelta(minutes=5)  # Job submission should be quick
        )

        extract_job_id = PythonOperator(
            task_id='extract_job_id',
            python_callable=extract_and_validate_job_id,
            execution_timeout=timedelta(minutes=2)  # Job ID extraction should be very quick
        )

        # Task 2: Wait for EMR job completion
        wait_for_completion = PythonOperator(
            task_id='wait_for_emr_completion',
            python_callable=wait_for_emr_job_completion,
            retries=0,  # Don't retry if EMR job times out/fails
            execution_timeout=timedelta(minutes=53)  # 60 - 5 (submit) - 2 (extract) = 53 minutes
        )
        
        # Define dependencies
        submit_emr_job >> extract_job_id >> wait_for_completion
    return dag

# Generate DAGs dynamically
for dataset in datasets:
    dag_id = f"emr-{dataset}-collection"
    globals()[dag_id] = create_dag(dag_id, dataset)

  




 
 