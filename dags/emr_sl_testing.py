from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import time
import json
import logging
from aws_secrets_manager import get_secret_emr_compatible

# Retrieve secrets from AWS Secrets Manager
def get_secrets(secret_name):    
    aws_secrets_json = get_secret_emr_compatible("development/deployment_variables")

    # Parse the JSON string
    secrets = json.loads(aws_secrets_json)   

    secret_value = secrets.get(secret_name)

    return secret_value    

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
    # Try to get job_run_id from XCom with better error handling
    job_run_id = context['task_instance'].xcom_pull(task_ids='extract_job_id', key='job_run_id')
    
    if not job_run_id:
        # Fallback: try to read from the temp file directly
        try:
            with open('/tmp/job_run_id.txt', 'r') as f:
                job_run_id = f.read().strip()
                print(f"Retrieved job_run_id from temp file: {job_run_id}")
        except FileNotFoundError:
            raise ValueError("No job_run_id found from XCom or temp file")
    
    if not job_run_id or job_run_id == 'None':
        raise ValueError(f"Invalid job_run_id retrieved: {job_run_id}")
    
    print(f"Using job_run_id: {job_run_id}")
    
    emr_client = boto3.client('emr-serverless', region_name='eu-west-2')
    # Use the application_id from the secrets (consistent with DAG setup)
    application_id = get_secrets("emr_application_id")
    
    print(f"Monitoring EMR Serverless job: {job_run_id}")
    
    # Set timeout (15 minutes = 900 seconds)
    timeout_seconds = 900
    start_time = time.time()
    
    try:
        while True:
            # Check if we've exceeded timeout
            if time.time() - start_time > timeout_seconds:
                print(f"Job monitoring timed out after {timeout_seconds} seconds")
                print(f"Attempting to cancel EMR Serverless job: {job_run_id}")
                
                try:
                    # Cancel the EMR Serverless job to prevent further costs
                    cancel_response = emr_client.cancel_job_run(
                        applicationId=application_id,
                        jobRunId=job_run_id
                    )
                    print(f"Job cancellation initiated: {cancel_response}")
                    
                    # Wait a bit for cancellation to take effect
                    time.sleep(10)
                    
                    # Check final state
                    final_response = emr_client.get_job_run(
                        applicationId=application_id,
                        jobRunId=job_run_id
                    )
                    final_state = final_response['jobRun']['state']
                    print(f"Final job state after cancellation: {final_state}")
                    
                except Exception as cancel_error:
                    print(f"Error cancelling job: {cancel_error}")
                
                raise Exception(f"Job monitoring timed out after {timeout_seconds} seconds. Job cancellation attempted.")
            
            try:
                response = emr_client.get_job_run(
                    applicationId=application_id,
                    jobRunId=job_run_id
                )
                
                job_state = response['jobRun']['state']
                print(f"Job state: {job_state}")
                
                if job_state == 'SUCCESS':
                    print("Job completed successfully!")
                    # Get final job details
                    final_details = response.get('jobRun', {})
                    print(f"Job completed in {time.time() - start_time:.2f} seconds")
                    if 'billedResourceUtilization' in final_details:
                        print(f"Resource utilization: {final_details['billedResourceUtilization']}")
                    return job_run_id
                elif job_state in ['FAILED', 'CANCELLED', 'CANCELLING']:
                    state_details = response.get('jobRun', {}).get('stateDetails', '')
                    failure_reason = response.get('jobRun', {}).get('failureReason', '')
                    
                    error_msg = f"EMR job {job_state}"
                    if state_details:
                        error_msg += f": {state_details}"
                    if failure_reason:
                        error_msg += f" (Reason: {failure_reason})"
                    
                    # Log additional debugging info
                    job_run_details = response.get('jobRun', {})
                    print(f"Job run details: {json.dumps(job_run_details, indent=2, default=str)}")
                    
                    raise Exception(error_msg)
                elif job_state in ['PENDING', 'SCHEDULED', 'RUNNING']:
                    elapsed = time.time() - start_time
                    print(f"Job still {job_state.lower()}, elapsed time: {elapsed:.2f}s, waiting 30 seconds...")
                    
                    # Log job progress if available
                    job_run_details = response.get('jobRun', {})
                    if 'jobProgress' in job_run_details:
                        print(f"Job progress: {job_run_details['jobProgress']}")
                    
                    time.sleep(30)
                else:
                    print(f"Unknown job state: {job_state}, continuing to wait...")
                    # Log the full response for debugging
                    print(f"Full EMR response: {json.dumps(response, indent=2, default=str)}")
                    time.sleep(30)
                    
            except Exception as e:
                if "timed out" in str(e):
                    raise
                print(f"Error checking job status: {e}, retrying in 30 seconds...")
                time.sleep(30)
                
    except Exception as outer_error:
        # Ensure we attempt to cancel the job if any unexpected error occurs
        if "timed out" not in str(outer_error):
            print(f"Unexpected error occurred: {outer_error}")
            print(f"Attempting to cancel EMR Serverless job: {job_run_id}")
            try:
                emr_client.cancel_job_run(
                    applicationId=application_id,
                    jobRunId=job_run_id
                )
                print("Job cancellation initiated due to unexpected error")
            except Exception as cancel_error:
                print(f"Error cancelling job due to unexpected error: {cancel_error}")
        raise

# Define the DAG to run EMR Serverless PySpark job
with DAG(
    dag_id='emr_sl_testing',
    default_args=default_args,
    description='Run transport-access-node PySpark job on EMR Serverless',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
) as dag:
    # EMR Serverless configuration from AWS Secrets Manager key value pairs
    ENV = get_secrets("environment") # development, staging, production
    EMR_APPLICATION_ID = get_secrets("emr_application_id")
    EXECUTION_ROLE_ARN = get_secrets("emr_execution_role")

    S3_BUCKET = f"{ENV}-pyspark-jobs-codepackage"
    
    LOAD_TYPE = get_secrets("load_type") #sample, full and delta
    
    DATA_SET = ("agricultural-land-classification")
    
    S3_SOURCE_DATA_PATH = f"{ENV}-collection-data"
    # S3 paths
    S3_ENTRY_POINT = f"s3://{S3_BUCKET}/pkg/entry_script/run_main.py"
    S3_WHEEL_FILE = f"s3://{S3_BUCKET}/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
    S3_LOG_URI = f"s3://{S3_BUCKET}/logs/"
    S3_DEPENDENCIES_PATH = f"s3://{S3_BUCKET}/pkg/dependencies/dependencies.zip"
    S3_POSTGRESQL_JAR = f"s3://{S3_BUCKET}/pkg/jars/postgresql-42.7.4.jar"  
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
          --name "{DATA_SET}-job-$(date +%Y%m%d-%H%M%S)" \\
          --application-id {EMR_APPLICATION_ID} \\
          --execution-role-arn {EXECUTION_ROLE_ARN} \\
          --job-driver '{{
            "sparkSubmit": {{
              "entryPoint": "{S3_ENTRY_POINT}",
              "entryPointArguments": ["--load_type", "{LOAD_TYPE}", "--data_set", "{DATA_SET}", "--path", "{S3_DATA_PATH}", "--env", "{ENV}"],
              "sparkSubmitParameters": "--py-files {S3_WHEEL_FILE},{S3_DEPENDENCIES_PATH} --jars {S3_POSTGRESQL_JAR}"
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
        do_xcom_push=False
    )

    # Python task to extract job run ID and push to XCom
    def extract_and_validate_job_id(**context):
        """Extract job run ID from temp file and validate it"""
        try:
            with open('/tmp/job_run_id.txt', 'r') as f:
                job_run_id = f.read().strip()
            
            if not job_run_id:
                raise ValueError("Empty job_run_id found in temp file")
            
            if job_run_id == 'null' or job_run_id == 'None':
                raise ValueError(f"Invalid job_run_id found: {job_run_id}")
            
            # Validate format (EMR job run IDs are typically alphanumeric with specific length)
            if len(job_run_id) < 10:
                raise ValueError(f"job_run_id appears to be invalid (too short): {job_run_id}")
            
            print(f"Successfully extracted job_run_id: {job_run_id}")
            
            # Push to XCom
            context['task_instance'].xcom_push(key='job_run_id', value=job_run_id)
            return job_run_id
            
        except FileNotFoundError:
            raise ValueError("Job ID temp file not found - EMR job submission may have failed")
        except Exception as e:
            raise ValueError(f"Failed to extract job_run_id: {str(e)}")

    extract_job_id = PythonOperator(
        task_id='extract_job_id',
        python_callable=extract_and_validate_job_id
    )

    # Task 2: Wait for EMR job completion
    wait_for_completion = PythonOperator(
        task_id='wait_for_emr_completion',
        python_callable=wait_for_emr_job_completion
    )

    # Set task dependencies
    submit_emr_job >> extract_job_id >> wait_for_completion