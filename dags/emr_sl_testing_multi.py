from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime, timedelta
import boto3
import time
import json
import logging
import os
from aws_secrets_manager import get_secret_emr_compatible

# Retrieve secrets from AWS Secrets Manager
def get_secrets(secret_name):    
    aws_secrets_json = get_secret_emr_compatible("development/deployment_variables")

    # Parse the JSON string
    secrets = json.loads(aws_secrets_json)   

    secret_value = secrets.get(secret_name)

    return secret_value

# Retrieve datasets list from AWS Secrets Manager or Parameter Store
def get_datasets_list():
    """
    Retrieve the list of datasets to process from AWS Secrets Manager or Parameter Store.
    
    This function tries multiple sources in order:
    1. AWS Secrets Manager (more secure for sensitive data)
    2. AWS Parameter Store (good for configuration data)
    3. Environment variable fallback (for testing)
    
    Returns:
        list: List of dataset names to process
    """
    try:
        # Option 1: Try AWS Secrets Manager first
        try:
            datasets_json = get_secret_emr_compatible("development/emr_datasets_list")
            datasets = json.loads(datasets_json)
            
            # Validate it's a list
            if isinstance(datasets, list):
                print(f"Retrieved {len(datasets)} datasets from Secrets Manager")
                return datasets
            elif isinstance(datasets, dict) and 'datasets' in datasets:
                datasets_list = datasets['datasets']
                if isinstance(datasets_list, list):
                    print(f"Retrieved {len(datasets_list)} datasets from Secrets Manager")
                    return datasets_list
                    
        except Exception as e:
            print(f"Failed to retrieve from Secrets Manager: {e}")
            
        # Option 2: Try AWS Parameter Store
        try:
            ssm_client = boto3.client('ssm', region_name='eu-west-2')
            response = ssm_client.get_parameter(
                Name='/emr/datasets-list',
                WithDecryption=True  # Use SecureString parameter type
            )
            
            datasets_json = response['Parameter']['Value']
            datasets = json.loads(datasets_json)
            
            # Validate it's a list
            if isinstance(datasets, list):
                print(f"Retrieved {len(datasets)} datasets from Parameter Store")
                return datasets
            elif isinstance(datasets, dict) and 'datasets' in datasets:
                datasets_list = datasets['datasets']
                if isinstance(datasets_list, list):
                    print(f"Retrieved {len(datasets_list)} datasets from Parameter Store")
                    return datasets_list
                    
        except Exception as e:
            print(f"Failed to retrieve from Parameter Store: {e}")
            
        # Option 3: Environment variable fallback (for testing)
        env_datasets = os.getenv('EMR_DATASETS_LIST')
        if env_datasets:
            datasets = json.loads(env_datasets)
            if isinstance(datasets, list):
                print(f"Retrieved {len(datasets)} datasets from environment variable")
                return datasets
                
        # Option 4: Default fallback
        print("Using default dataset list")
        return ["ancient-woodland"]  # Default to current dataset
        
    except Exception as e:
        print(f"Error retrieving datasets list: {e}")
        # Fallback to current single dataset
        return ["ancient-woodland"]

def submit_emr_job_for_dataset(dataset_name: str):
    """
    Submit EMR Serverless job for a specific dataset.
    
    Args:
        dataset_name (str): Name of the dataset to process
        
    Returns:
        str: Job run ID
    """
    # Get configuration from secrets
    ENV = get_secrets("environment") 
    EMR_APPLICATION_ID = get_secrets("emr_application_id")
    EXECUTION_ROLE_ARN = get_secrets("emr_execution_role")
    LOAD_TYPE = get_secrets("load_type")
    
    S3_BUCKET = f"{ENV}-pyspark-jobs-codepackage"
    S3_LOG_BUCKET = f"{ENV}-pyspark-jobs-logs"
    S3_SOURCE_DATA_PATH = f"{ENV}-collection-data"
    
    # S3 paths
    S3_ENTRY_POINT = f"s3://{S3_BUCKET}/pkg/entry_script/run_main.py"
    S3_WHEEL_FILE = f"s3://{S3_BUCKET}/pkg/whl_pkg/pyspark_jobs-0.1.0-py3-none-any.whl"
    S3_LOG_URI = f"s3://{S3_LOG_BUCKET}/"
    S3_DEPENDENCIES_PATH = f"s3://{S3_BUCKET}/pkg/dependencies/dependencies.zip"
    S3_POSTGRESQL_JAR = f"s3://{S3_BUCKET}/pkg/jars/postgresql-42.7.4.jar"  
    S3_DATA_PATH = f"s3://{S3_SOURCE_DATA_PATH}/"
    
    print(f"Submitting EMR job for dataset: {dataset_name}")
    print(f"Application ID: {EMR_APPLICATION_ID}")
    print(f"Load Type: {LOAD_TYPE}")
    print(f"Environment: {ENV}")
    
    emr_client = boto3.client('emr-serverless', region_name='eu-west-2')
    
    try:
        response = emr_client.start_job_run(
            name=f"{dataset_name}-job-{int(time.time())}",
            applicationId=EMR_APPLICATION_ID,
            executionRoleArn=EXECUTION_ROLE_ARN,
            jobDriver={
                'sparkSubmit': {
                    'entryPoint': S3_ENTRY_POINT,
                    'entryPointArguments': [
                        "--load_type", LOAD_TYPE,
                        "--data_set", dataset_name,
                        "--path", S3_DATA_PATH,
                        "--env", ENV
                    ],
                    'sparkSubmitParameters': f"--py-files {S3_WHEEL_FILE},{S3_DEPENDENCIES_PATH} --jars {S3_POSTGRESQL_JAR}"
                }
            },
            configurationOverrides={
                'monitoringConfiguration': {
                    's3MonitoringConfiguration': {
                        'logUri': S3_LOG_URI
                    }
                }
            }
        )
        
        job_run_id = response['jobRunId']
        print(f"Successfully submitted EMR job for {dataset_name} with ID: {job_run_id}")
        
        return job_run_id
        
    except Exception as e:
        error_msg = f"Failed to submit EMR job for dataset {dataset_name}: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)

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

# Define the DAG to run EMR Serverless PySpark jobs for multiple datasets
with DAG(
    dag_id='emr_sl_testing_multi_dataset',
    default_args=default_args,
    description='Run PySpark jobs on EMR Serverless for multiple datasets in parallel',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
) as dag:

    @task
    def get_datasets():
        """Get the list of datasets to process"""
        datasets = get_datasets_list()
        print(f"Processing {len(datasets)} datasets: {datasets}")
        return datasets

    @task
    def submit_emr_job_for_dataset_task(dataset_name: str):
        """Task wrapper for submitting EMR job for a specific dataset"""
        return submit_emr_job_for_dataset(dataset_name)

    @task  
    def wait_for_emr_job_completion_task(job_run_id: str, dataset_name: str):
        """Task wrapper for waiting for EMR job completion"""
        emr_client = boto3.client('emr-serverless', region_name='eu-west-2')
        application_id = get_secrets("emr_application_id")
        
        print(f"Monitoring EMR job for dataset {dataset_name} with job_run_id: {job_run_id}")
        
        # Set timeout (15 minutes = 900 seconds)
        timeout_seconds = 900
        start_time = time.time()
        
        try:
            while True:
                # Check if we've exceeded timeout
                if time.time() - start_time > timeout_seconds:
                    print(f"Job monitoring timed out after {timeout_seconds} seconds for dataset {dataset_name}")
                    print(f"Attempting to cancel EMR Serverless job: {job_run_id}")
                    
                    try:
                        cancel_response = emr_client.cancel_job_run(
                            applicationId=application_id,
                            jobRunId=job_run_id
                        )
                        print(f"Job cancellation initiated: {cancel_response}")
                        time.sleep(10)
                        
                        final_response = emr_client.get_job_run(
                            applicationId=application_id,
                            jobRunId=job_run_id
                        )
                        final_state = final_response['jobRun']['state']
                        print(f"Final job state after cancellation: {final_state}")
                        
                    except Exception as cancel_error:
                        print(f"Error cancelling job: {cancel_error}")
                    
                    from airflow.exceptions import AirflowSkipException
                    raise AirflowSkipException(f"Job monitoring timed out after {timeout_seconds} seconds for dataset {dataset_name}")
                
                try:
                    response = emr_client.get_job_run(
                        applicationId=application_id,
                        jobRunId=job_run_id
                    )
                    
                    job_state = response['jobRun']['state']
                    print(f"Dataset {dataset_name} - Job state: {job_state}")
                    
                    if job_state == 'SUCCESS':
                        print(f"Dataset {dataset_name} - Job completed successfully!")
                        final_details = response.get('jobRun', {})
                        print(f"Dataset {dataset_name} - Job completed in {time.time() - start_time:.2f} seconds")
                        if 'billedResourceUtilization' in final_details:
                            print(f"Dataset {dataset_name} - Resource utilization: {final_details['billedResourceUtilization']}")
                        return {"dataset": dataset_name, "job_run_id": job_run_id, "status": "SUCCESS"}
                        
                    elif job_state in ['FAILED', 'CANCELLED', 'CANCELLING']:
                        state_details = response.get('jobRun', {}).get('stateDetails', '')
                        failure_reason = response.get('jobRun', {}).get('failureReason', '')
                        
                        error_msg = f"Dataset {dataset_name} - EMR job {job_state}"
                        if state_details:
                            error_msg += f": {state_details}"
                        if failure_reason:
                            error_msg += f" (Reason: {failure_reason})"
                        
                        job_run_details = response.get('jobRun', {})
                        print(f"Dataset {dataset_name} - Job run details: {json.dumps(job_run_details, indent=2, default=str)}")
                        
                        raise Exception(error_msg)
                        
                    elif job_state in ['PENDING', 'SCHEDULED', 'RUNNING']:
                        elapsed = time.time() - start_time
                        print(f"Dataset {dataset_name} - Job still {job_state.lower()}, elapsed time: {elapsed:.2f}s, waiting 30 seconds...")
                        
                        job_run_details = response.get('jobRun', {})
                        if 'jobProgress' in job_run_details:
                            print(f"Dataset {dataset_name} - Job progress: {job_run_details['jobProgress']}")
                        
                        time.sleep(30)
                    else:
                        print(f"Dataset {dataset_name} - Unknown job state: {job_state}, continuing to wait...")
                        print(f"Dataset {dataset_name} - Full EMR response: {json.dumps(response, indent=2, default=str)}")
                        time.sleep(30)
                        
                except Exception as e:
                    if "timed out" in str(e):
                        raise
                    print(f"Dataset {dataset_name} - Error checking job status: {e}, retrying in 30 seconds...")
                    time.sleep(30)
                    
        except Exception as outer_error:
            if "timed out" not in str(outer_error):
                print(f"Dataset {dataset_name} - Unexpected error occurred: {outer_error}")
                print(f"Dataset {dataset_name} - Attempting to cancel EMR Serverless job: {job_run_id}")
                try:
                    emr_client.cancel_job_run(
                        applicationId=application_id,
                        jobRunId=job_run_id
                    )
                    print(f"Dataset {dataset_name} - Job cancellation initiated due to unexpected error")
                except Exception as cancel_error:
                    print(f"Dataset {dataset_name} - Error cancelling job due to unexpected error: {cancel_error}")
            raise

    @task
    def summarize_results(results):
        """Summarize the results of all EMR jobs"""
        successful_jobs = [r for r in results if r and r.get('status') == 'SUCCESS']
        failed_jobs = [r for r in results if r and r.get('status') != 'SUCCESS']
        
        print(f"EMR Job Summary:")
        print(f"  Total datasets processed: {len(results)}")
        print(f"  Successful jobs: {len(successful_jobs)}")
        print(f"  Failed jobs: {len(failed_jobs)}")
        
        if successful_jobs:
            print(f"  Successfully processed datasets:")
            for job in successful_jobs:
                print(f"    - {job['dataset']} (Job ID: {job['job_run_id']})")
        
        if failed_jobs:
            print(f"  Failed datasets:")
            for job in failed_jobs:
                print(f"    - {job.get('dataset', 'Unknown')} (Job ID: {job.get('job_run_id', 'Unknown')})")
        
        return {
            "total": len(results),
            "successful": len(successful_jobs),
            "failed": len(failed_jobs),
            "successful_datasets": [job['dataset'] for job in successful_jobs],
            "failed_datasets": [job.get('dataset', 'Unknown') for job in failed_jobs]
        }

    # Task flow
    datasets = get_datasets()
    
    # Submit EMR jobs for each dataset (parallel execution)
    job_submissions = submit_emr_job_for_dataset_task.expand(dataset_name=datasets)
    
    # Wait for completion of each job (parallel monitoring)
    job_completions = wait_for_emr_job_completion_task.expand(
        job_run_id=job_submissions,
        dataset_name=datasets
    )
    
    # Summarize results
    summary = summarize_results(job_completions)
    
    # Set dependencies
    datasets >> job_submissions >> job_completions >> summary
