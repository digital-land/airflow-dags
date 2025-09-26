from datetime import datetime, timedelta
import csv
import urllib.request
from aws_secrets_manager import get_secret_emr_compatible
import json
import boto3
import time


def get_datasets():
    dataset_spec_url = 'https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv'

    with urllib.request.urlopen(dataset_spec_url) as response:
        lines = [l.decode('utf-8') for l in response.readlines()]
        reader = csv.DictReader(lines)
        datasets = [row for row in reader]
    print(type(datasets))

    unique_keys = set()
    for item in datasets:
        unique_keys.update(item.keys())

    print(list(unique_keys))

    # Filter rows where realm is 'dataset' and typology is 'geograpthy'
    filtered_datasets = [row['dataset'] for row in datasets 
                        if row.get('realm') == 'dataset' and row.get('typology') == 'geography']

    # Print the filtered datasets column
    print(filtered_datasets)
    return filtered_datasets

# Retrieve secrets from AWS Secrets Manager
def get_secrets(secret_name):    
    aws_secrets_json = get_secret_emr_compatible("development/deployment_variables")

    # Parse the JSON string
    secrets = json.loads(aws_secrets_json)   

    secret_value = secrets.get(secret_name)

    return secret_value  

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
    print(f"Application ID: {application_id}")
    
    # Set timeout (48 minutes = 2880 seconds) to ensure task completes within 53-minute limit with buffer
    timeout_seconds = 2880
    start_time = time.time()
    
    # Initial job status check to validate we can access the job
    try:
        initial_response = emr_client.get_job_run(
            applicationId=application_id,
            jobRunId=job_run_id
        )
        initial_state = initial_response['jobRun']['state']
        print(f"Initial job state check: {initial_state}")
        
        # If job is already in a terminal state, handle immediately
        if initial_state == 'SUCCESS':
            print("Job already completed successfully!")
            return job_run_id
        elif initial_state in ['FAILED', 'CANCELLED', 'CANCELLING']:
            state_details = initial_response.get('jobRun', {}).get('stateDetails', '')
            failure_reason = initial_response.get('jobRun', {}).get('failureReason', '')
            error_msg = f"EMR job already in terminal state: {initial_state}"
            if state_details:
                error_msg += f": {state_details}"
            if failure_reason:
                error_msg += f" (Reason: {failure_reason})"
            raise Exception(error_msg)
    except Exception as init_error:
        print(f"Error during initial job status check: {init_error}")
        raise ValueError(f"Cannot access EMR job {job_run_id}: {init_error}")
    
    try:
        poll_count = 0
        while True:
            poll_count += 1
            # Check if we've exceeded timeout
            if time.time() - start_time > timeout_seconds:
                print(f"Job monitoring timed out after {timeout_seconds} seconds (poll count: {poll_count})")
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
                
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException(f"Job monitoring timed out after {timeout_seconds} seconds. Job cancellation attempted.")
            
            try:
                print(f"Poll #{poll_count} - Checking job status...")
                response = emr_client.get_job_run(
                    applicationId=application_id,
                    jobRunId=job_run_id
                )
                
                job_state = response['jobRun']['state']
                elapsed = time.time() - start_time
                print(f"Poll #{poll_count} - Job state: {job_state} (elapsed: {elapsed:.2f}s)")
                
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
                print(f"Error checking job status (poll #{poll_count}): {e}")
                print(f"Error type: {type(e).__name__}")
                
                # Add exponential backoff for API errors
                if "throttling" in str(e).lower() or "rate" in str(e).lower():
                    backoff_time = min(60, 30 * (poll_count % 3 + 1))  # 30, 60, 90 seconds, then repeat
                    print(f"API throttling detected, waiting {backoff_time} seconds...")
                    time.sleep(backoff_time)
                else:
                    print("Retrying in 30 seconds...")
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

    



