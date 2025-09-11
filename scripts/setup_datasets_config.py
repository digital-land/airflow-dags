#!/usr/bin/env python3
"""
Setup script to configure datasets list in AWS Secrets Manager or Parameter Store.

This script helps you securely store the list of datasets to be processed by the 
EMR multi-dataset DAG. You can choose between AWS Secrets Manager (more secure)
or AWS Parameter Store (good for configuration data).

Usage:
    python setup_datasets_config.py
"""

import boto3
import json
import sys
from typing import List

def setup_secrets_manager(datasets: List[str], region: str = 'eu-west-2'):
    """
    Store datasets list in AWS Secrets Manager.
    
    Args:
        datasets: List of dataset names
        region: AWS region
    """
    client = boto3.client('secretsmanager', region_name=region)
    secret_name = "development/emr_datasets_list"
    
    # Create the secret value as JSON
    secret_value = json.dumps(datasets)
    
    try:
        # Try to update existing secret
        response = client.update_secret(
            SecretId=secret_name,
            SecretString=secret_value,
            Description="List of datasets to process in EMR Serverless jobs"
        )
        print(f"✓ Updated existing secret '{secret_name}' in Secrets Manager")
        print(f"  Secret ARN: {response['ARN']}")
        
    except client.exceptions.ResourceNotFoundException:
        # Create new secret if it doesn't exist
        response = client.create_secret(
            Name=secret_name,
            SecretString=secret_value,
            Description="List of datasets to process in EMR Serverless jobs"
        )
        print(f"✓ Created new secret '{secret_name}' in Secrets Manager")
        print(f"  Secret ARN: {response['ARN']}")
        
    except Exception as e:
        print(f"✗ Error setting up Secrets Manager: {e}")
        return False
        
    return True

def setup_parameter_store(datasets: List[str], region: str = 'eu-west-2'):
    """
    Store datasets list in AWS Parameter Store.
    
    Args:
        datasets: List of dataset names
        region: AWS region
    """
    client = boto3.client('ssm', region_name=region)
    parameter_name = "/emr/datasets-list"
    
    # Create the parameter value as JSON
    parameter_value = json.dumps(datasets)
    
    try:
        response = client.put_parameter(
            Name=parameter_name,
            Value=parameter_value,
            Type='SecureString',  # Encrypted parameter
            Description="List of datasets to process in EMR Serverless jobs",
            Overwrite=True
        )
        print(f"✓ Set parameter '{parameter_name}' in Parameter Store")
        print(f"  Version: {response['Version']}")
        
    except Exception as e:
        print(f"✗ Error setting up Parameter Store: {e}")
        return False
        
    return True

def verify_secrets_manager(region: str = 'eu-west-2'):
    """Verify the secret can be retrieved from Secrets Manager."""
    try:
        client = boto3.client('secretsmanager', region_name=region)
        response = client.get_secret_value(SecretId="development/emr_datasets_list")
        datasets = json.loads(response['SecretString'])
        print(f"✓ Verification successful - Retrieved {len(datasets)} datasets from Secrets Manager:")
        for dataset in datasets:
            print(f"  - {dataset}")
        return True
    except Exception as e:
        print(f"✗ Verification failed for Secrets Manager: {e}")
        return False

def verify_parameter_store(region: str = 'eu-west-2'):
    """Verify the parameter can be retrieved from Parameter Store."""
    try:
        client = boto3.client('ssm', region_name=region)
        response = client.get_parameter(Name="/emr/datasets-list", WithDecryption=True)
        datasets = json.loads(response['Parameter']['Value'])
        print(f"✓ Verification successful - Retrieved {len(datasets)} datasets from Parameter Store:")
        for dataset in datasets:
            print(f"  - {dataset}")
        return True
    except Exception as e:
        print(f"✗ Verification failed for Parameter Store: {e}")
        return False

def main():
    print("EMR Multi-Dataset Configuration Setup")
    print("=" * 40)
    
    # Example datasets - modify this list as needed
    example_datasets = [
        "ancient-woodland",
        "conservation-area", 
        "listed-building",
        "tree-preservation-order",
        "article-4-direction"
    ]
    
    print(f"Example datasets list: {example_datasets}")
    print()
    
    # Get user input for datasets
    print("Enter datasets to process (comma-separated), or press Enter to use example list:")
    user_input = input("> ").strip()
    
    if user_input:
        datasets = [dataset.strip() for dataset in user_input.split(',')]
    else:
        datasets = example_datasets
    
    print(f"Using datasets: {datasets}")
    print()
    
    # Choose storage method
    print("Choose storage method:")
    print("1. AWS Secrets Manager (recommended for sensitive data)")
    print("2. AWS Parameter Store (good for configuration data)")
    print("3. Both (for redundancy)")
    
    choice = input("Enter choice (1/2/3): ").strip()
    
    if choice == "1" or choice == "3":
        print("\nSetting up AWS Secrets Manager...")
        if setup_secrets_manager(datasets):
            verify_secrets_manager()
    
    if choice == "2" or choice == "3":
        print("\nSetting up AWS Parameter Store...")
        if setup_parameter_store(datasets):
            verify_parameter_store()
    
    print("\n" + "=" * 40)
    print("Setup complete!")
    print()
    print("Next steps:")
    print("1. Ensure your Airflow execution role has permissions to read from the chosen service")
    print("2. Test the DAG with a small subset of datasets first")
    print("3. Monitor the EMR Serverless jobs in the AWS console")
    
    # Show required permissions
    print("\nRequired IAM permissions:")
    if choice == "1" or choice == "3":
        print("For Secrets Manager:")
        print("  - secretsmanager:GetSecretValue on 'development/emr_datasets_list'")
    
    if choice == "2" or choice == "3":
        print("For Parameter Store:")
        print("  - ssm:GetParameter on '/emr/datasets-list'")
    
    print("For EMR Serverless:")
    print("  - emr-serverless:StartJobRun")
    print("  - emr-serverless:GetJobRun") 
    print("  - emr-serverless:CancelJobRun")

if __name__ == "__main__":
    main()
