"""
Generic AWS Secrets Manager utility for retrieving secrets safely.

This module provides a simple interface to AWS Secrets Manager that can be used
across different applications. It supports both JSON and string secrets.

Environment Variables:
    AWS_REGION: AWS region where secrets are stored (optional, defaults to us-east-1)
    
Usage:
    from utils.aws_secrets_manager import get_secret, get_secret_json
    
    # For JSON secrets (e.g., database credentials)
    db_creds = get_secret_json("myapp/database/credentials")
    username = db_creds.get("username")
    password = db_creds.get("password")
    
    # For string secrets (e.g., API keys)
    api_key = get_secret("myapp/api/key")
"""

import json
import logging
import os
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logger = logging.getLogger(__name__)


class SecretsManagerError(Exception):
    """Custom exception for Secrets Manager related errors."""
    pass


def get_secret(secret_name: str, region_name: Optional[str] = None) -> str:
    """
    Retrieve a string secret from AWS Secrets Manager using native AWS SDK.
    
    Args:
        secret_name (str): The name or ARN of the secret to retrieve
        region_name (str, optional): AWS region. If not provided, uses AWS_REGION 
                                   environment variable or defaults to eu-west-2
    
    Returns:
        str: The secret string value
        
    Raises:
        SecretsManagerError: If the secret cannot be retrieved
        ValueError: If secret_name is empty or None
    """
    if not secret_name:
        raise ValueError("secret_name cannot be empty or None")
    
    if not region_name:
        region_name = os.getenv("AWS_REGION", "eu-west-2")
    
    logger.info(f"Retrieving secret '{secret_name}' using native AWS SDK")
    
    try:
        # Use native boto3 client (simple and clean)
        client = boto3.client('secretsmanager', region_name=region_name)
        
        # Retrieve the secret
        response = client.get_secret_value(SecretId=secret_name)
        
        # Get the secret string
        secret_value = response.get('SecretString')
        if secret_value is None:
            raise SecretsManagerError("Secret does not contain a string value")
        
        logger.info("Successfully retrieved secret")
        return secret_value
        
    except NoCredentialsError:
        error_msg = "AWS credentials not found. Ensure AWS credentials are configured."
        logger.error(error_msg)
        raise SecretsManagerError(error_msg)
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = f"Failed to retrieve secret: {error_code}"
        logger.error(error_msg)
        raise SecretsManagerError(error_msg)
        
    except Exception as e:
        error_msg = f"Unexpected error retrieving secret: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Exception details: {type(e).__name__}: {e}")
        raise SecretsManagerError(error_msg)


def get_secret_json(secret_name: str, region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Retrieve a JSON secret from AWS Secrets Manager and parse it.
    
    Args:
        secret_name (str): The name or ARN of the secret to retrieve
        region_name (str, optional): AWS region. If not provided, uses AWS_REGION 
                                   environment variable or defaults to us-east-1
    
    Returns:
        Dict[str, Any]: The parsed JSON secret as a dictionary
        
    Raises:
        SecretsManagerError: If the secret cannot be retrieved or parsed
        ValueError: If secret_name is empty or None
    """
    secret_string = get_secret(secret_name, region_name)
    
    try:
        secret_dict = json.loads(secret_string)
        logger.info("Successfully parsed JSON secret")
        return secret_dict
        
    except json.JSONDecodeError as e:
        error_msg = "Secret is not valid JSON"
        logger.error(error_msg)
        raise SecretsManagerError(error_msg)


def get_database_credentials(secret_name: str, region_name: Optional[str] = None) -> Dict[str, str]:
    """
    Convenience method to retrieve database credentials from AWS Secrets Manager.
    
    Expected JSON format:
    {
        "username": "db_user",
        "password": "db_password",
        "host": "db_host",
        "port": "5432",
        "database": "db_name",
        "engine": "postgres",
        "dbClusterIdentifier": "my-cluster"
    }
    
    Args:
        secret_name (str): The name or ARN of the database secret
        region_name (str, optional): AWS region. If not provided, uses AWS_REGION 
                                   environment variable or defaults to us-east-1
    
    Returns:
        Dict[str, str]: Database credentials with keys: username, password, host, port, 
                       and optionally: database, engine, dbClusterIdentifier
        
    Raises:
        SecretsManagerError: If the secret cannot be retrieved or required keys are missing
        ValueError: If secret_name is empty or None
    """
    credentials = get_secret_json(secret_name, region_name)
    
    required_keys = ["username", "password", "host"]
    missing_keys = [key for key in required_keys if key not in credentials]
    
    if missing_keys:
        error_msg = f"Database secret missing required keys: {missing_keys}"
        logger.error(error_msg)
        raise SecretsManagerError(error_msg)
    
    # Set default port if not provided, with engine-specific defaults
    if "port" not in credentials:
        engine = credentials.get("engine", "").lower()
        if engine == "mysql":
            credentials["port"] = "3306"
        elif engine in ["postgres", "postgresql"]:
            credentials["port"] = "5432"
        else:
            credentials["port"] = "5432"  # Default to PostgreSQL port
    
    # Convert port to string if it's an integer
    credentials["port"] = str(credentials["port"])
    
    # Note: Do not log actual credential values (username, password, host, port, etc.) for security
    logger.info("Successfully retrieved database credentials")
    return credentials


def get_secret_emr_compatible(secret_name: str, region_name: Optional[str] = None) -> str:
    """
    EMR Serverless compatible secret retrieval using native AWS SDK.
    
    This function uses the native boto3 installation in EMR Serverless and includes
    environment variable fallback for testing/development scenarios.
    
    Args:
        secret_name (str): The name or ARN of the secret to retrieve
        region_name (str, optional): AWS region. If not provided, uses AWS_REGION 
                                   environment variable or defaults to eu-west-2
    
    Returns:
        str: The secret string value
        
    Raises:
        SecretsManagerError: If the secret cannot be retrieved through any method
        ValueError: If secret_name is empty or None
    """
    if not secret_name:
        raise ValueError("secret_name cannot be empty or None")
    
    if not region_name:
        region_name = os.getenv("AWS_REGION", "eu-west-2")
    
    logger.info(f"EMR-compatible retrieval of secret '{secret_name}' using native AWS SDK")
    
    # Strategy 1: Try environment variable override first (for testing/development)
    env_secret_name = secret_name.replace("/", "_").replace("-", "_").upper()
    env_value = os.getenv(f"SECRET_{env_secret_name}")
    if env_value:
        logger.info(f"Using environment variable fallback: SECRET_{env_secret_name}")
        return env_value
    
    # Strategy 2: Use native EMR boto3 (simple approach)
    try:
        logger.info("Using native EMR boto3 client")
        client = boto3.client('secretsmanager', region_name=region_name)
        
        response = client.get_secret_value(SecretId=secret_name)
        secret_value = response.get('SecretString')
        
        if secret_value is None:
            raise SecretsManagerError("Secret does not contain a string value")
            
        logger.info("Successfully retrieved secret using native EMR AWS SDK")
        return secret_value
        
    except NoCredentialsError:
        error_msg = "AWS credentials not found. Ensure EMR job has proper IAM role."
        logger.error(error_msg)
        raise SecretsManagerError(error_msg)
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = f"Failed to retrieve secret: {error_code}"
        logger.error(error_msg)
        raise SecretsManagerError(error_msg)
        
    except Exception as e:
        error_msg = f"Unexpected error retrieving secret: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Exception details: {type(e).__name__}: {e}")
        raise SecretsManagerError(error_msg)


def get_secret_with_fallback(secret_name: str, 
                           env_var_name: Optional[str] = None,
                           region_name: Optional[str] = None) -> str:
    """
    Retrieve a secret from AWS Secrets Manager with optional environment variable fallback.
    
    This is useful for development environments where you might want to use environment
    variables instead of AWS Secrets Manager.
    
    Args:
        secret_name (str): The name or ARN of the secret to retrieve
        env_var_name (str, optional): Environment variable name to check as fallback
        region_name (str, optional): AWS region. If not provided, uses AWS_REGION 
                                   environment variable or defaults to us-east-1
    
    Returns:
        str: The secret value from AWS Secrets Manager or environment variable
        
    Raises:
        SecretsManagerError: If neither secret nor environment variable is found
        ValueError: If secret_name is empty or None
    """
    if not secret_name:
        raise ValueError("secret_name cannot be empty or None")
    
    # Try environment variable first if provided
    if env_var_name:
        env_value = os.getenv(env_var_name)
        if env_value:
            logger.info("Using environment variable fallback")
            return env_value
    
    # Try AWS Secrets Manager
    try:
        return get_secret(secret_name, region_name)
    except SecretsManagerError as e:
        if env_var_name:
            error_msg = "Failed to retrieve secret and environment variable fallback is not set"
        else:
            error_msg = "Failed to retrieve secret"
        
        logger.error(error_msg)
        raise SecretsManagerError(error_msg)


# Convenience function for backward compatibility
def get_postgres_secret(region_name: Optional[str] = None) -> Dict[str, str]:
    """
    Convenience function to get PostgreSQL credentials using environment variable.
    
    Uses POSTGRES_SECRET_NAME environment variable to determine the secret name.
    
    Args:
        region_name (str, optional): AWS region. If not provided, uses AWS_REGION 
                                   environment variable or defaults to us-east-1
    
    Returns:
        Dict[str, str]: PostgreSQL credentials
        
    Raises:
        SecretsManagerError: If secret cannot be retrieved
        ValueError: If POSTGRES_SECRET_NAME environment variable is not set
    """
    secret_name = os.getenv("POSTGRES_SECRET_NAME")
    if not secret_name:
        raise ValueError("Environment variable POSTGRES_SECRET_NAME must be set")
    
    return get_database_credentials(secret_name, region_name)