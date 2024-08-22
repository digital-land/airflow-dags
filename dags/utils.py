import json
import boto3
import logging

def get_config(path):
    with open(path) as file:
        config = json.load(file)
    return config



def get_task_log_config(ecs_client,task_definition_family):
    """
    returns the log configuration of a task definition stored in aws
    assumes the local environment is set up to access aws
    """
    
    # Describe the task definition
    response = ecs_client.describe_task_definition(taskDefinition=task_definition_family)
    
    # Extract the log configuration from the container definitions
    log_config = response['taskDefinition']['containerDefinitions'][0].get('logConfiguration',{})
    
    return log_config