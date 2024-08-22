import json
import boto3
import logging

def get_config(path):
    with open(path) as file:
        config = json.load(file)
    return config



def get_task_log_config(task_definition_name):
    """
    returns the log configuration of a task definition stored in aws
    assumes the local environment is set up to access aws
    """
    ecs_client = boto3.client('ecs')
    
    # Describe the task definition
    response = ecs_client.describe_task_definition(taskDefinition=task_definition_name)

    logging.warning(response['taskdefinition'])
    
    # Extract the log configuration from the container definitions
    log_config = response['taskDefinition']['containerDefinitions'][0].get('logConfiguration',{})
    
    return log_config