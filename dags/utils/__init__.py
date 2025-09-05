
# Lazy imports to avoid dependency issues
__all__ = ['setup_logging', 'get_logger', 'get_secret', 'get_secret_json', 'get_database_credentials', 'get_postgres_secret', 'load_json_from_repo', 'resolve_repo_path', 'resolve_desktop_path', 'cleanup_dataset_data', 'validate_s3_path', 'validate_s3_bucket_access', 'get_task_log_config', 'sort_collections_dict', 'dag_default_args', 'get_config', 'setup_configure_dag_callable', 'load_specification_datasets', 'push_log_variables', 'push_vpc_config', 'get_collections_dict', 'get_dataset_collection', 'get_datasets']

def __getattr__(name):
    if name == 'setup_logging' or name == 'get_logger':
        from .logger_config import setup_logging, get_logger
        return locals()[name]
    elif name in ['get_secret', 'get_secret_json', 'get_database_credentials', 'get_postgres_secret']:
        from .aws_secrets_manager import get_secret, get_secret_json, get_database_credentials, get_postgres_secret
        return locals()[name]
    elif name in ['load_json_from_repo', 'resolve_repo_path', 'resolve_desktop_path']:
        from .path_utils import load_json_from_repo, resolve_repo_path, resolve_desktop_path
        return locals()[name]
    elif name in ['cleanup_dataset_data', 'validate_s3_path', 'validate_s3_bucket_access']:
        from .s3_utils import cleanup_dataset_data, validate_s3_path, validate_s3_bucket_access
        return locals()[name]
    elif name == 'get_task_log_config':
        def get_task_log_config(ecs_client, task_definition_family):
            """returns the log configuration of a task definition stored in aws"""
            response = ecs_client.describe_task_definition(taskDefinition=task_definition_family)
            log_config = response['taskDefinition']['containerDefinitions'][0].get('logConfiguration', {})
            return log_config
        return get_task_log_config
    elif name == 'sort_collections_dict':
        def sort_collections_dict(collections_dict):
            """Given a dictionary of collections and datasets, return a sorted list of collections."""
            priority = ["tree-preservation-order", "transport-access-node","flood-risk-zone","listed-building","conservation-area"]
            def sort_key(item):
                key, value = item
                if key in priority:
                    return (0, priority.index(key))
                return (1, value)
            sorted_collections = dict(sorted(collections_dict.items(), key=sort_key))
            return sorted_collections
        return sort_collections_dict
    elif name == 'dag_default_args':
        from datetime import datetime, timedelta
        dag_default_args = {
            "owner": "airflow",
            "depends_on_past": False,
            "start_date": datetime(2024, 1, 1),
            "dagrun_timeout": timedelta(minutes=5),
        }
        return dag_default_args
    elif name == 'get_config':
        def get_config(path=None):
            import os
            import json
            if path is None:
                my_dir = os.path.dirname(os.path.abspath(__file__))
                path = os.path.join(my_dir, "config.json")
            try:
                with open(path) as file:
                    config = json.load(file)
                return config
            except FileNotFoundError:
                # Return default config if file doesn't exist
                return {
                    "env": "development",
                    "aws_region": "us-east-1"
                }
        return get_config
    elif name == 'setup_configure_dag_callable':
        def setup_configure_dag_callable(config, task_definition_name):
            def configure_dag(**kwargs):
                """
                function which returns the relevant configuration details
                and stores them in xcoms for other tasks. this includes:
                - get and process params into correct formats
                - read in env variables
                - access options defined in the task definitions
                """
                ti = kwargs['ti']
                
                # add env from config
                ti.xcom_push(key='env', value=config['env'])
                
                # add DAG parameters
                params = kwargs['params']
                
                memory = int(params.get('memory'))
                cpu = int(params.get('cpu'))
                
                # Store basic task info
                ti.xcom_push(key='memory', value=memory)
                ti.xcom_push(key='cpu', value=cpu)
                ti.xcom_push(key='task_definition_name', value=task_definition_name)
                
            return configure_dag
        return setup_configure_dag_callable
    elif name in ['load_specification_datasets', 'push_log_variables', 'push_vpc_config', 'get_collections_dict', 'get_dataset_collection', 'get_datasets']:
        # For complex functions that have dependencies, we'll need to implement them as needed
        raise ImportError(f"Function '{name}' not yet implemented in utils package. Please add it manually.")
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
