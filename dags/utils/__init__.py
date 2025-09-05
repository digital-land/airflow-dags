
# Lazy imports to avoid dependency issues
__all__ = ['setup_logging', 'get_logger', 'get_secret', 'get_secret_json', 'get_database_credentials', 'get_postgres_secret', 'load_json_from_repo', 'resolve_repo_path', 'resolve_desktop_path', 'cleanup_dataset_data', 'validate_s3_path', 'validate_s3_bucket_access', 'get_task_log_config', 'sort_collections_dict']

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
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
