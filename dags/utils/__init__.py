
# Lazy imports to avoid dependency issues
__all__ = ['setup_logging', 'get_logger', 'get_secret', 'get_secret_json', 'get_database_credentials', 'get_postgres_secret', 'load_json_from_repo', 'resolve_repo_path', 'resolve_desktop_path', 'cleanup_dataset_data', 'validate_s3_path', 'validate_s3_bucket_access']

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
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
