import os
from flask_appbuilder.security.manager import AUTH_OAUTH

# Enable OAuth authentication
AUTH_TYPE = AUTH_OAUTH
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_USER_REGISTRATION = True
AUTH_ROLES_MAPPING = {
    "Viewer": ["Viewer"],
    "Admin": ["Admin"],
}

# Configure the OAuth provider
OAUTH_PROVIDERS = [{
    'name': 'github',
    'token_key': 'access_token',
    'icon': 'fa-github',
    'remote_app': {
        'client_id': os.getenv('AIRFLOW__CORE__GITHUBCLIENTID'),
        'client_secret': os.getenv('AIRFLOW__CORE__GITHUBCLIENTSECRET'),
        'api_base_url': 'https://api.github.com/',
        'client_kwargs': {
            'scope': 'user:email',
        },
        'authorize_url': 'https://github.com/login/oauth/authorize',
        'access_token_url': 'https://github.com/login/oauth/access_token',
         'request_token_url': None,
    }
}]
