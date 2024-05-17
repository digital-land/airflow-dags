import os
from flask_appbuilder.security.manager import AUTH_OAUTH

# Enable OAuth authentication
AUTH_TYPE = AUTH_OAUTH

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
        'authorize_params': {
            'redirect_uri': os.getenv('AIRFLOW__CORE__GITHUBREDIRECTURI')
        },
    }
}]

# Enable automatic user registration
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = 'Viewer'  # Change as needed
AUTH_ROLES_SYNC_AT_LOGIN = True
