"""
Start GitHub Actions workflows as the GitHub App Airflow uses for this.

The App's credentials are in Secrets Manager. The App only has Actions: write, and is only installed on digital-land/config.
"""

import base64
import json
import logging
import time

import jwt
import requests
from aws_secrets_manager import get_secret_emr_compatible

logger = logging.getLogger(__name__)

GITHUB_API_URL = "https://api.github.com"
GITHUB_HEADERS = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
REQUEST_TIMEOUT_SECONDS = 30


def github_app_secret_name(env):
    return f"/{env}-pd-batch/github-actions-trigger"


def get_github_app_credentials(env):
    """Return the App's ID, its installation ID and its private key, read from Secrets Manager."""
    secret_name = github_app_secret_name(env)
    try:
        secret = json.loads(get_secret_emr_compatible(secret_name))
        private_key = base64.b64decode(secret["private_key_base64"], validate=True).decode("utf-8")
        return str(secret["app_id"]), str(secret["installation_id"]), private_key
    except Exception as error:
        # names the secret and what went wrong, never its contents
        raise RuntimeError(f"Could not read the GitHub App credentials from {secret_name}: {error!r}") from error


def get_installation_token(app_id, installation_id, private_key):
    """Exchange a JWT signed with the App's private key for an installation token, valid for an hour."""
    now = int(time.time())
    # iat is backdated to allow for clock drift; GitHub rejects an exp more than 10 minutes ahead
    app_jwt = jwt.encode({"iat": now - 60, "exp": now + 540, "iss": app_id}, private_key, algorithm="RS256")
    response = requests.post(
        f"{GITHUB_API_URL}/app/installations/{installation_id}/access_tokens",
        headers={**GITHUB_HEADERS, "Authorization": f"Bearer {app_jwt}"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()["token"]


def dispatch_workflow(token, repository, workflow, ref, inputs):
    """Start a workflow_dispatch run. GitHub answers 204 with no body, so there is no run ID to return."""
    response = requests.post(
        f"{GITHUB_API_URL}/repos/{repository}/actions/workflows/{workflow}/dispatches",
        headers={**GITHUB_HEADERS, "Authorization": f"Bearer {token}"},
        json={"ref": ref, "inputs": inputs},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code != 204:
        raise RuntimeError(f"GitHub did not start {workflow} in {repository}: {response.status_code} {response.text}")


def revoke_installation_token(token):
    """Revoke the token as soon as it has been used, rather than leaving it valid for the rest of its hour."""
    try:
        requests.delete(
            f"{GITHUB_API_URL}/installation/token",
            headers={**GITHUB_HEADERS, "Authorization": f"Bearer {token}"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        logger.warning("Could not revoke the GitHub installation token; it expires within the hour anyway")


def trigger_workflow(env, repository, workflow, ref="main", inputs=None):
    """Start a workflow run and return without waiting for it; the workflows raise their own Slack alerts."""
    inputs = inputs or {}
    app_id, installation_id, private_key = get_github_app_credentials(env)
    token = get_installation_token(app_id, installation_id, private_key)
    try:
        dispatch_workflow(token, repository, workflow, ref, inputs)
    finally:
        revoke_installation_token(token)
    logger.info("Started %s in %s on %s with inputs %s", workflow, repository, ref, inputs)
