import base64
import json
from types import SimpleNamespace
from unittest.mock import Mock

import github_actions
import jwt
import pytest
import requests
from aws_secrets_manager import SecretsManagerError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

SECRET_ENV_VAR = "SECRET__PRODUCTION_PD_BATCH_GITHUB_ACTIONS_TRIGGER"


@pytest.fixture(scope="module")
def private_key():
    """A throwaway RSA key standing in for the GitHub App's private key."""
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="module")
def private_key_pem(private_key):
    return private_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()).decode("utf-8")


@pytest.fixture
def github_app_secret(monkeypatch, private_key_pem):
    """The App's secret as stored in Secrets Manager, read through get_secret_emr_compatible's env var override."""
    secret = {"app_id": "123", "installation_id": "456", "private_key_base64": base64.b64encode(private_key_pem.encode("utf-8")).decode("utf-8")}
    monkeypatch.setenv(SECRET_ENV_VAR, json.dumps(secret))
    return secret


@pytest.fixture
def github(monkeypatch):
    """Stand-ins for GitHub's API: a token on POST access_tokens, 204 on POST dispatches, 204 on DELETE token."""

    def post(url, **kwargs):
        if url.endswith("/access_tokens"):
            return Mock(status_code=201, json=Mock(return_value={"token": "installation-token"}), raise_for_status=Mock())
        return Mock(status_code=204, text="")

    github = SimpleNamespace(post=Mock(side_effect=post), delete=Mock(return_value=Mock(status_code=204)))
    monkeypatch.setattr(github_actions.requests, "post", github.post)
    monkeypatch.setattr(github_actions.requests, "delete", github.delete)
    return github


def posts_to(github, url_suffix):
    return [request for request in github.post.call_args_list if request.args[0].endswith(url_suffix)]


def test_secret_name_matches_the_pd_batch_prefix_airflow_can_read():
    assert github_actions.github_app_secret_name("production") == "/production-pd-batch/github-actions-trigger"


def test_credentials_are_read_and_the_private_key_decoded(github_app_secret, private_key_pem):
    assert github_actions.get_github_app_credentials("production") == ("123", "456", private_key_pem)


def test_an_unreadable_secret_names_the_secret(monkeypatch):
    monkeypatch.delenv(SECRET_ENV_VAR, raising=False)
    monkeypatch.setattr(github_actions, "get_secret_emr_compatible", Mock(side_effect=SecretsManagerError("Failed to retrieve secret: AccessDeniedException")))

    with pytest.raises(RuntimeError, match="/production-pd-batch/github-actions-trigger.*AccessDeniedException"):
        github_actions.get_github_app_credentials("production")


@pytest.mark.parametrize(
    "secret",
    [
        {"app_id": "123", "installation_id": "456"},
        {"app_id": "123", "installation_id": "456", "private_key_base64": "not base64!"},
    ],
)
def test_a_malformed_secret_names_the_secret_without_showing_its_contents(monkeypatch, secret):
    monkeypatch.setenv(SECRET_ENV_VAR, json.dumps(secret))

    with pytest.raises(RuntimeError, match="/production-pd-batch/github-actions-trigger") as error:
        github_actions.get_github_app_credentials("production")

    assert "not base64!" not in str(error.value)


def test_installation_token_is_requested_with_a_jwt_signed_by_the_app(github, private_key, private_key_pem):
    token = github_actions.get_installation_token("123", "456", private_key_pem)

    assert token == "installation-token"
    (request,) = posts_to(github, "/access_tokens")
    assert request.args[0] == "https://api.github.com/app/installations/456/access_tokens"
    app_jwt = request.kwargs["headers"]["Authorization"].removeprefix("Bearer ")
    claims = jwt.decode(app_jwt, private_key.public_key(), algorithms=["RS256"])
    assert claims["iss"] == "123"
    # GitHub rejects a JWT that is valid for more than 10 minutes
    assert claims["exp"] - claims["iat"] <= 600


def test_a_rejected_installation_token_request_raises(monkeypatch, private_key_pem):
    response = Mock(raise_for_status=Mock(side_effect=requests.HTTPError("401 Unauthorized")))
    monkeypatch.setattr(github_actions.requests, "post", Mock(return_value=response))

    with pytest.raises(requests.HTTPError):
        github_actions.get_installation_token("123", "456", private_key_pem)


def test_dispatch_sends_the_ref_and_inputs(github):
    github_actions.dispatch_workflow("installation-token", "digital-land/config", "batch-assign.yml", "main", {"scope": "all"})

    (request,) = posts_to(github, "/dispatches")
    assert request.args[0] == "https://api.github.com/repos/digital-land/config/actions/workflows/batch-assign.yml/dispatches"
    assert request.kwargs["json"] == {"ref": "main", "inputs": {"scope": "all"}}
    assert request.kwargs["headers"]["Authorization"] == "Bearer installation-token"


def test_dispatch_raises_unless_github_starts_the_run(monkeypatch):
    rejected = Mock(status_code=422, text='{"message":"Unexpected inputs provided"}')
    monkeypatch.setattr(github_actions.requests, "post", Mock(return_value=rejected))

    with pytest.raises(RuntimeError, match="422.*Unexpected inputs provided"):
        github_actions.dispatch_workflow("installation-token", "digital-land/config", "batch-assign.yml", "main", {"scope": "all"})


def test_trigger_workflow_starts_the_run_and_revokes_the_token(github_app_secret, github):
    github_actions.trigger_workflow("production", repository="digital-land/config", workflow="batch-assign.yml", inputs={"scope": "all"})

    assert len(posts_to(github, "/dispatches")) == 1
    github.delete.assert_called_once()
    assert github.delete.call_args.args[0] == "https://api.github.com/installation/token"


def test_the_token_is_revoked_even_when_the_dispatch_fails(github_app_secret, github, monkeypatch):
    monkeypatch.setattr(github_actions, "dispatch_workflow", Mock(side_effect=RuntimeError("GitHub did not start it")))

    with pytest.raises(RuntimeError):
        github_actions.trigger_workflow("production", repository="digital-land/config", workflow="batch-assign.yml", inputs={"scope": "all"})

    github.delete.assert_called_once()


def test_a_failed_revoke_does_not_fail_the_task(github_app_secret, github, monkeypatch):
    monkeypatch.setattr(github_actions.requests, "delete", Mock(side_effect=requests.ConnectionError()))

    github_actions.trigger_workflow("production", repository="digital-land/config", workflow="batch-assign.yml", inputs={"scope": "all"})
