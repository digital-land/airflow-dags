from airflow.auth.managers.fab.security_manager.override import FabAirflowSecurityManagerOverride
from typing import Any, List, Union
import logging
import os

log = logging.getLogger(__name__)
log.setLevel(os.getenv("AIRFLOW__LOGGING__FAB_LOGGING_LEVEL", "INFO"))

FAB_ADMIN_ROLE = "Admin"
FAB_VIEWER_ROLE = "Viewer"
FAB_PUBLIC_ROLE = "Public"  # The "Public" role is given no permissions
TEAM_ID_A_FROM_GITHUB = 7380651  # Team - digital-land-admin
TEAM_ID_B_FROM_GITHUB = 7380661  # Team - digital-land-member


def team_parser(team_payload: dict[str, Any]) -> list[int]:
    # Parse the team payload from GitHub however you want here.
    return [team["id"] for team in team_payload]


def map_roles(team_list: list[int]) -> list[str]:
    # Associate the team IDs with Roles here.
    # The expected output is a list of roles that FAB will use to Authorize the user.

    team_role_map = {
        TEAM_ID_A_FROM_GITHUB: FAB_ADMIN_ROLE,
        TEAM_ID_B_FROM_GITHUB: FAB_VIEWER_ROLE,
    }
    return list(set(team_role_map.get(team, FAB_PUBLIC_ROLE) for team in team_list))


class GithubTeamAuthorizer(FabAirflowSecurityManagerOverride):
    # In this example, the oauth provider == 'github'.
    # If you ever want to support other providers, see how it is done here:
    # https://github.com/dpgaspar/Flask-AppBuilder/blob/master/flask_appbuilder/security/manager.py#L550
    def get_oauth_user_info(self, provider: str, resp: Any) -> dict[str, Union[str, list[str]]]:
        # Creates the user info payload from Github.
        # The user previously allowed your app to act on their behalf,
        #   so now we can query the user and teams endpoints for their data.
        # Username and team membership are added to the payload and returned to FAB.

        remote_app = self.appbuilder.sm.oauth_remotes[provider]
        me = remote_app.get("user")
        user_data = me.json()
        team_data = remote_app.get("user/teams")
        teams = team_parser(team_data.json())
        roles = map_roles(teams)
        log.debug(f"User info from Github: {user_data}\nTeam info from Github: {teams}")
        return {"username": "github_" + user_data.get("login"), "role_keys": roles}