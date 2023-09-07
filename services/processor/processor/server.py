import os
import logging

import ayon_api
from ayon_api import get_addons_studio_settings, init_service
from ftrack_common import FtrackServer

from .ftrack_session import AYONServerSession


def get_handler_paths() -> list[str]:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    handler_paths = [
        os.path.join(current_dir, "default_handlers"),
    ]
    return handler_paths


def get_secrets():
    """Backwards compatibility for older ayon api versions.

    Returns:
        list[dict[str, str]]: List of secrets from server.
    """

    if hasattr(ayon_api, "get_secrets"):
        return ayon_api.get_secrets()
    return ayon_api.get("secrets").data


def main():
    logging.basicConfig(level=logging.INFO)

    init_service()

    handler_paths = get_handler_paths()
    settings = get_addons_studio_settings()
    ftrack_settings = settings["ftrack"]
    ftrack_url = ftrack_settings["ftrack_server"]
    service_settings = ftrack_settings["service_settings"]

    secrets_by_name = {
        secret["name"]: secret["value"]
        for secret in get_secrets()
    }
    api_key = service_settings["api_key"]
    username = service_settings["username"]
    if api_key in secrets_by_name:
        api_key = secrets_by_name[api_key]

    if username in secrets_by_name:
        username = secrets_by_name[username]

    session = AYONServerSession(
        ftrack_url.strip("/ "),
        api_key,
        username,
        auto_connect_event_hub=False
    )
    server = FtrackServer(handler_paths)
    server.run_server(session)
