import os
import logging

from ayon_api import get_addons_studio_settings, init_service
from ftrack_common import FtrackServer

from .ftrack_session import AYONServerSession


def get_handler_paths() -> list[str]:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    handler_paths = [
        os.path.join(current_dir, "default_handlers"),
    ]
    return handler_paths


def main():
    logging.basicConfig()

    init_service()

    handler_paths = get_handler_paths()
    settings = get_addons_studio_settings()
    ftrack_settings = settings["ftrack"]
    service_settings = ftrack_settings["service_settings"]
    session = AYONServerSession(
        ftrack_settings["ftrack_server"],
        service_settings["api_key"],
        service_settings["username"],
        auto_connect_event_hub=False
    )
    server = FtrackServer(handler_paths)
    server.run_server(session)
