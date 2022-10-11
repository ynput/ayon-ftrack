import os

from ayclient import addon_settings
from .ftrack_session import OPServerSession
from .ftrack_server import FtrackServer


def get_handler_paths() -> list[str]:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    handler_paths = [
        os.path.join(current_dir, "base_handlers"),
    ]
    return handler_paths


def main():
    handler_paths = get_handler_paths()
    settings = addon_settings()
    service_settings = settings["service_settings"]
    session = OPServerSession(
        settings["ftrack_server"],
        service_settings["api_key"],
        service_settings["username"]
    )
    server = FtrackServer(handler_paths)
    server.run_server(session)


if __name__ == "__main__":
    main()
