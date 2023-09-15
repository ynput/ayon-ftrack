import os
import sys
import threading
import time
import logging
import signal
import traceback

import ayon_api
import ftrack_api

from ftrack_common import FtrackServer

from .ftrack_session import AYONServerSession


class _GlobalContext:
    stop_event = threading.Event()
    session = None
    session_fail_logged = 0


def get_handler_paths() -> list[str]:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    handler_paths = [
        os.path.join(current_dir, "default_handlers"),
    ]
    return handler_paths


def get_service_label():
    return " ".join([
        ayon_api.get_service_addon_name(),
        ayon_api.get_service_addon_version()
    ])


def _create_session():
    ftrack_settings = ayon_api.get_service_addon_settings()
    ftrack_url = ftrack_settings["ftrack_server"]
    service_settings = ftrack_settings["service_settings"]

    secrets_by_name = {
        secret["name"]: secret["value"]
        for secret in ayon_api.get_secrets()
    }
    api_key = service_settings["api_key"]
    username = service_settings["username"]
    if api_key in secrets_by_name:
        api_key = secrets_by_name[api_key]

    if username in secrets_by_name:
        username = secrets_by_name[username]

    if not username or not api_key:
        return (
            "Missing Ftrack Username or Ftrack API in settings."
            f" Please check your settings of {get_service_label()}."
        )

    _GlobalContext.session = AYONServerSession(
        ftrack_url.strip("/ "),
        api_key,
        username,
        auto_connect_event_hub=False
    )


def create_session():
    tb_content = None
    error_summary = "Failed to create ftrack session."
    try:
        error_message = _create_session()
    except ftrack_api.exception.ServerError:
        error_message = (
            "Ftrack Username or ftrack API in settings are not valid."
            f" Please check your settings of {get_service_label()}."
        )
    except Exception:
        error_message = f"{error_summary} Crashed!!!"
        tb_lines = traceback.format_exception(*sys.exc_info())
        tb_content = "".join(tb_lines)

    session = _GlobalContext.session
    if session is not None:
        print("Created ftrack session")
        return session

    if not error_message:
        error_message = error_summary
    print(error_message)
    if tb_content:
        print(tb_content)
    if (
        (tb_content is not None and _GlobalContext.session_fail_logged == 2)
        or (tb_content is None and _GlobalContext.session_fail_logged == 1)
    ):
        return

    event_data = {
        "sender": ayon_api.ServiceContext.service_name,
        "finished": True,
        "store": True,
        "description": "ftrack processor error",
        "payload": {
            "message": error_message,
        }
    }
    if tb_lines is None:
        _GlobalContext.session_fail_logged = 2
        event_data["summary"] = error_summary

    else:
        _GlobalContext.session_fail_logged = 1
        event_data["payload"]["traceback"] = tb_content

    ayon_api.dispatch_event(
        "log.error",
        **event_data,
    )


def main_loop():
    while not _GlobalContext.stop_event.is_set():
        session = create_session()
        if session is None:
            time.sleep(10)
            continue

        _GlobalContext.session_fail_logged = False

        handler_paths = get_handler_paths()

        server = FtrackServer(handler_paths)
        print("Starting listening loop")
        server.run_server(session)


def main():
    logging.basicConfig(level=logging.INFO)

    try:
        ayon_api.init_service()
        connected = True
    except Exception:
        connected = False

    if not connected:
        print("Failed to connect to AYON server.")
        # Sleep for 10 seconds, so it is possible to see the message in
        #   docker
        # NOTE: Becuase AYON connection failed, there's no way how to log it
        #   to AYON server (obviously)... So stdout is all we have.
        time.sleep(10)
        sys.exit(1)

    print("Connected to AYON server.")

    # Register interrupt signal
    def signal_handler(sig, frame):
        print("Process stop requested. Terminating process.")
        _GlobalContext.stop_event.set()
        session = _GlobalContext.session
        if session is not None:
            if session.event_hub.connected is True:
                session.event_hub.disconnect()
            session.close()
        print("Termination finished.")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    main_loop()
