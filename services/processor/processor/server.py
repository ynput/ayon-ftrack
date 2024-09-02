import os
import sys
import threading
import time
import logging
import signal
import traceback
import atexit

import ayon_api
import ftrack_api

from ftrack_common import FtrackServer

from .ftrack_session import AYONServerSession
from .download_utils import (
    cleanup_download_root,
    downloaded_event_handlers,
)


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
        logging.info("Created ftrack session")
        return session

    if not error_message:
        error_message = error_summary

    log_msg = error_message
    if tb_content:
        log_msg += f"\n{tb_content}"
    logging.error(log_msg)
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
    if tb_content is None:
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
    addon_name = ayon_api.get_service_addon_name()
    addon_version = ayon_api.get_service_addon_version()
    variant = ayon_api.get_default_settings_variant()
    handlers_url = (
        f"addons/{addon_name}/{addon_version}/customProcessorHandlers"
        f"?variant={variant}"
    )
    while not _GlobalContext.stop_event.is_set():
        session = create_session()
        if session is None:
            time.sleep(10)
            continue

        _GlobalContext.session_fail_logged = False

        # Cleanup download root
        cleanup_download_root()

        response = ayon_api.get(handlers_url)
        custom_handlers = []
        if response.status_code == 200:
            custom_handlers = response.data["custom_handlers"]

        handler_paths = get_handler_paths()
        with downloaded_event_handlers(custom_handlers) as custom_handler_dirs:
            handler_paths.extend(custom_handler_dirs)
            logging.info("Starting listen server")
            server = FtrackServer(handler_paths)
            server.run_server(session)
        logging.info("Server stopped.")
    logging.info("Main loop stopped.")


def _cleanup_process():
    """Cleanup timer threads on exit."""
    logging.info("Process stop requested. Terminating process.")
    logging.info("Canceling threading timers.")
    for thread in threading.enumerate():
        if isinstance(thread, threading.Timer):
            thread.cancel()

    logging.info("Stopping main loop.")
    if not _GlobalContext.stop_event.is_set():
        _GlobalContext.stop_event.set()
    session = _GlobalContext.session
    logging.info("Closing ftrack session.")
    if session is not None:
        if session.event_hub.connected is True:
            session.event_hub.disconnect()
        session.close()


def main():
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    try:
        ayon_api.init_service()
        connected = True
    except Exception:
        connected = False

    if not connected:
        logging.warning("Failed to connect to AYON server.")
        # Sleep for 10 seconds, so it is possible to see the message in
        #   docker
        # NOTE: Becuase AYON connection failed, there's no way how to log it
        #   to AYON server (obviously)... So stdout is all we have.
        time.sleep(10)
        sys.exit(1)

    logging.info("Connected to AYON server.")

    # Register interrupt signal
    def signal_handler(sig, frame):
        _cleanup_process()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(_cleanup_process)
    try:
        main_loop()
    finally:
        _cleanup_process()
