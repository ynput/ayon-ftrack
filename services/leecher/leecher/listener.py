import sys
import time
import signal
from typing import Any, Callable, Union

import ftrack_api
import ayclient
from nxtools import logging

IGNORE_TOPICS = {
    "ftrack.meta.connected",
    "ftrack.meta.disconnected",
}


def create_event_description(payload: dict[str, Any]):
    uname = payload.get("source", {}).get("user", {}).get("username")
    if not uname:
        return f"Leeched {payload['topic']}"
    return f"Leeched {payload['topic']} by {uname}"


def callback(event):
    if event["topic"] in IGNORE_TOPICS:
        return

    event_data = event._data
    description = create_event_description(event_data)
    ayclient.dispatch_event(
        "ftrack.leech",
        sender=ayclient.config.service_name,
        hash=event_data["id"],
        description=description,
        payload=event_data,
    )
    logging.info("Stored event", event_data["topic"])


def listen_loop(session, callback):
    while not session.event_hub.connected:
        time.sleep(0.1)

    session.event_hub.subscribe("topic=*", callback)
    session.event_hub.wait()


def main(func: Union[Callable, None] = None):
    if func is None:
        func = callback
    settings = ayclient.addon_settings()

    session = ftrack_api.Session(
        settings["ftrack_server"],
        settings["service_settings"]["api_key"],
        settings["service_settings"]["username"],
        auto_connect_event_hub=True,
    )

    # Register interrupt signal
    def signal_handler(sig, frame):
        logging.warning("You pressed Ctrl+C. Terminating process.")
        if session.event_hub.connected is True:
            session.event_hub.disconnect()
        session.close()
        logging.warning("Termination finished.")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    sys.exit(listen_loop(session, func))
