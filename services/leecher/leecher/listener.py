import sys
import time
import signal
import ftrack_api

from typing import Callable
from nxtools import logging


def main(session, callback):
    while not session.event_hub.connected:
        time.sleep(0.1)

    session.event_hub.subscribe("topic=*", callback)
    session.event_hub.wait()


def listen(url: str, api_key: str, username: str, callback: Callable):
    session = ftrack_api.Session(
        url,
        api_key,
        username,
        auto_connect_event_hub=True,
    )

    # Register interupt signal
    def signal_handler(sig, frame):
        logging.warning("You pressed Ctrl+C. Process ended.")
        if session.event_hub.connected is True:
            session.event_hub.disconnect()
        session.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    sys.exit(main(session, callback))
