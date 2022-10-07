import ayclient

from typing import Any
from nxtools import logging

from .listener import listen

IGNORE_TOPICS = {}

def create_description(payload: dict[str, Any]):
    uname = payload.get("source", {}).get("user", {}).get("username") or "somebody"
    if not uname:
        return "Somewhat happended"
    return f"{uname} did something"


def callback(event):
    if event["topic"] in IGNORE_TOPICS:
        return

    event_data = event._data
    description = create_description(event_data)
    ayclient.dispatch_event(
        "ftrack.leech",
        sender=ayclient.config.service_name,
        hash=event_data["id"],
        description=description,
        payload=event_data,
    )
    logging.info("Stored event", event_data["topic"])


if __name__ == "__main__":
    settings = ayclient.addon_settings()
    listen(
        url=settings["server"],
        api_key=settings["key"],
        username=settings["user"],
        callback=callback,
    )
