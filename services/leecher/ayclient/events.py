from typing import Optional, Any
from nxtools import logging

from ayclient.api import api


def dispatch_event(
    topic: str,
    *,
    sender: Optional[str] = None,
    hash: Optional[str] = None,
    project: Optional[str] = None,
    user: Optional[str] = None,
    dependencies: Optional[list[str]] = None,
    description: Optional[str] = None,
    summary: Optional[dict[str, Any]] = None,
    payload: Optional[dict[str, Any]] = None,
    finished: bool = True,
    store: bool = True,
) -> bool:
    event_data = dict(
        topic=topic,
        sender=sender,
        hash=hash,
        project=project,
        user=user,
        dependencies=dependencies,
        description=description,
        summary=summary if summary is not None else {},
        payload=payload if payload is not None else {},
        finished=finished,
        store=store,
    )
    if api.post("events", json=event_data):
        logging.info(f"Dispatched event {topic}")
        return True
    else:
        logging.error(f"Unable to dispatch event {topic}")
        return False
