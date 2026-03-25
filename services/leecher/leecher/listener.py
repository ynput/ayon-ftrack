import sys
import time
import signal
import logging
import threading
import traceback
from typing import Any

import ftrack_api
import ayon_api
from ayon_api.graphql_queries import events_graphql_query
from ayon_api.exceptions import HTTPRequestError

IGNORE_TOPICS = {
    "ftrack.meta.connected",
    "ftrack.meta.disconnected",
}

log = logging.getLogger(__name__)


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

    try:
        ayon_api.dispatch_event(
            "ftrack.leech",
            sender=ayon_api.ServiceContext.service_name,
            event_hash=event_data["id"],
            description=description,
            payload=event_data,
        )
    except HTTPRequestError as exc:
        if exc.response.status_code != 409:
            raise
        log.info(f"Event {event_data['topic']} already stored.")

    log.info(f"Stored event {event_data['topic']}")


def _has_pending_start_event() -> bool:
    query = events_graphql_query({"id"}, None, False)
    events_field = query.get_field_by_path("events")
    try:
        text_filter_var = query.add_variable("textFilter", "String!")
        events_field.set_filter("filter", text_filter_var)
    except KeyError:
        pass
    try:
        last_n_var = query.add_variable("lastFilter", "Int!")
        events_field.set_filter("last", last_n_var)
    except KeyError:
        pass
    events_field.set_limit(1)

    query.set_variable_value("eventTopics", ["ftrack.leech"])
    query.set_variable_value("lastFilter", 1)
    query.set_variable_value(
        "textFilter", "ayon.ftrack.leecher.started.ynternal"
    )

    con = ayon_api.get_server_api_connection()
    src_event = None
    for parsed_data in query.continuous_query(con):
        for event in parsed_data["events"]:
            src_event = event

    if src_event is None:
        return False

    response = ayon_api.post(
        "query",
        entity="event",
        filter={
            "conditions": [
                {"key": "topic", "value": "ftrack.proc"},
                {"key": "depends_on", "value": src_event["id"]},
            ]
        },
        limit=1,
    )
    response.raise_for_status()
    if response.data:
        return False
    return True


def _trigger_leecher_started_event(session: ftrack_api.Session):
    user = session.query(
        f"User where username is \"{session.api_user}\""
    ).one()
    user_data = {
        "username": user["username"],
        "id": user["id"]
    }
    # Publish internal ftrack event directly to AYON (skip ftrack)
    # - do not trigger the event if there already is one unprocessed
    if _has_pending_start_event():
        return

    print("Triggering internal ftrack event to AYON.")
    topic = "ayon.ftrack.leecher.started.ynternal"
    event = ftrack_api.event.base.Event(
        topic=topic,
        data={},
        source=dict(user=user_data)
    )
    event_data = event._data

    ayon_api.dispatch_event(
        "ftrack.leech",
        sender=ayon_api.ServiceContext.service_name,
        event_hash=event_data["id"],
        description=topic,
        payload=event_data,
    )


class _GlobalContext:
    stop_event = threading.Event()
    session = None
    session_fail_logged = 0


def get_service_label() -> str:
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
            "Missing ftrack Username or ftrack API in settings."
            f" Please check your settings of {get_service_label()}."
        )

    _GlobalContext.session = ftrack_api.Session(
        ftrack_url.strip("/ "),
        api_key,
        username,
        auto_connect_event_hub=True,
    )


def create_session():
    tb_content = None
    error_summary = "Failed to create ftrack session."
    try:
        error_message = _create_session()
    except ftrack_api.exception.ServerError:
        error_message = (
            "ftrack Username or ftrack API in settings are not valid."
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
        "description": "ftrack leecher error",
        "payload": {
            "message": error_message,
        }
    }
    if tb_content is None:
        _GlobalContext.session_fail_logged = 2
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

        while not session.event_hub.connected:
            time.sleep(0.1)

        session.event_hub.subscribe("topic=*", callback)
        _trigger_leecher_started_event(session)
        session.event_hub.wait()


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
