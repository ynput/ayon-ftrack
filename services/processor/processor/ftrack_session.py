import os
import logging
import getpass
import atexit
import threading
import time
import queue
import collections

import appdirs
import requests
import ftrack_api
import ftrack_api.session
import ftrack_api.cache
import ftrack_api.operation
import ftrack_api._centralized_storage_scenario
import ftrack_api.event

from weakref import WeakMethod

from ayon_api import (
    get_service_addon_name,
    enroll_event_job,
    get_event,
    update_event,
)


class ProcessEventHub(ftrack_api.event.hub.EventHub):
    _server_con = None

    def get_next_ftrack_event(self):
        return enroll_event_job(
            source_topic="ftrack.leech",
            target_topic="ftrack.proc",
            sender=get_service_addon_name(),
            description="Event processing",
            sequential=True,
        )

    def finish_job(self, job):
        event_id = job["id"]
        source_id = job["dependsOn"]
        source_event = get_event(event_id)
        print(f"Processing event... {source_id}")

        description = f"Processed {source_event['description']}"

        update_event(
            event_id,
            sender=get_service_addon_name(),
            status="finished",
            description=description,
        )

    def load_event_from_jobs(self):
        job = self.get_next_ftrack_event()
        if not job:
            return False

        src_job = get_event(job["dependsOn"])
        ftrack_event = ftrack_api.event.base.Event(**src_job["payload"])
        self._event_queue.put((ftrack_event, job))
        return True

    def wait(self, duration=None):
        """Overridden wait
        Event are loaded from Mongo DB when queue is empty. Handled event is
        set as processed in Mongo DB.
        """

        started = time.time()
        while True:
            job = None
            try:
                item = self._event_queue.get(timeout=0.1)
                if isinstance(item, tuple):
                    event, job = item
                else:
                    event = item

            except queue.Empty:
                if not self.load_event_from_jobs():
                    time.sleep(0.1)
                continue

            self._handle(event)

            if job is not None:
                self.finish_job(job)

            # Additional special processing of events.
            if event["topic"] == "ftrack.meta.disconnected":
                break

            if duration is not None:
                if (time.time() - started) > duration:
                    break

    def _handle_packet(self, code, packet_identifier, path, data):
        """Override `_handle_packet` which skip events and extend heartbeat"""
        code_name = self._code_name_mapping[code]
        if code_name == "event":
            return

        return super()._handle_packet(code, packet_identifier, path, data)


class CustomEventHubSession(ftrack_api.session.Session):
    """An isolated session for interaction with an ftrack server."""

    def __init__(
        self,
        server_url=None,
        api_key=None,
        api_user=None,
        auto_populate=True,
        plugin_paths=None,
        cache=None,
        cache_key_maker=None,
        auto_connect_event_hub=False,
        schema_cache_path=None,
        plugin_arguments=None,
        timeout=60,
        **kwargs,
    ):
        self.kwargs = kwargs

        super(ftrack_api.session.Session, self).__init__()
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self._closed = False

        if server_url is None:
            server_url = os.environ.get("FTRACK_SERVER")

        if not server_url:
            raise TypeError(
                'Required "server_url" not specified. Pass as argument or set '
                "in environment variable FTRACK_SERVER."
            )

        self._server_url = server_url

        if api_key is None:
            api_key = os.environ.get(
                "FTRACK_API_KEY",
                # Backwards compatibility
                os.environ.get("FTRACK_APIKEY"),
            )

        if not api_key:
            raise TypeError(
                'Required "api_key" not specified. Pass as argument or set in '
                "environment variable FTRACK_API_KEY."
            )

        self._api_key = api_key

        if api_user is None:
            api_user = os.environ.get("FTRACK_API_USER")
            if not api_user:
                try:
                    api_user = getpass.getuser()
                except Exception:
                    pass

        if not api_user:
            raise TypeError(
                'Required "api_user" not specified. Pass as argument, set in '
                "environment variable FTRACK_API_USER or one of the standard "
                "environment variables used by Python's getpass module."
            )

        self._api_user = api_user

        # Currently pending operations.
        self.recorded_operations = ftrack_api.operation.Operations()

        # AYON change - In new API are operations properties
        new_api = hasattr(self.__class__, "record_operations")

        if new_api:
            self._record_operations = collections.defaultdict(lambda: True)
            self._auto_populate = collections.defaultdict(lambda: auto_populate)
        else:
            self.record_operations = True
            self.auto_populate = auto_populate

        self.cache_key_maker = cache_key_maker
        if self.cache_key_maker is None:
            self.cache_key_maker = ftrack_api.cache.StringKeyMaker()

        # Enforce always having a memory cache at top level so that the same
        # in-memory instance is returned from session.
        self.cache = ftrack_api.cache.LayeredCache([ftrack_api.cache.MemoryCache()])

        if cache is not None:
            if callable(cache):
                cache = cache(self)

            if cache is not None:
                self.cache.caches.append(cache)

        if new_api:
            self.merge_lock = threading.RLock()

        self._managed_request = None
        self._request = requests.Session()
        self._request.auth = ftrack_api.session.SessionAuthentication(
            self._api_key, self._api_user
        )
        self.request_timeout = timeout

        # Fetch server information and in doing so also check credentials.
        self._server_information = self._fetch_server_information()

        # Now check compatibility of server based on retrieved information.
        self.check_server_compatibility()

        # Construct event hub and load plugins.
        self._event_hub = self._create_event_hub()

        self._auto_connect_event_hub_thread = None
        if auto_connect_event_hub:
            # Connect to event hub in background thread so as not to block main
            # session usage waiting for event hub connection.
            self._auto_connect_event_hub_thread = threading.Thread(
                target=self._event_hub.connect
            )
            self._auto_connect_event_hub_thread.daemon = True
            self._auto_connect_event_hub_thread.start()

        # Register to auto-close session on exit.
        atexit.register(WeakMethod(self.close))

        self._plugin_paths = plugin_paths
        if self._plugin_paths is None:
            self._plugin_paths = os.environ.get("FTRACK_EVENT_PLUGIN_PATH", "").split(
                os.pathsep
            )

        self._discover_plugins(plugin_arguments=plugin_arguments)

        # TODO: Make schemas read-only and non-mutable (or at least without
        # rebuilding types)?
        if schema_cache_path is not False:
            if schema_cache_path is None:
                schema_cache_path = appdirs.user_cache_dir()
                schema_cache_path = os.environ.get(
                    "FTRACK_API_SCHEMA_CACHE_PATH", schema_cache_path
                )

            schema_cache_path = os.path.join(
                schema_cache_path, "ftrack_api_schema_cache.json"
            )

        self.schemas = self._load_schemas(schema_cache_path)
        self.types = self._build_entity_type_classes(self.schemas)

        ftrack_api._centralized_storage_scenario.register(self)

        self._configure_locations()
        self.event_hub.publish(
            ftrack_api.event.base.Event(
                topic="ftrack.api.session.ready", data=dict(session=self)
            ),
            synchronous=True,
        )

    def _create_event_hub(self):
        return ftrack_api.event.hub.EventHub(
            self._server_url, self._api_user, self._api_key
        )


class AYONServerSession(CustomEventHubSession):
    def _create_event_hub(self):
        return ProcessEventHub(self._server_url, self._api_user, self._api_key)
