import os
import sys
import time
import socket
import threading
import subprocess
import logging
import getpass
import atexit
import collections
import appdirs

import requests
import ftrack_api
import ftrack_api.session
import ftrack_api.cache
import ftrack_api.operation
import ftrack_api._centralized_storage_scenario
import ftrack_api.event
try:
    from weakref import WeakMethod
except ImportError:
    from ftrack_api._weakref import WeakMethod

from ayon_core.lib import get_ayon_launcher_args, Logger


class SocketBaseEventHub(ftrack_api.event.hub.EventHub):
    hearbeat_msg = b"hearbeat"
    heartbeat_callbacks = []

    def __init__(self, *args, **kwargs):
        self.sock = kwargs.pop("sock")
        super(SocketBaseEventHub, self).__init__(*args, **kwargs)

    def _handle_packet(self, code, packet_identifier, path, data):
        """Override `_handle_packet` which extend heartbeat"""
        code_name = self._code_name_mapping[code]
        if code_name == "heartbeat":
            # Reply with heartbeat.
            for callback in self.heartbeat_callbacks:
                callback()

            self.sock.sendall(self.hearbeat_msg)
            return self._send_packet(self._code_name_mapping["heartbeat"])

        return super(SocketBaseEventHub, self)._handle_packet(
            code, packet_identifier, path, data
        )


class CustomEventHubSession(ftrack_api.session.Session):
    '''An isolated session for interaction with an ftrack server.'''
    def __init__(
        self, server_url=None, api_key=None, api_user=None, auto_populate=True,
        plugin_paths=None, cache=None, cache_key_maker=None,
        auto_connect_event_hub=False, schema_cache_path=None,
        plugin_arguments=None, timeout=60, **kwargs
    ):
        self.kwargs = kwargs

        super(ftrack_api.session.Session, self).__init__()
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
        )
        self._closed = False

        if server_url is None:
            server_url = os.environ.get('FTRACK_SERVER')

        if not server_url:
            raise TypeError(
                'Required "server_url" not specified. Pass as argument or set '
                'in environment variable FTRACK_SERVER.'
            )

        self._server_url = server_url

        if api_key is None:
            api_key = os.environ.get(
                'FTRACK_API_KEY',
                # Backwards compatibility
                os.environ.get('FTRACK_APIKEY')
            )

        if not api_key:
            raise TypeError(
                'Required "api_key" not specified. Pass as argument or set in '
                'environment variable FTRACK_API_KEY.'
            )

        self._api_key = api_key

        if api_user is None:
            api_user = os.environ.get('FTRACK_API_USER')
            if not api_user:
                try:
                    api_user = getpass.getuser()
                except Exception:
                    pass

        if not api_user:
            raise TypeError(
                'Required "api_user" not specified. Pass as argument, set in '
                'environment variable FTRACK_API_USER or one of the standard '
                'environment variables used by Python\'s getpass module.'
            )

        self._api_user = api_user

        # Currently pending operations.
        self.recorded_operations = ftrack_api.operation.Operations()

        # AYON change - In new API are operations properties
        new_api = hasattr(self.__class__, "record_operations")

        if new_api:
            self._record_operations = collections.defaultdict(
                lambda: True
            )
            self._auto_populate = collections.defaultdict(
                lambda: auto_populate
            )
        else:
            self.record_operations = True
            self.auto_populate = auto_populate

        self.cache_key_maker = cache_key_maker
        if self.cache_key_maker is None:
            self.cache_key_maker = ftrack_api.cache.StringKeyMaker()

        # Enforce always having a memory cache at top level so that the same
        # in-memory instance is returned from session.
        self.cache = ftrack_api.cache.LayeredCache([
            ftrack_api.cache.MemoryCache()
        ])

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
            self._plugin_paths = os.environ.get(
                'FTRACK_EVENT_PLUGIN_PATH', ''
            ).split(os.pathsep)

        self._discover_plugins(plugin_arguments=plugin_arguments)

        # TODO: Make schemas read-only and non-mutable (or at least without
        # rebuilding types)?
        if schema_cache_path is not False:
            if schema_cache_path is None:
                schema_cache_path = appdirs.user_cache_dir()
                schema_cache_path = os.environ.get(
                    'FTRACK_API_SCHEMA_CACHE_PATH', schema_cache_path
                )

            schema_cache_path = os.path.join(
                schema_cache_path, 'ftrack_api_schema_cache.json'
            )

        self.schemas = self._load_schemas(schema_cache_path)
        self.types = self._build_entity_type_classes(self.schemas)

        ftrack_api._centralized_storage_scenario.register(self)

        self._configure_locations()
        self.event_hub.publish(
            ftrack_api.event.base.Event(
                topic='ftrack.api.session.ready',
                data=dict(
                    session=self
                )
            ),
            synchronous=True
        )

    def _create_event_hub(self):
        return ftrack_api.event.hub.EventHub(
            self._server_url,
            self._api_user,
            self._api_key
        )


class SocketSession(CustomEventHubSession):
    def _create_event_hub(self):
        self.sock = self.kwargs["sock"]
        return self.kwargs["Eventhub"](
            self._server_url,
            self._api_user,
            self._api_key,
            sock=self.sock
        )


class SocketThread(threading.Thread):
    """Thread that checks subprocess of storer of processor of events"""

    MAX_TIMEOUT = 45

    def __init__(self, name, port, filepath, additional_args=None):
        super(SocketThread, self).__init__()
        if additional_args is None:
            additional_args = []
        self.log = Logger.get_logger(self.__class__.__name__)
        self.setName(name)
        self.name = name
        self.port = port
        self.filepath = filepath
        self.additional_args = additional_args

        self.sock = None
        self.subproc = None
        self.connection = None
        self._is_running = False
        self.finished = False

        self._temp_data = {}

    def stop(self):
        self._is_running = False

    def run(self):
        self._is_running = True
        time_socket = time.time()
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock = sock

        # Bind the socket to the port - skip already used ports
        while True:
            try:
                server_address = ("localhost", self.port)
                sock.bind(server_address)
                break
            except OSError:
                self.port += 1

        self.log.debug(
            "Running Socked thread on {}:{}".format(*server_address)
        )

        env = os.environ.copy()
        # AYON executable (with path to start script if not build)
        args = get_ayon_launcher_args(
            # Add `run` command
            "run",
            self.filepath,
            *self.additional_args,
            str(self.port)
        )
        kwargs = {
            "env": env,
            "stdin": subprocess.PIPE
        }
        if not sys.stdout:
            # Redirect to devnull if stdout is None
            kwargs["stdout"] = subprocess.DEVNULL
            kwargs["stderr"] = subprocess.DEVNULL

        self.subproc = subprocess.Popen(args, **kwargs)

        # Listen for incoming connections
        sock.listen(1)
        sock.settimeout(1.0)
        while True:
            if not self._is_running:
                break
            try:
                connection, client_address = sock.accept()
                time_socket = time.time()
                connection.settimeout(1.0)
                self.connection = connection

            except socket.timeout:
                if (time.time() - time_socket) > self.MAX_TIMEOUT:
                    self.log.error("Connection timeout passed. Terminating.")
                    self._is_running = False
                    self.subproc.terminate()
                    break
                continue

            try:
                time_con = time.time()
                # Receive the data in small chunks and retransmit it
                while True:
                    try:
                        if not self._is_running:
                            break
                        data = None
                        try:
                            data = self.get_data_from_con(connection)
                            time_con = time.time()

                        except socket.timeout:
                            if (time.time() - time_con) > self.MAX_TIMEOUT:
                                self.log.error(
                                    "Connection timeout passed. Terminating."
                                )
                                self._is_running = False
                                self.subproc.terminate()
                                break
                            continue

                        except ConnectionResetError:
                            self._is_running = False
                            break

                        self._handle_data(connection, data)

                    except Exception as exc:
                        self.log.error(
                            "Event server process failed", exc_info=True
                        )

            finally:
                # Clean up the connection
                connection.close()
                if self.subproc.poll() is None:
                    self.subproc.terminate()

                self.finished = True

    def get_data_from_con(self, connection):
        return connection.recv(16)

    def _handle_data(self, connection, data):
        if not data:
            return

        connection.sendall(data)
