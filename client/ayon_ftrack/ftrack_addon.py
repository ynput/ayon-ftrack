import os
import tempfile
import json

import requests
import ayon_api

from ayon_core.addon import (
    AYONAddon,
    ITrayAddon,
    IPluginPaths,
)
from ayon_core.lib import Logger, run_ayon_launcher_process
from ayon_core.settings import get_project_settings, get_studio_settings
from ayon_core.tools.tray import get_tray_server_url

from ayon_ftrack.lib.credentials import (
    save_credentials,
    get_credentials,
    check_credentials,
)

from .version import __version__

FTRACK_ADDON_DIR = os.path.dirname(os.path.abspath(__file__))
_URL_NOT_SET = object()


class FtrackAddon(
    AYONAddon,
    ITrayAddon,
    IPluginPaths,
):
    name = "ftrack"
    version = __version__

    def initialize(self, settings):
        ftrack_settings = settings[self.name]

        self._settings_ftrack_url = ftrack_settings["ftrack_server"]
        self._ftrack_url = _URL_NOT_SET

        current_dir = os.path.dirname(os.path.abspath(__file__))

        # User event handler paths
        user_event_handlers_paths = [
            os.path.join(current_dir, "event_handlers_user")
        ]

        # Prepare attribute
        self.user_event_handlers_paths = user_event_handlers_paths
        self._tray_wrapper = None

        # TimersManager connection
        self.timers_manager_connector = None
        self._timers_manager_addon = None

    def webserver_initialization(self, web_manager):
        self._tray_wrapper.webserver_initialization(web_manager)

    def get_ftrack_url(self):
        """Resolved ftrack url.

        Resolving is trying to fill missing information in url and tried to
        connect to the server.

        Returns:
            Union[str, None]: Final variant of url or None if url could not be
                reached.
        """

        if self._ftrack_url is _URL_NOT_SET:
            self._ftrack_url = resolve_ftrack_url(
                self._settings_ftrack_url,
                logger=self.log
            )
        return self._ftrack_url

    ftrack_url = property(get_ftrack_url)

    @property
    def settings_ftrack_url(self):
        """ftrack url from settings in a format as it is.

        Returns:
            str: ftrack url from settings.
        """

        return self._settings_ftrack_url

    def get_global_environments(self):
        """ftrack's global environments."""

        return {
            "FTRACK_SERVER": self.ftrack_url
        }

    def get_plugin_paths(self):
        """ftrack plugin paths."""
        plugins_dir = os.path.join(FTRACK_ADDON_DIR, "plugins")
        return {
            "actions": [os.path.join(plugins_dir, "launcher_actions")],
            "publish": [os.path.join(plugins_dir, "publish")]
        }

    def get_launch_hook_paths(self):
        """Implementation for applications launch hooks."""

        return os.path.join(FTRACK_ADDON_DIR, "launch_hooks")

    def connect_with_addons(self, enabled_addons):
        for addon in enabled_addons:
            if not hasattr(addon, "get_ftrack_event_handler_paths"):
                continue

            try:
                paths_by_type = addon.get_ftrack_event_handler_paths()
            except Exception:
                continue

            if not isinstance(paths_by_type, dict):
                continue

            for key, value in paths_by_type.items():
                if not value:
                    continue

                if key not in ("server", "user"):
                    self.log.warning(
                        "Unknown event handlers key \"{}\" skipping.".format(
                            key
                        )
                    )
                    continue

                if not isinstance(value, (list, tuple, set)):
                    value = [value]

                if key == "user":
                    self.user_event_handlers_paths.extend(value)

    def create_ftrack_session(self, **session_kwargs):
        import ftrack_api

        if "server_url" not in session_kwargs:
            session_kwargs["server_url"] = self.ftrack_url

        api_key = session_kwargs.get("api_key")
        api_user = session_kwargs.get("api_user")
        # First look into environments
        # - both AYON tray and ftrack event server should have set them
        # - ftrack event server may crash when credentials are tried to load
        #   from keyring
        if not api_key or not api_user:
            api_key = os.environ.get("FTRACK_API_KEY")
            api_user = os.environ.get("FTRACK_API_USER")

        if not api_key or not api_user:
            cred = get_credentials()
            api_user = cred.get("username")
            api_key = cred.get("api_key")

        session_kwargs["api_user"] = api_user
        session_kwargs["api_key"] = api_key
        return ftrack_api.Session(**session_kwargs)

    def tray_init(self):
        from .tray import FtrackTrayWrapper

        self._tray_wrapper = FtrackTrayWrapper(self)
        # Addon is it's own connector to TimersManager
        self.timers_manager_connector = self

    def tray_menu(self, parent_menu):
        return self._tray_wrapper.tray_menu(parent_menu)

    def tray_start(self):
        return self._tray_wrapper.validate()

    def tray_exit(self):
        self._tray_wrapper.tray_exit()

    def set_credentials_to_env(self, username, api_key):
        os.environ["FTRACK_API_USER"] = username or ""
        os.environ["FTRACK_API_KEY"] = api_key or ""

    # --- TimersManager connection methods ---
    def start_timer(self, data):
        if self._tray_wrapper:
            self._tray_wrapper.start_timer_manager(data)

    def stop_timer(self):
        if self._tray_wrapper:
            self._tray_wrapper.stop_timer_manager()

    def ensure_is_process_ready(self, context):
        """Ensure addon is ready for process.

        Args:
            context (ProcessContext): Process context.

        """
        # Safe to support older ayon-core without 'ProcessPreparationError'
        from ayon_core.addon import ProcessPreparationError
        from ayon_ftrack.common import is_ftrack_enabled_in_settings

        # Do not continue if ftrack is not enabled in settings
        if context.project_name:
            settings = get_project_settings(context.project_name)
        else:
            settings = get_studio_settings()

        if not is_ftrack_enabled_in_settings(settings):
            return

        # Not sure if this should crash or silently continue?
        server_url = self.get_ftrack_url()
        if not server_url:
            return

        username = os.getenv("FTRACK_API_USER")
        api_key = os.getenv("FTRACK_API_KEY")

        if (
            username and api_key
            and check_credentials(username, api_key, server_url)
        ):
            self.set_credentials_to_env(username, api_key)
            return

        username, api_key = self.get_credentials()
        if (
            username and api_key
            and check_credentials(username, api_key, server_url)
        ):
            self.set_credentials_to_env(username, api_key)
            return

        if context.headless:
            raise ProcessPreparationError(
                "ftrack login details are missing. Unable to proceed"
                " without a user interface."
            )

        username, api_key = self._ask_for_credentials(server_url)
        if username and api_key:
            self.set_credentials_to_env(username, api_key)
            # Send the credentials to the running tray
            save_credentials(username, api_key, self.get_ftrack_url())
            tray_url = get_tray_server_url()
            if tray_url:
                requests.post(
                    f"{tray_url}/addons/ftrack/credentials",
                    json={"username": username, "api_key": api_key},
                )
            return

        raise ProcessPreparationError(
            "Unable to connect to ftrack. The process cannot proceed"
            " without this connection."
        )

    def register_timers_manager(self, timers_manager_addon):
        self._timers_manager_addon = timers_manager_addon

    def timer_started(self, data):
        if self._timers_manager_addon is not None:
            self._timers_manager_addon.timer_started(self.id, data)

    def timer_stopped(self):
        if self._timers_manager_addon is not None:
            self._timers_manager_addon.timer_stopped(self.id)

    def get_task_time(self, project_name, folder_path, task_name):
        folder_entity = ayon_api.get_folder_by_path(project_name, folder_path)
        if not folder_entity:
            return 0
        ftrack_id = folder_entity["attrib"].get("ftrackId")
        if not ftrack_id:
            return 0

        session = self.create_ftrack_session()
        query = (
            'select time_logged from Task where name is "{}"'
            ' and parent_id is "{}"'
            ' and project.full_name is "{}"'
        ).format(task_name, ftrack_id, project_name)
        task_entity = session.query(query).first()
        if not task_entity:
            return 0
        hours_logged = (task_entity["time_logged"] / 60) / 60
        return hours_logged

    def get_credentials(self):
        # type: () -> tuple
        """Get local ftrack credentials."""

        cred = get_credentials(self.ftrack_url)
        return cred.get("username"), cred.get("api_key")

    @staticmethod
    def _ask_for_credentials(ftrack_url):
        login_script = os.path.join(
            FTRACK_ADDON_DIR, "tray", "login_dialog.py"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", prefix="ay_ftrack", suffix=".json", delete=False
        ) as tmp:
            json_path = tmp.name
            json.dump({"server_url": ftrack_url}, tmp.file)

        run_ayon_launcher_process(
            "--skip-bootstrap",
            login_script, json_path,
            add_sys_paths=True,
            creationflags=0,

        )

        with open(json_path, "r") as stream:
            data = json.load(stream)
        return data.get("username"), data.get("api_key")


def _check_ftrack_url(url):
    import requests

    try:
        result = requests.get(url, allow_redirects=False)
    except requests.exceptions.RequestException:
        return False

    if (result.status_code != 200 or "FTRACK_VERSION" not in result.headers):
        return False
    return True


def resolve_ftrack_url(url, logger=None):
    """Checks if ftrack server is responding."""

    if logger is None:
        logger = Logger.get_logger(__name__)

    url = url.strip("/ ")
    if not url:
        logger.error("ftrack URL is not set!")
        return None

    if not url.startswith("http"):
        url = "https://" + url

    ftrack_url = None
    if url and _check_ftrack_url(url):
        ftrack_url = url

    if not ftrack_url and not url.endswith("ftrackapp.com"):
        ftrackapp_url = url + ".ftrackapp.com"
        if _check_ftrack_url(ftrackapp_url):
            ftrack_url = ftrackapp_url

    if not ftrack_url and _check_ftrack_url(url):
        ftrack_url = url

    if ftrack_url:
        logger.debug("ftrack server \"{}\" is accessible.".format(ftrack_url))

    else:
        logger.error("ftrack server \"{}\" is not accessible!".format(url))

    return ftrack_url
