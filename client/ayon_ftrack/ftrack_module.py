import os

import click

from openpype.modules import (
    OpenPypeModule,
    ITrayModule,
    IPluginPaths,
)
from openpype.lib import Logger

FTRACK_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_URL_NOT_SET = object()


class FtrackModule(
    OpenPypeModule,
    ITrayModule,
    IPluginPaths,
):
    name = "ftrack"
    enabled = True

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
        self.tray_module = None

        # TimersManager connection
        self.timers_manager_connector = None
        self._timers_manager_module = None

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
        """Ftrack url from settings in a format as it is.

        Returns:
            str: Ftrack url from settings.
        """

        return self._settings_ftrack_url

    def get_global_environments(self):
        """Ftrack's global environments."""

        return {
            "FTRACK_SERVER": self.ftrack_url
        }

    def get_plugin_paths(self):
        """Ftrack plugin paths."""
        return {
            "publish": [os.path.join(FTRACK_MODULE_DIR, "plugins", "publish")]
        }

    def get_launch_hook_paths(self):
        """Implementation for applications launch hooks."""

        return os.path.join(FTRACK_MODULE_DIR, "launch_hooks")

    def modify_application_launch_arguments(self, application, env):
        if not application.use_python_2:
            return

        self.log.info("Adding Ftrack Python 2 packages to PYTHONPATH.")

        # Prepare vendor dir path
        python_2_vendor = os.path.join(FTRACK_MODULE_DIR, "python2_vendor")

        # Add Python 2 modules
        python_paths = [
            # `python-ftrack-api`
            os.path.join(python_2_vendor, "ftrack-python-api", "source"),
        ]

        # Load PYTHONPATH from current launch context
        python_path = env.get("PYTHONPATH")
        if python_path:
            python_paths.append(python_path)

        # Set new PYTHONPATH to launch context environments
        env["PYTHONPATH"] = os.pathsep.join(python_paths)

    def connect_with_modules(self, enabled_modules):
        for module in enabled_modules:
            if not hasattr(module, "get_ftrack_event_handler_paths"):
                continue

            try:
                paths_by_type = module.get_ftrack_event_handler_paths()
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
            from .lib import credentials
            cred = credentials.get_credentials()
            api_user = cred.get("username")
            api_key = cred.get("api_key")

        session_kwargs["api_user"] = api_user
        session_kwargs["api_key"] = api_key
        return ftrack_api.Session(**session_kwargs)

    def tray_init(self):
        from .tray import FtrackTrayWrapper

        self.tray_module = FtrackTrayWrapper(self)
        # Module is it's own connector to TimersManager
        self.timers_manager_connector = self

    def tray_menu(self, parent_menu):
        return self.tray_module.tray_menu(parent_menu)

    def tray_start(self):
        return self.tray_module.validate()

    def tray_exit(self):
        self.tray_module.tray_exit()

    def set_credentials_to_env(self, username, api_key):
        os.environ["FTRACK_API_USER"] = username or ""
        os.environ["FTRACK_API_KEY"] = api_key or ""

    # --- TimersManager connection methods ---
    def start_timer(self, data):
        if self.tray_module:
            self.tray_module.start_timer_manager(data)

    def stop_timer(self):
        if self.tray_module:
            self.tray_module.stop_timer_manager()

    def register_timers_manager(self, timer_manager_module):
        self._timers_manager_module = timer_manager_module

    def timer_started(self, data):
        if self._timers_manager_module is not None:
            self._timers_manager_module.timer_started(self.id, data)

    def timer_stopped(self):
        if self._timers_manager_module is not None:
            self._timers_manager_module.timer_stopped(self.id)

    def get_task_time(self, project_name, asset_name, task_name):
        session = self.create_ftrack_session()
        query = (
            'Task where name is "{}"'
            ' and parent.name is "{}"'
            ' and project.full_name is "{}"'
        ).format(task_name, asset_name, project_name)
        task_entity = session.query(query).first()
        if not task_entity:
            return 0
        hours_logged = (task_entity["time_logged"] / 60) / 60
        return hours_logged

    def get_credentials(self):
        # type: () -> tuple
        """Get local Ftrack credentials."""
        from .lib import credentials

        cred = credentials.get_credentials(self.ftrack_url)
        return cred.get("username"), cred.get("api_key")

    def cli(self, click_group):
        click_group.add_command(cli_main)


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
    """Checks if Ftrack server is responding."""

    if logger is None:
        logger = Logger.get_logger(__name__)

    url = url.strip("/ ")
    if not url:
        logger.error("Ftrack URL is not set!")
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
        logger.debug("Ftrack server \"{}\" is accessible.".format(ftrack_url))

    else:
        logger.error("Ftrack server \"{}\" is not accessible!".format(url))

    return ftrack_url


@click.group(FtrackModule.name, help="Ftrack module related commands.")
def cli_main():
    pass
