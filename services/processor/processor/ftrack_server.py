import os
import sys
import time
import types
import logging
import traceback
import importlib

import ftrack_api

"""
# Required - Needed for connection to Ftrack
FTRACK_SERVER # Ftrack server e.g. "https://myFtrack.ftrackapp.com"
FTRACK_API_KEY # Ftrack user's API key "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
FTRACK_API_USER # Ftrack username e.g. "user.name"

# Required - Paths to folder with actions
FTRACK_ACTIONS_PATH # Paths to folders where are located actions
    - EXAMPLE: "M:/FtrackApi/../actions/"
FTRACK_EVENTS_PATH # Paths to folders where are located actions
    - EXAMPLE: "M:/FtrackApi/../events/"

# Required - Needed for import included modules
PYTHONPATH # Path to ftrack_api and paths to all modules used in actions
    - path to ftrack_action_handler, etc.
"""


def import_filepath(filepath, module_name=None):
    """Import python file as python module.

    Python 2 and Python 3 compatibility.

    Args:
        filepath(str): Path to python file.
        module_name(str): Name of loaded module. Only for Python 3. By default
            is filled with filename of filepath.
    """
    if module_name is None:
        module_name = os.path.splitext(os.path.basename(filepath))[0]

    # Make sure it is not 'unicode' in Python 2
    module_name = str(module_name)

    # Prepare module object where content of file will be parsed
    module = types.ModuleType(module_name)

    # Use loader so module has full specs
    module_loader = importlib.machinery.SourceFileLoader(
        module_name, filepath
    )
    module_loader.exec_module(module)
    return module


def modules_from_path(folder_path, log=None):
    """Get python scripts as modules from a path.

    Arguments:
        path (str): Path to folder containing python scripts.

    Returns:
        tuple<list, list>: First list contains successfully imported modules
            and second list contains tuples of path and exception.
    """
    crashed = []
    modules = []
    output = (modules, crashed)
    # Just skip and return empty list if path is not set
    if not folder_path:
        return output

    if log is None:
        log = logging.getLogger("modules_from_path")
    # Do not allow relative imports
    if folder_path.startswith("."):
        log.warning((
            "BUG: Relative paths are not allowed for security reasons. {}"
        ).format(folder_path))
        return output

    folder_path = os.path.normpath(folder_path)

    if not os.path.isdir(folder_path):
        log.warning("Not a directory path: {}".format(folder_path))
        return output

    for filename in os.listdir(folder_path):
        # Ignore files which start with underscore
        if filename.startswith("_"):
            continue

        mod_name, mod_ext = os.path.splitext(filename)
        if not mod_ext == ".py":
            continue

        full_path = os.path.join(folder_path, filename)
        if not os.path.isfile(full_path):
            continue

        try:
            module = import_filepath(full_path, mod_name)
            modules.append((full_path, module))

        except Exception:
            crashed.append((full_path, sys.exc_info()))
            log.warning(
                "Failed to load path: \"{0}\"".format(full_path),
                exc_info=True
            )
            continue

    return output


class FtrackServer:
    def __init__(self, handler_paths=None):
        """
            - 'type' is by default set to 'action' - Runs Action server
            - enter 'event' for Event server

            EXAMPLE FOR EVENT SERVER:
                ...
                server = FtrackServer()
                server.run_server()
                ..
        """

        # set Ftrack logging to Warning only - OPTIONAL
        ftrack_log = logging.getLogger("ftrack_api")
        ftrack_log.setLevel(logging.WARNING)

        self.log = logging.getLogger(__name__)

        self.stopped = True
        self.is_running = False

        self.handler_paths = handler_paths or []

    def stop_session(self):
        self.stopped = True
        if self.session.event_hub.connected is True:
            self.session.event_hub.disconnect()
        self.session.close()
        self.session = None

    def set_files(self, paths):
        # Iterate all paths
        register_functions = []
        for path in paths:
            # Try to format path with environments
            try:
                path = path.format(**os.environ)
            except BaseException:
                pass

            # Get all modules with functions
            modules, crashed = modules_from_path(path)
            for filepath, exc_info in crashed:
                self.log.warning("Filepath load crashed {}.\n{}".format(
                    filepath, traceback.format_exception(*exc_info)
                ))

            for filepath, module in modules:
                register_function = None
                for name, attr in module.__dict__.items():
                    if (
                        name == "register"
                        and isinstance(attr, types.FunctionType)
                    ):
                        register_function = attr
                        break

                if not register_function:
                    self.log.warning(
                        "\"{}\" - Missing register method".format(filepath)
                    )
                    continue

                register_functions.append(
                    (filepath, register_function)
                )

        if not register_functions:
            self.log.warning((
                "There are no events with `register` function"
                " in registered paths: \"{}\""
            ).format("| ".join(paths)))

        for filepath, register_func in register_functions:
            try:
                register_func(self.session)
            except Exception:
                self.log.warning(
                    "\"{}\" - register was not successful".format(filepath),
                    exc_info=True
                )

    def set_handler_paths(self, paths):
        self.handler_paths = paths
        if self.is_running:
            self.stop_session()
            self.run_server()

        elif not self.stopped:
            self.run_server()

    def run_server(self, session=None, load_files=True):
        self.stopped = False
        self.is_running = True
        if not session:
            session = ftrack_api.Session(auto_connect_event_hub=True)

        # Wait until session has connected event hub
        if session._auto_connect_event_hub_thread:
            # Use timeout from session (since ftrack-api 2.1.0)
            timeout = getattr(session, "request_timeout", 60)
            started = time.time()
            while not session.event_hub.connected:
                if (time.time() - started) > timeout:
                    raise RuntimeError((
                        "Connection to Ftrack was not created in {} seconds"
                    ).format(timeout))
                time.sleep(0.1)

        self.session = session
        if load_files:
            if not self.handler_paths:
                self.log.warning((
                    "Paths to event handlers are not set."
                    " Ftrack server won't launch."
                ))
                self.is_running = False
                return

            self.set_files(self.handler_paths)

            msg = "Registration of event handlers has finished!"
            self.log.info(len(msg) * "*")
            self.log.info(msg)

        print(self.handler_paths)

        # keep event_hub on session running
        self.session.event_hub.wait()
        self.is_running = False
