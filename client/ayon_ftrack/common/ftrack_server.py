import os
import time
import logging
import traceback
import types
import inspect

import ftrack_api

from .python_module_tools import modules_from_path
from .event_handlers import BaseHandler


class FtrackServer:
    """Helper wrapper to run ftrack server with event handlers.

    Handlers are discovered based on a list of paths. Each path is scanned for
    python files which are imported as modules. Each module is checked for
    'register' function or classes inheriting from 'BaseHandler'. If class
    inheriting from 'BaseHandler' is found it is instantiated and 'register'
    method is called. If 'register' function is found it is called with
    ftrack session as argument and 'BaseHandler' from the file are ignored.

    Function 'register' tells discovery system to skip looking for classes.

    Classes that start with '_' are ignored. It is possible to define
    attribute `__ignore_handler_class = True` on class definition to mark
    a "base class" that will be ignored on discovery, so you can safely import
    custom base classes in the files.
    """
    def __init__(self, handler_paths=None):
        # set ftrack logging to Warning only - OPTIONAL
        ftrack_log = logging.getLogger("ftrack_api")
        ftrack_log.setLevel(logging.WARNING)

        self.log = logging.getLogger(__name__)

        self._stopped = True
        self._is_running = False

        if handler_paths is None:
            handler_paths = []

        self._handler_paths = handler_paths

        self._session = None
        self._cached_modules = []
        self._cached_objects = []

    def stop_session(self):
        session = self._session
        self._session = None
        self._stopped = True
        if session.event_hub.connected is True:
            session.event_hub.disconnect()
        session.close()

    def get_session(self):
        return self._session

    def get_handler_paths(self):
        return self._handler_paths

    def set_handler_paths(self, paths):
        if self._is_running:
            raise ValueError(
                "Cannot change handler paths when server is running."
            )
        self._handler_paths = paths

    session = property(get_session)
    handler_paths = property(get_handler_paths, set_handler_paths)

    def run_server(self, session=None):
        if self._is_running:
            raise ValueError("Server is already running.")
        self._stopped = False
        self._is_running = True
        if not session:
            session = ftrack_api.Session(auto_connect_event_hub=True)

        # Wait until session has connected event hub
        if session._auto_connect_event_hub_thread:
            # Use timeout from session (since ftrack-api 2.1.0)
            timeout = getattr(session, "request_timeout", 60)
            self.log.info("Waiting for event hub to connect")
            started = time.time()
            while not session.event_hub.connected:
                if (time.time() - started) > timeout:
                    raise RuntimeError((
                        "Connection to ftrack was not created in {} seconds"
                    ).format(timeout))
                time.sleep(0.1)

        elif not session.event_hub.connected:
            self.log.info("Connecting event hub")
            session.event_hub.connect()

        self._session = session
        if not self._handler_paths:
            self.log.warning((
                "Paths to event handlers are not set."
                " ftrack server won't launch."
            ))
            self._is_running = False
            return

        self._load_handlers()

        msg = "Registration of event handlers has finished!"
        self.log.info(len(msg) * "*")
        self.log.info(msg)

        # keep event_hub on session running
        try:
            session.event_hub.wait()
        finally:
            for handler in self._cached_objects:
                try:
                    handler.cleanup()
                except Exception:
                    self.log.warning(
                        "Failed to cleanup handler", exc_info=True
                    )
            self._is_running = False
            self._cached_modules = []

    def _load_handlers(self):
        register_functions = []
        handler_classes = []

        # Iterate all paths
        paths = self._handler_paths
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
                    filepath, "".join(traceback.format_exception(*exc_info))
                ))

            for filepath, module in modules:
                self._cached_modules.append(module)
                register_function = getattr(module, "register", None)
                if register_function is not None:
                    if isinstance(register_function, types.FunctionType):
                        register_functions.append(
                            (filepath, register_function)
                        )
                    else:
                        self.log.warning(
                            f"\"{filepath}\""
                            " - Found 'register' but it is not a function."
                        )
                    continue

                for attr_name in dir(module):
                    if attr_name.startswith("_"):
                        self.log.debug(
                            f"Skipping private class '{attr_name}'"
                        )
                        continue

                    attr = getattr(module, attr_name, None)
                    if (
                        not inspect.isclass(attr)
                        or not issubclass(attr, BaseHandler)
                        or attr.ignore_handler_class()
                    ):
                        continue

                    if inspect.isabstract(attr):
                        self.log.warning(
                            f"Skipping abstract class '{attr_name}'."
                        )
                        continue
                    handler_classes.append(attr)

                if not handler_classes:
                    self.log.warning(
                        f"\"{filepath}\""
                        " - No 'register' function"
                        " or 'BaseHandler' classes found."
                    )

        if not register_functions and not handler_classes:
            self.log.warning((
                "There are no files with `register` function or 'BaseHandler'"
                " classes in registered paths:\n- \"{}\""
            ).format("- \n".join(paths)))

        for filepath, register_func in register_functions:
            try:
                register_func(self._session)
            except Exception:
                self.log.warning(
                    f"\"{filepath}\" - register was not successful",
                    exc_info=True
                )

        for handler_class in handler_classes:
            try:
                obj = handler_class(self._session)
                obj.register()
                self._cached_objects.append(obj)

            except Exception:
                self.log.warning(
                    f"\"{handler_class}\" - register was not successful",
                    exc_info=True
                )
