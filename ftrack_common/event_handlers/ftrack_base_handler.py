import os
import tempfile
import json
import functools
import uuid
import datetime
import traceback
import time
import logging
from abc import ABCMeta, abstractmethod

import ftrack_api

from ayon_api import get_bundle_settings, get_project


class BaseHandler(object, metaclass=ABCMeta):
    """Base class for handling ftrack events.

    Attributes:
        enabled (bool): Is handler enabled.
        priority (int): Priority of handler processing. The lower value is the
            earlier is handler processed.
        handler_type (str): Has only debugging purposes.

    Args:
        session (ftrack_api.Session): Connected ftrack session.
    """

    _log = None
    _process_id = None
    # Default priority is 100
    enabled = True
    priority = 100
    handler_type = "Base"
    _handler_label = None

    def __init__(self, session):
        if not isinstance(session, ftrack_api.session.Session):
            raise TypeError(
                "Expected 'ftrack_api.Session' object got '{}'".format(
                    str(type(session))))

        self._session = session

        self.register = self.register_wrapper(self.register)

    @staticmethod
    def join_filter_values(values):
        return ",".join({'"{}"'.format(value) for value in values})

    @classmethod
    def join_query_keys(cls, keys):
        return cls.join_filter_values(keys)

    @property
    def log(self):
        """Quick access to logger.

        Returns:
            logging.Logger: Logger that can be used for logging of handler.
        """

        if self._log is None:
            # TODO better logging mechanism
            self._log = logging.getLogger(self.__class__.__name__)
            self._log.setLevel(logging.DEBUG)
        return self._log

    @property
    def handler_label(self):
        if self._handler_label is None:
            self._handler_label = self.__class__.__name__
        return self._handler_label

    @property
    def session(self):
        """Fast access to session.

        Returns:
            session (ftrack_api.Session): Session which is source of events.
        """

        return self._session

    def reset_session(self):
        """Reset session cache."""

        self.session.reset()

    @staticmethod
    def process_identifier():
        """Helper property to have unified access to process id.

        Todos:
            Use some global approach rather then implementation on
                'BaseEntity'.
        """

        if not BaseHandler._process_id:
            BaseHandler._process_id = str(uuid.uuid4())
        return BaseHandler._process_id

    @abstractmethod
    def register(self):
        """Subscribe to event topics."""

        pass

    def register_wrapper(self, func):
        @functools.wraps(func)
        def wrapper_register(*args, **kwargs):
            if not self.enabled:
                return

            try:
                start_time = time.perf_counter()
                func(*args, **kwargs)
                end_time = time.perf_counter()
                run_time = end_time - start_time
                self.log.info((
                    "{} \"{}\" - Registered successfully ({:.4f}sec)"
                ).format(self.handler_type, self.handler_label, run_time))

            except NotImplementedError:
                self.log.error((
                    "{} \"{}\" - Register method is not implemented"
                ).format(self.handler_type, self.handler_label))

            except Exception as exc:
                self.log.error("{} \"{}\" - Registration failed ({})".format(
                    self.handler_type, self.handler_label, str(exc)
                ))
        return wrapper_register

    def _get_entities(self, event, session=None, ignore=None):
        entities = []
        selection = event["data"].get("selection")
        if not selection:
            return entities

        if ignore is None:
            ignore = []
        elif isinstance(ignore, str):
            ignore = [ignore]

        filtered_selection = []
        for entity in selection:
            if entity["entityType"] not in ignore:
                filtered_selection.append(entity)

        if not filtered_selection:
            return entities

        if session is None:
            session = self.session
            session._local_cache.clear()

        for entity in filtered_selection:
            entities.append(session.get(
                self._get_entity_type(entity, session),
                entity.get("entityId")
            ))

        return entities

    def _get_entity_type(self, entity, session=None):
        """Translate entity type so it can be used with API.

        Todos:
            Use object id rather.
        """

        # Get entity type and make sure it is lower cased. Most places except
        # the component tab in the Sidebar will use lower case notation.
        entity_type = entity.get("entityType").replace("_", "").lower()

        if session is None:
            session = self.session

        for schema in session.schemas:
            alias_for = schema.get("alias_for")

            if (
                alias_for
                and isinstance(alias_for, str)
                and alias_for.lower() == entity_type
            ):
                return schema["id"]

        for schema in self.session.schemas:
            if schema["id"].lower() == entity_type:
                return schema["id"]

        raise ValueError(
            "Unable to translate entity type: {0}.".format(entity_type)
        )

    def show_message(self, event, message, success=False):
        """Shows message to user who triggered event.

        Args:
            event (ftrack_api.Event): Event used for source of user id.
            message (str): Message that will be shown to user.
            success (bool): Define type (color) of message. False -> red color.
        """

        if not isinstance(success, bool):
            success = False

        try:
            message = str(message)
        except Exception:
            return

        user_id = event["source"]["user"]["id"]
        target = (
            "applicationId=ftrack.client.web and user.id=\"{}\""
        ).format(user_id)
        self.session.event_hub.publish(
            ftrack_api.event.base.Event(
                topic="ftrack.action.trigger-user-interface",
                data={
                    "type": "message",
                    "success": success,
                    "message": message
                },
                target=target
            ),
            on_error="ignore"
        )

    def show_interface(
        self,
        items,
        title="",
        user_id=None,
        user=None,
        event=None,
        username=None,
        submit_btn_label=None
    ):
        """Shows ftrack widgets interface to user.

        Interface is shown to a user. To identify user one of arguments must be
        passed: 'user_id', 'user', 'event', 'username'.

        Args:
            items (List[Dict[str, Any]]) Interface items (their structure is
                defined by ftrack documentation).
            title (str): Title of shown widget.
            user_id (str): User id.
            user (Any): Object of ftrack user (queried using ftrack api
                session).
            event (ftrack_api.Event): Event which can be used as source for
                user id.
            username (str): Username of user to get it's id. This is slowest
                way how user id is received.
            submit_btn_label (str): Label of submit button in ftrack widget.
        """

        if user_id:
            pass

        elif user:
            user_id = user["id"]

        elif username:
            user = self.session.query(
                "User where username is \"{}\"".format(username)
            ).first()
            if not user:
                raise ValueError((
                    "Ftrack user with username \"{}\" was not found!"
                ).format(username))

            user_id = user["id"]

        elif event:
            user_id = event["source"]["user"]["id"]

        if not user_id:
            return

        target = (
            "applicationId=ftrack.client.web and user.id=\"{}\""
        ).format(user_id)

        event_data = {
            "type": "widget",
            "items": items,
            "title": title
        }
        if submit_btn_label:
            event_data["submit_button_label"] = submit_btn_label

        self.session.event_hub.publish(
            ftrack_api.event.base.Event(
                topic="ftrack.action.trigger-user-interface",
                data=event_data,
                target=target
            ),
            on_error="ignore"
        )

    def show_interface_from_dict(self, messages, *args, **kwargs):
        # TODO Find out how and where is this used
        if not messages:
            self.log.debug("No messages to show! (messages dict is empty)")
            return
        items = []
        splitter = {"type": "label", "value": "---"}
        first = True
        for key, value in messages.items():
            if not first:
                items.append(splitter)
            else:
                first = False

            items.append({"type": "label", "value": "<h3>{}</h3>".format(key)})
            if isinstance(value, list):
                for item in value:
                    message = {
                        "type": "label", "value": "<p>{}</p>".format(item)
                    }
                    items.append(message)
            else:
                message = {"type": "label", "value": "<p>{}</p>".format(value)}
                items.append(message)

        self.show_interface(items, *args, **kwargs)

    def trigger_action(
        self,
        action_identifier,
        event=None,
        session=None,
        selection=None,
        user_data=None,
        topic="ftrack.action.launch",
        additional_event_data={},
        on_error="ignore"
    ):
        self.log.debug(
            "Triggering action \"{}\" Begins".format(action_identifier))

        if not session:
            session = self.session

        # Getting selection and user data
        if event:
            if selection is None:
                selection = event.get("data", {}).get("selection")
            if user_data is None:
                user_data = event.get("source", {}).get("user")

        # Without selection and user data skip triggering
        msg = "Can't trigger \"{}\" action without {}."
        if selection is None:
            self.log.error(msg.format(action_identifier, "selection"))
            return

        if user_data is None:
            self.log.error(msg.format(action_identifier, "user data"))
            return

        event_data = {
            "actionIdentifier": action_identifier,
            "selection": selection
        }

        # Add additional data
        if additional_event_data:
            event_data.update(additional_event_data)

        # Create and trigger event
        session.event_hub.publish(
            ftrack_api.event.base.Event(
                topic=topic,
                data=event_data,
                source={"user": user_data}
            ),
            on_error=on_error
        )
        self.log.debug(
            "Action \"{}\" triggered".format(action_identifier))

    def trigger_event(
        self,
        topic,
        event_data=None,
        session=None,
        source=None,
        event=None,
        on_error="ignore"
    ):
        if session is None:
            session = self.session

        if not source and event:
            source = event.get("source")

        if event_data is None:
            event_data = {}
        # Create and trigger event
        event = ftrack_api.event.base.Event(
            topic=topic,
            data=event_data,
            source=source
        )
        session.event_hub.publish(event, on_error=on_error)

        self.log.debug((
            "Publishing event: {}"
        ).format(str(event.__dict__)))

    def get_project_from_entity(self, entity, session=None):
        low_entity_type = entity.entity_type.lower()
        if low_entity_type == "project":
            return entity

        if "project" in entity:
            # reviewsession, task(Task, Shot, Sequence,...)
            return entity["project"]

        if low_entity_type == "filecomponent":
            entity = entity["version"]
            low_entity_type = entity.entity_type.lower()

        if low_entity_type == "assetversion":
            asset = entity["asset"]
            parent = None
            if asset:
                parent = asset["parent"]

            if parent:
                if parent.entity_type.lower() == "project":
                    return parent

                if "project" in parent:
                    return parent["project"]

        project_data = entity["link"][0]

        if session is None:
            session = self.session
        return session.query(
            "Project where id is {}".format(project_data["id"])
        ).one()

    def get_project_settings_from_event(self, event, project_name):
        """Load or fill AYON's project settings from event data.

        Project data are stored by ftrack id because in most cases it is
        easier to access project id than project name.

        Args:
            event (ftrack_api.Event): Processed event by session.
            project_name (str): Project name.
        """

        project_settings_by_id = event["data"].get("project_settings")
        if not project_settings_by_id:
            project_settings_by_id = {}
            event["data"]["project_settings"] = project_settings_by_id

        project_settings = project_settings_by_id.get(project_name)
        if not project_settings:
            # NOTE there is no safe way how to get project settings if project
            #   does not exist on AYON server.
            # TODO Should we somehow find out if ftrack is enabled for the
            #   project?
            # TODO how to find out which bundle should be used?
            if get_project(project_name):
                bundle_settings = get_bundle_settings(
                    project_name=project_name
                )
            else:
                bundle_settings = get_bundle_settings()
            project_settings = {
                addon_data["name"]: addon_data["settings"]
                for addon_data in bundle_settings["addons"]
            }
            event["data"]["project_settings"][project_name] = project_settings
        return project_settings

    @staticmethod
    def get_entity_path(entity):
        """Return full hierarchical path to entity."""

        return "/".join(
            [ent["name"] for ent in entity["link"]]
        )

    @classmethod
    def add_traceback_to_job(
        cls, job, session, exc_info,
        description=None,
        component_name=None,
        job_status=None
    ):
        """Add traceback file to a job.

        Args:
            job (JobEntity): Entity of job where file should be able to
                download (Created or queried with passed session).
            session (Session): Ftrack session which was used to query/create
                entered job.
            exc_info (tuple): Exception info (e.g. from `sys.exc_info()`).
            description (str): Change job description to describe what
                happened. Job description won't change if not passed.
            component_name (str): Name of component and default name of
                downloaded file. Class name and current date time are used if
                not specified.
            job_status (str): Status of job which will be set. By default is
                set to 'failed'.
        """

        if description:
            job_data = {
                "description": description
            }
            job["data"] = json.dumps(job_data)

        if not job_status:
            job_status = "failed"

        job["status"] = job_status

        # Create temp file where traceback will be stored
        with tempfile.NamedTemporaryFile(
            mode="w", prefix="ayon_ftrack_", suffix=".txt", delete=False
        ) as temp_obj:
            temp_filepath = temp_obj.name

        # Store traceback to file
        result = traceback.format_exception(*exc_info)
        with open(temp_filepath, "w") as temp_file:
            temp_file.write("".join(result))

        # Upload file with traceback to ftrack server and add it to job
        if not component_name:
            component_name = "{}_{}".format(
                cls.__name__,
                datetime.datetime.now().strftime("%y-%m-%d-%H%M")
            )
        cls.add_file_component_to_job(
            job, session, temp_filepath, component_name
        )
        # Delete temp file
        os.remove(temp_filepath)

    @staticmethod
    def add_file_component_to_job(job, session, filepath, basename=None):
        """Add filepath as downloadable component to job.

        Args:
            job (JobEntity): Entity of job where file should be able to
                download (Created or queried with passed session).
            session (Session): Ftrack session which was used to query/create
                entered job.
            filepath (str): Path to file which should be added to job.
            basename (str): Defines name of file which will be downloaded on
                user's side. Must be without extension otherwise extension will
                be duplicated in downloaded name. Basename from entered path
                used when not entered.
        """

        # Make sure session's locations are configured
        # - they can be deconfigured e.g. using `rollback` method
        session._configure_locations()

        # Query `ftrack.server` location where component will be stored
        location = session.query(
            "Location where name is \"ftrack.server\""
        ).one()

        # Use filename as basename if not entered (must be without extension)
        if basename is None:
            basename = os.path.splitext(
                os.path.basename(filepath)
            )[0]

        component = session.create_component(
            filepath,
            data={"name": basename},
            location=location
        )
        session.create(
            "JobComponent",
            {
                "component_id": component["id"],
                "job_id": job["id"]
            }
        )
        session.commit()
