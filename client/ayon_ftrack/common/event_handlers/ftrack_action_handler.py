import functools
from typing import Optional, List, Dict, Any, Union

import ftrack_api

from .ftrack_base_handler import BaseHandler


class BaseAction(BaseHandler):
    """Custom Action base class.

    Simplify action discovery and launch. This implementation represents
    single action with single callback. To change the behavior implement
    custom callbacks or override '_discover' and '_launch' methods.

    Attributes:
        label (str): Label of action of group name of action. Can be combined
            with 'variant' attribute.
        variant (str): Variant under 'label'. Can be combined with 'label' e.g.
            when 'label' is "Admin" and variant is "Kill jobs". In case
            there is more variants for "Admin" label they'll grouped in ftrack
            UI widgets.
        identifier (str): Action identifier. Is used to trigger the launch
            logic of action.
        icon (str): Url to icon (Browser which should show the icon must have
            access to the resource).
        description (str): Hint of action which is showed to user hovering
            over the action.

    Args:
        session (ftrack_api.Session): Connected ftrack session.

    """
    __ignore_handler_class = True

    label: Optional[str] = None
    variant: Optional[str] = None
    identifier: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None
    handler_type: str = "Action"
    preactions: List[str] = []

    _full_label: Optional[str] = None
    _discover_identifier: Optional[str] = None
    _launch_identifier: Optional[str] = None

    settings_frack_subkey: str = "user_handlers"
    settings_enabled_key: str = "enabled"

    def __init__(self, session: ftrack_api.Session):
        # Validate minimum requirements
        if not self.label:
            raise ValueError("Action missing 'label'.")

        if not self.identifier:
            raise ValueError("Action missing 'identifier'.")

        super().__init__(session)
        self.setup_launch_wrapper()

    def setup_launch_wrapper(self):
        self._launch = self.launch_wrapper(self._launch)

    @property
    def discover_identifier(self) -> str:
        return self.identifier

    @property
    def launch_identifier(self) -> str:
        return self.identifier

    @property
    def handler_label(self) -> str:
        return self.full_label

    @property
    def full_label(self) -> str:
        """Full label of action.

        Value of full label is cached.

        Returns:
            str: Label created from 'label' and 'variant' attributes.

        """
        if self._full_label is None:
            if self.variant:
                label = "{} {}".format(self.label, self.variant)
            else:
                label = self.label
            self._full_label = label
        return self._full_label

    def register(self):
        """Register to ftrack topics to discover and launch action."""
        self.session.event_hub.subscribe(
            "topic=ftrack.action.discover",
            self._discover,
            priority=self.priority
        )

        launch_subscription = (
            "topic=ftrack.action.launch and data.actionIdentifier={}"
        ).format(self.launch_identifier)
        self.session.event_hub.subscribe(launch_subscription, self._launch)

    def _translate_event(
        self,
        event: ftrack_api.event.base.Event,
        session: Optional[ftrack_api.Session] = None
    ) -> List[ftrack_api.entity.base.Entity]:
        """Translate event to receive entities based on it's data."""
        if session is None:
            session = self.session

        _entities = event["data"].get("entities_object", None)
        if _entities is not None and not _entities:
            return _entities

        if (
            _entities is None
            or _entities[0].get("link") == ftrack_api.symbol.NOT_SET
        ):
            _entities = [
                item
                for item in self._get_entities(
                    event,
                    session=session,
                    ignore=["socialfeed", "socialnotification", "team"]
                )
                if item is not None
            ]
            event["data"]["entities_object"] = _entities

        return _entities

    def _discover(
        self, event: ftrack_api.event.base.Event
    ) -> Optional[Dict[str, Any]]:
        """Decide if and how will be action showed to user in ftrack.

        Args:
            event (ftrack_api.Event): Event with topic which triggered this
                callback.

        Returns:
            Union[None, Dict[str, Any]]: None if action is not returned
                otherwise returns items to show in UI (structure of items is
                defined by ftrack and can be found in documentation).

        """
        entities = self._translate_event(event)
        if not entities:
            return None

        accepts = self.discover(self.session, entities, event)
        if not accepts:
            return None

        self.log.debug("Discovering action with selection: {}".format(
            event["data"].get("selection") or []
        ))

        return {
            "items": [{
                "label": self.label,
                "variant": self.variant,
                "description": self.description,
                "actionIdentifier": self.discover_identifier,
                "icon": self.icon,
            }]
        }

    def discover(
        self,
        session: ftrack_api.Session,
        entities: List[ftrack_api.entity.base.Entity],
        event: ftrack_api.event.base.Event,
    ) -> bool:
        """Decide if action is showed to used based on event data.

        Action should override the method to implement logic to show the
        action. The most common logic is based on combination of user roles
        and selected entities.

        Args:
            session (ftrack_api.Session): Session which triggered callback of
                the event.
            entities (List[Any]): Prepared list of entities from event data.
            event (ftrack_api.Event): ftrack event which caused this callback
                is triggered.

        Returns:
            bool: True if action should be returned.

        """
        return False

    def _handle_preactions(
        self, session: ftrack_api.Session, event: ftrack_api.event.base.Event
    ) -> bool:
        """Launch actions before launching this action.

        Concept came from Pype and got deprecated (and used) over time. Should
        be probably removed.

        Note:
            Added warning log that this functionlity is deprecated and will
                be removed in the future.

        Args:
            session (ftrack_api.Session): ftrack session.
            event (ftrack_api.Event): Event which triggered launch of this
                action.

        Returns:
            bool: Preactions were launched or not.

        Deprecated:
            Preactions are marked as deprecated. Server actions should not
                use preactions and local actions use local identifier which
                is hard to handle automatically

        """
        # If preactions are not set
        if len(self.preactions) == 0:
            return True

        if not event.get("data", {}).get("selection"):
            return False

        # If preactions were already started
        if event["data"].get("preactions_launched") is True:
            return True

        self.log.warning((
            "DEPRECATION WARNING: Action \"{}\" is using 'preactions'"
            " which are deprecated and will be removed Q2 2023."
        ).format(self.full_label))

        # Launch preactions
        for preaction in self.preactions:
            self.trigger_action(preaction, event)

        # Relaunch this action
        self.trigger_action(
            self.launch_identifier,
            event,
            additional_event_data={"preactions_launched": True}
        )
        return False

    def launch_wrapper(self, func):
        @functools.wraps(func)
        def wrapper_func(*args, **kwargs):
            self.log.info("{} \"{}\": Launched".format(
                self.handler_type, self.full_label
            ))

            try:
                output = func(*args, **kwargs)
                self.log.info("{} \"{}\": Finished".format(
                    self.handler_type, self.full_label
                ))

            except BaseException as exc:
                self.session.rollback()
                self.session._configure_locations()
                msg = "{} \"{}\": Failed ({})".format(
                    self.handler_type, self.full_label, str(exc))
                self.log.error(msg, exc_info=True)
                output = {
                    "success": False,
                    "message": msg
                }

            return output
        return wrapper_func

    def _launch(
        self, event: ftrack_api.event.base.Event
    ) -> Optional[Dict[str, Any]]:
        entities = self._translate_event(event)
        if not entities:
            return

        preactions_launched = self._handle_preactions(self.session, event)
        if preactions_launched is False:
            return

        interface = self._interface(self.session, entities, event)
        if interface:
            return interface

        response = self.launch(self.session, entities, event)

        return self._handle_result(response)

    def launch(
        self,
        session: ftrack_api.Session,
        entities: List[ftrack_api.entity.base.Entity],
        event: ftrack_api.event.base.Event
    ) -> Optional[Union[bool, Dict[str, Any]]]:
        """Main part of handling event callback.

        Args:
            session (ftrack_api.Session): Session which queried entities.
            entities (List[Any]): Prequeried entities based on event data.
            event (ftrack_api.Event): ftrack event to process.

        Returns:
            Union[bool, Dict[str, Any]]: True or false for success or fail,
                or more complex data structure e.g. to show interface to user.

        """
        raise NotImplementedError()

    def _interface(
        self,
        session: ftrack_api.Session,
        entities: List[ftrack_api.entity.base.Entity],
        event: ftrack_api.event.base.Event
    ) -> Optional[Dict[str, Any]]:
        interface = self.interface(session, entities, event)
        if not interface:
            return

        if isinstance(interface, (tuple, list)):
            return {"items": interface}

        if isinstance(interface, dict):
            if (
                "items" in interface
                or ("success" in interface and "message" in interface)
            ):
                return interface

            raise ValueError((
                "Invalid interface output expected key: \"items\" or keys:"
                " \"success\" and \"message\". Got: \"{}\""
            ).format(str(interface)))

        raise ValueError(
            "Invalid interface output type \"{}\"".format(
                str(type(interface))
            )
        )

    def interface(
        self,
        session: ftrack_api.Session,
        entities: List[ftrack_api.entity.base.Entity],
        event: ftrack_api.event.base.Event
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """Show an interface to user befor the action is processed.

        This is part of launch callback which gives option to return ftrack
        widgets items. These items are showed to user which can fill/change
        values and submit them.

        Interface must in that case handle if event contains values from user.

        Args:
            session (ftrack_api.Session): Connected ftrack api session.
            entities (List[Any]): Entities on which was action triggered.
            event (ftrack_api.Event): Event which triggered launch callback.

        Returns:
            Union[None, List[Dict[str, Any], Dict[str, Any]]: None if nothing
                should be showed, list of items to show or dictionary with
                'items' key and possibly additional data
                (e.g. submit button label).

        """
        return None

    def _handle_result(self, result: Any) -> Optional[Dict[str, Any]]:
        """Validate the returned result from the action callback."""
        if not result:
            return None

        if isinstance(result, dict):
            if "items" in result:
                if not isinstance(result["items"], list):
                    raise TypeError(
                        "Invalid items type {} expected list".format(
                            str(type(result["items"]))))
                return result

            if "success" not in result and "message" not in result:
                self.log.error((
                    "{} \"{}\" Missing required keys"
                    " \"success\" and \"message\" in callback output. This is"
                    " soft fail."
                ).format(self.handler_type, self.full_label))

            elif "message" in result:
                if "success" not in result:
                    result["success"] = True
                return result

            # Fallback to 'bool' result
            result = result.get("success", True)

        if isinstance(result, bool):
            if result:
                return {
                    "success": True,
                    "message": "{} finished.".format(self.full_label)
                }
            return {
                "success": False,
                "message": "{} failed.".format(self.full_label)
            }

        return result

    @staticmethod
    def roles_check(
        settings_roles: List[str],
        user_roles: List[str],
        default: Optional[bool] = True
    ) -> bool:
        """Compare roles from setting and user's roles.

        Args:
            settings_roles(list): List of role names from settings.
            user_roles(list): User's lowered role names.
            default(bool): If 'settings_roles' is empty list.

        Returns:
            bool: 'True' if user has at least one role from settings or
                default if 'settings_roles' is empty.

        """
        if not settings_roles:
            return default

        user_roles = {
            role_name.lower()
            for role_name in user_roles
        }
        for role_name in settings_roles:
            if role_name.lower() in user_roles:
                return True
        return False

    @classmethod
    def get_user_entity_from_event(
        cls,
        session: ftrack_api.Session,
        event: ftrack_api.event.base.Event
    ) -> Optional[ftrack_api.entity.user.User]:
        """Query user entity from event."""

        not_set = object()

        # Check if user is already stored in event data
        user_entity = event["data"].get("user_entity", not_set)
        if user_entity is not_set:
            # Query user entity from event
            user_info = event.get("source", {}).get("user", {})
            user_id = user_info.get("id")
            username = user_info.get("username")
            if user_id:
                user_entity = session.query(
                    "User where id is {}".format(user_id)
                ).first()
            if not user_entity and username:
                user_entity = session.query(
                    "User where username is {}".format(username)
                ).first()
            event["data"]["user_entity"] = user_entity

        return user_entity

    @classmethod
    def get_user_roles_from_event(
        cls,
        session: ftrack_api.Session,
        event: ftrack_api.event.base.Event,
        lower: Optional[bool] = False
    ) -> List[str]:
        """Get user roles based on data in event.

        Args:
            session (ftrack_api.Session): Prepared ftrack session.
            event (ftrack_api.event.Event): Event which is processed.
            lower (Optional[bool]): Lower the role names. Default 'False'.

        Returns:
            List[str]: List of user roles.

        """
        not_set = object()

        user_roles = event["data"].get("user_roles", not_set)
        if user_roles is not_set:
            user_roles = []
            user_entity = cls.get_user_entity_from_event(session, event)
            for role in user_entity["user_security_roles"]:
                role_name = role["security_role"]["name"]
                if lower:
                    role_name = role_name.lower()
                user_roles.append(role_name)
            event["data"]["user_roles"] = user_roles
        return user_roles

    def get_project_name_from_event_with_entities(
        self,
        session: ftrack_api.Session,
        event: ftrack_api.event.base.Event,
        entities: List[ftrack_api.entity.base.Entity],
    ) -> Optional[str]:
        """Load or query and fill project entity from/to event data.

        Project data are stored by ftrack id because in most cases it is
        easier to access project id than project name.

        Args:
            session (ftrack_api.Session): Current session.
            event (ftrack_api.Event): Processed event by session.
            entities (List[Any]): ftrack entities of selection.

        Returns:
            Optional[str]: Project name from event data.

        """
        # Try to get project entity from event
        project_name = event["data"].get("project_name")
        if not project_name:
            project_entity = self.get_project_from_entity(
                entities[0], session
            )
            project_name = project_entity["full_name"]

            event["data"]["project_name"] = project_name
        return project_name

    def get_ftrack_settings(
        self,
        session: ftrack_api.Session,
        event: ftrack_api.event.base.Event,
        entities: List[ftrack_api.entity.base.Entity],
    ) -> Dict[str, Any]:
        project_name = self.get_project_name_from_event_with_entities(
            session, event, entities
        )
        project_settings = self.get_project_settings_from_event(
            event, project_name
        )
        return project_settings["ftrack"]

    def valid_roles(
        self,
        session: ftrack_api.Session,
        entities: List[ftrack_api.entity.base.Entity],
        event: ftrack_api.event.base.Event,
    ) -> bool:
        """Validate user roles by settings.

        Method requires to have set `settings_key` attribute.
        """
        ftrack_settings = self.get_ftrack_settings(session, event, entities)
        settings = (
            ftrack_settings[self.settings_frack_subkey][self.settings_key]
        )
        if self.settings_enabled_key:
            if not settings.get(self.settings_enabled_key, True):
                return False

        user_role_list = self.get_user_roles_from_event(session, event)
        if not self.roles_check(settings.get("role_list"), user_role_list):
            return False
        return True


class LocalAction(BaseAction):
    """Action that warn user when more Processes with same action are running.

    Action is launched all the time but if id does not match id of current
    instanace then message is shown to user.

    Handy for actions where matters if is executed on specific machine.
    """
    __ignore_handler_class: bool = True
    _full_launch_identifier: bool = None

    @property
    def discover_identifier(self) -> str:
        if self._discover_identifier is None:
            self._discover_identifier = "{}.{}".format(
                self.identifier, self.process_identifier()
            )
        return self._discover_identifier

    @property
    def launch_identifier(self) -> str:
        """Catch all topics with same identifier."""
        if self._launch_identifier is None:
            self._launch_identifier = "{}.*".format(self.identifier)
        return self._launch_identifier

    @property
    def full_launch_identifier(self):
        """Catch all topics with same identifier."""
        if self._full_launch_identifier is None:
            self._full_launch_identifier = "{}.{}".format(
                self.identifier, self.process_identifier()
            )
        return self._full_launch_identifier

    def register(self):
        """Register to ftrack topics to discover and launch action.

        Filter events to this session user.
        """
        # Subscribe to discover topic for user under this session
        self.session.event_hub.subscribe(
            "topic=ftrack.action.discover and source.user.username={}".format(
                self.session.api_user
            ),
            self._discover,
            priority=self.priority
        )

        launch_subscription = (
            "topic=ftrack.action.launch"
            " and data.actionIdentifier={}"
            " and source.user.username={}"
        ).format(self.launch_identifier, self.session.api_user)
        self.session.event_hub.subscribe(
            launch_subscription,
            self._launch
        )

    def _discover(
        self, event: ftrack_api.event.base.Event
    ) -> Optional[Dict[str, Any]]:
        entities = self._translate_event(event)
        if not entities:
            return

        accepts = self.discover(self.session, entities, event)
        if not accepts:
            return

        self.log.debug("Discovering action with selection: {0}".format(
            event["data"].get("selection", [])
        ))

        return {
            "items": [{
                "label": self.label,
                "variant": self.variant,
                "description": self.description,
                "actionIdentifier": self.discover_identifier,
                "icon": self.icon,
            }]
        }

    def _launch(
        self, event: ftrack_api.event.base.Event
    ) -> Optional[Dict[str, Any]]:
        event_identifier = event["data"]["actionIdentifier"]
        # Check if identifier is same
        # - show message that acion may not be triggered on this machine
        if event_identifier != self.full_launch_identifier:
            return {
                "success": False,
                "message": (
                    "There are running more AYON processes"
                    " where this action could be launched."
                )
            }
        return super()._launch(event)


class ServerAction(BaseAction):
    """Action class meant to be used on event server.

    Unlike the `BaseAction` roles are not checked on register but on discover.
    For the same reason register is modified to not filter topics by username.
    """
    __ignore_handler_class: bool = True

    settings_frack_subkey: str = "service_event_handlers"
