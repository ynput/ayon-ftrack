import time
from typing import Optional
from urllib.parse import urlparse, urlunparse

import ayon_api

from ayon_core.addon import AddonsManager
from ayon_ftrack.common import (
    is_ftrack_enabled_in_settings,
    get_folder_path_for_entities,
    BaseAction,
)
from ayon_applications import (
    ApplicationLaunchFailed,
    ApplicationExecutableNotFound,
)
try:
    from ayon_applications.utils import get_applications_for_context
except ImportError:
    # Backwards compatibility for older ayon-applications addon
    def get_applications_for_context(
        project_name,
        folder_entity,
        task_entity,
        project_settings,
        project_entity,
    ):
        ayon_project_apps = project_entity["attrib"].get("applications")
        if ayon_project_apps:
            return ayon_project_apps
        return []


class AppplicationsAction(BaseAction):
    """Applications Action class."""

    type = "Application"
    label = "Application action"

    identifier = "ayon_app"
    _launch_identifier_with_id = None

    # 30 seconds
    cache_lifetime = 30

    def __init__(self, *args, **kwargs):
        super(AppplicationsAction, self).__init__(*args, **kwargs)

        self._applications_manager = None
        self._applications_addon = None
        self._expire_time = 0
        self._icons_mapping = {}

    @property
    def applications_addon(self):
        if self._applications_addon is None:
            addons_manager = AddonsManager()
            self._applications_addon = addons_manager.get("applications")
        return self._applications_addon

    @property
    def applications_manager(self):
        """

        Applications manager is refreshed in regular interval. Interval is
            defined by 'cache_lifetime' property.

        Returns:
            ApplicationManager: Application manager instance.
        """

        current_time = time.time()
        if self._applications_manager is None:
            self._applications_manager = (
                self.applications_addon.get_applications_manager()
            )
            self._expire_time = current_time + self.cache_lifetime

        elif self._expire_time < current_time:
            self._applications_manager.refresh()
            self._expire_time = current_time + self.cache_lifetime
        return self._applications_manager

    @property
    def discover_identifier(self):
        if self._discover_identifier is None:
            self._discover_identifier = "{}.{}".format(
                self.identifier, self.process_identifier()
            )
        return self._discover_identifier

    @property
    def launch_identifier(self):
        if self._launch_identifier is None:
            self._launch_identifier = "{}.*".format(self.identifier)
        return self._launch_identifier

    @property
    def launch_identifier_with_id(self):
        if self._launch_identifier_with_id is None:
            self._launch_identifier_with_id = "{}.{}".format(
                self.identifier, self.process_identifier()
            )
        return self._launch_identifier_with_id

    def construct_requirements_validations(self):
        # Override validation as this action does not need them
        return

    def register(self):
        """Registers the action, subscribing the discover and launch topics."""

        discovery_subscription = (
            "topic=ftrack.action.discover and source.user.username={0}"
        ).format(self.session.api_user)

        self.session.event_hub.subscribe(
            discovery_subscription,
            self._discover,
            priority=self.priority
        )

        launch_subscription = (
            "topic=ftrack.action.launch"
            " and data.actionIdentifier={0}"
            " and source.user.username={1}"
        ).format(
            self.launch_identifier,
            self.session.api_user
        )
        self.session.event_hub.subscribe(
            launch_subscription,
            self._launch
        )

    def _discover(self, event):
        entities = self._translate_event(event)
        items = self.discover(self.session, entities, event)
        if items:
            return {"items": items}

    def discover(self, session, entities, event):
        """Return true if we can handle the selected entities.

        Args:
            session (ftrack_api.Session): Helps to query necessary data.
            entities (list): Object of selected entities.
            event (ftrack_api.Event): ftrack event causing discover callback.
        """

        if (
            len(entities) != 1
            or entities[0].entity_type.lower() != "task"
        ):
            return False

        entity = entities[0]
        if entity["parent"].entity_type.lower() == "project":
            return False

        # TODO we only need project name
        ft_project = self.get_project_from_entity(entity)
        project_name = ft_project["full_name"]
        ayon_project_entity = self.get_ayon_project_from_event(
            event, project_name
        )
        if not ayon_project_entity:
            return False

        project_settings = self.get_project_settings_from_event(
            event, project_name
        )
        ftrack_settings = project_settings.get("ftrack")
        if (
            not ftrack_settings
            or not is_ftrack_enabled_in_settings(ftrack_settings)
        ):
            return False

        folder_path = self._get_folder_path(session, entity["parent"])
        task_name = entity["name"]
        folder_entity = ayon_api.get_folder_by_path(project_name, folder_path)
        task_entity = ayon_api.get_task_by_name(
            project_name, folder_entity["id"], task_name
        )

        only_available = project_settings["applications"].get(
            "only_available", False
        )

        app_names = get_applications_for_context(
            project_name,
            folder_entity,
            task_entity,
            project_settings=project_settings,
            project_entity=ayon_project_entity
        )
        items = []
        for app_name in app_names:
            app = self.applications_manager.applications.get(app_name)
            if not app or not app.enabled:
                continue

            # Skip applications without valid executables
            if only_available and not app.find_executable():
                continue

            app_icon = self.applications_addon.get_app_icon_url(
                app.icon, server=False
            )
            items.append({
                "label": app.group.label,
                "variant": app.label,
                "description": None,
                "actionIdentifier": "{}.{}".format(
                    self.launch_identifier_with_id, app_name
                ),
                "icon": self._get_icon_mapping(app_icon),
            })

        return items

    def _get_icon_mapping(self, icon: Optional[str]):
        """Get icon mapping.

        This function does create and store mapping of icon url. Urls with
            '127.0.0.1' IP address are replaced with 'localhost'. Otherwise
            is icon kept as was.

        """
        if not icon:
            return icon

        if icon in self._icons_mapping:
            # ftrack frontend does not allow redirect to IP address, but
            #   allows redirect to 'localhost'
            result = urlparse(icon)
            if result.hostname == "127.0.0.1":
                port = ""
                if result.port:
                    port = f":{result.port}"
                icon = urlunparse(
                    result._replace(netloc=f"localhost{port}")
                )
            self._icons_mapping[icon] = icon
        return self._icons_mapping[icon]

    def _launch(self, event):
        event_identifier = event["data"]["actionIdentifier"]
        # Check if identifier is same
        # - show message that acion may not be triggered on this machine
        if event_identifier.startswith(self.launch_identifier_with_id):
            return BaseAction._launch(self, event)

        return {
            "success": False,
            "message": (
                "There are running more AYON processes"
                " where Application can be launched."
            )
        }

    def launch(self, session, entities, event):
        """Callback method for the custom action.

        return either a bool (True if successful or False if the action failed)
        or a dictionary with they keys `message` and `success`, the message
        should be a string and will be displayed as feedback to the user,
        success should be a bool, True if successful or False if the action
        failed.

        *session* is a `ftrack_api.Session` instance

        *entities* is a list of tuples each containing the entity type and
        the entity id. If the entity is a hierarchical you will always get
        the entity type TypedContext, once retrieved through a get operation
        you will have the "real" entity type ie. example Shot, Sequence
        or Asset Build.

        *event* the unmodified original event
        """
        identifier = event["data"]["actionIdentifier"]
        id_identifier_len = len(self.launch_identifier_with_id) + 1
        app_name = identifier[id_identifier_len:]

        entity = entities[0]

        task_name = entity["name"]
        folder_path = self._get_folder_path(session, entity["parent"])
        project_name = entity["project"]["full_name"]
        self.log.info(
            f"ftrack launch app: \"{app_name}\""
            f" on {project_name}{folder_path}/{task_name}"
        )
        try:
            self.applications_manager.launch(
                app_name,
                project_name=project_name,
                folder_path=folder_path,
                task_name=task_name
            )

        except ApplicationExecutableNotFound as exc:
            self.log.warning(exc.exc_msg)
            return {
                "success": False,
                "message": exc.msg
            }

        except ApplicationLaunchFailed as exc:
            self.log.error(str(exc))
            return {
                "success": False,
                "message": str(exc)
            }

        except Exception:
            msg = "Unexpected failure of application launch {}".format(
                self.label
            )
            self.log.error(msg, exc_info=True)
            return {
                "success": False,
                "message": msg
            }

        return {
            "success": True,
            "message": "Launching {0}".format(self.label)
        }

    def _get_folder_path(self, session, entity):
        entity_id = entity["id"]
        return get_folder_path_for_entities(session, [entity])[entity_id]
