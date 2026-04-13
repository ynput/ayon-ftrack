import os
import time
from urllib.parse import urlencode

import ayon_api

from ayon_core.addon import AddonsManager
from ayon_core.lib import get_settings_variant
from ayon_core.lib.execute import (
    run_detached_ayon_launcher_process,
    clean_envs_for_ayon_process,
)

from ayon_ftrack.common import (
    is_ftrack_enabled_in_settings,
    get_folder_path_for_entities,
    BaseAction,
)
try:
    from ayon_applications.utils import get_applications_action_info_for_task
except ImportError:
    get_applications_action_info_for_task = None

IDENTIFIER_PREFIX = "application.launch."


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
        items = []
        if get_applications_action_info_for_task is not None:
            for app_info in get_applications_action_info_for_task(
                project_name,
                task_entity["id"],
                task_entity["taskType"],
            ):
                items.append({
                    "label": app_info.group_label,
                    "variant": app_info.variant_label,
                    "description": None,
                    "actionIdentifier": "|".join((
                        self.launch_identifier_with_id,
                        app_info.addon_name,
                        app_info.addon_version,
                        app_info.identifier,
                    )),
                    "icon": app_info.icon,
                })
            return items

        variant = get_settings_variant()
        for action in ayon_api.get_actions(
            project_name,
            entity_type="task",
            entity_ids=[task_entity["id"]],
            entity_subtypes=[task_entity["taskType"]],
            variant=variant,
            mode="simple",
        ):
            if not action["identifier"].startswith(IDENTIFIER_PREFIX):
                continue

            identifier = action["identifier"]
            variant_label = action["label"]
            group_label = action.get("groupLabel")
            if not group_label:
                group_label = variant_label or identifier
                variant_label = None

            icon_url = None
            icon = action["icon"]
            if icon["type"] == "url":
                icon_url = icon["url"]
                if icon_url.startswith("/"):
                    icon_url = (
                        f"{ayon_api.get_base_url()}/{icon_url.lstrip('/')}"
                    )

            addon_name = action["addonName"]
            addon_version = action["addonVersion"]
            items.append({
                "label": group_label,
                "variant": variant_label,
                "description": None,
                "actionIdentifier": "|".join((
                    self.launch_identifier_with_id,
                    addon_name,
                    addon_version,
                    identifier,
                )),
                "icon": icon_url,
            })
        return items

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
        parts = event["data"]["actionIdentifier"].split("|")
        _ = parts.pop(0)
        addon_name = parts.pop(0)
        addon_version = parts.pop(0)
        action_id = "|".join(parts)

        conn = ayon_api.get_server_api_connection()
        headers = conn.get_headers()
        if "referer" in headers:
            headers = None
        else:
            headers["referer"] = conn.get_base_url()

        entity = entities[0]
        ft_project = self.get_project_from_entity(entity)
        project_name = ft_project["full_name"]
        folder_path = self._get_folder_path(session, entity["parent"])
        task_name = entity["name"]
        folder_entity = ayon_api.get_folder_by_path(project_name, folder_path)
        task_entity = ayon_api.get_task_by_name(
            project_name, folder_entity["id"], task_name
        )
        variant = get_settings_variant()
        query = {
            "addonName": addon_name,
            "addonVersion": addon_version,
            "identifier": action_id,
            "variant": variant,
        }
        url = f"actions/execute?{urlencode(query)}"
        request_data = {
            "projectName": project_name,
            "entityType": "task",
            "entityIds": [task_entity["id"]],
        }
        response = ayon_api.raw_post(
            url, headers=headers, json=request_data
        )
        response.raise_for_status()

        data = response.data
        if data["type"] != "launcher":
            return {
                "success": False,
                "message": "Not launched. Unknown action type."
            }
        uri = data["payload"]["uri"]

        # Remove bundles from environment variables
        env = os.environ.copy()
        env.pop("AYON_BUNDLE_NAME", None)
        env.pop("AYON_STUDIO_BUNDLE_NAME", None)
        env = clean_envs_for_ayon_process(env)
        run_detached_ayon_launcher_process(uri, env=env)

        return {
            "success": True,
            "message": "Application launched",
        }

    def _get_folder_path(self, session, entity):
        entity_id = entity["id"]
        return get_folder_path_for_entities(session, [entity])[entity_id]
