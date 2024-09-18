import json
import uuid
import threading
import datetime
import copy
import collections
from typing import Any, Union

import ftrack_api

import ayon_api
from ayon_api import (
    get_addon_settings,
    get_service_addon_name,
    get_service_addon_version,
    get_service_addon_settings,
)

from ftrack_common import (
    ServerAction,
    get_service_ftrack_icon_url,
    get_datetime_data,
    create_chunks,
    query_custom_attribute_values,
    is_ftrack_enabled_in_settings,
)

WEEKDAY_MAPPING = {
    0: "monday",
    1: "tuesday",
    2: "wednesday",
    3: "thursday",
    4: "friday",
    5: "saturday",
    6: "sunday",
}


class CreateDailyListServerAction(ServerAction):
    identifier = "create.daily.lists"
    label = "AYON Admin"
    variant = "- Create Daily Lists (Server)"
    icon = get_service_ftrack_icon_url("AYONAdmin.svg")
    description = "Manually create daily lists"
    role_list = {"Administrator", "Project Manager"}

    automated_topic = "{}.automated".format(identifier)
    settings_key = "create_daily_lists"

    def __init__(self, *args, **kwargs):
        super(CreateDailyListServerAction, self).__init__(
            *args, **kwargs
        )

        self._cycle_timer = None
        self._last_cyle_time = None
        self._day_delta = datetime.timedelta(days=1)

    def discover(self, session, entities, event):
        """Show action only on AssetVersions."""

        valid_selection = False
        for ent in event["data"]["selection"]:
            # Ignore entities that are not tasks or projects
            if ent["entityType"].lower() in (
                "show", "task", "reviewsession", "assetversion"
            ):
                valid_selection = True
                break

        if not valid_selection:
            return False
        return self.valid_roles(session, entities, event)

    def interface(self, session, entities, event):
        values = event["data"].get("values")
        if values:
            return None

        project_entity = self.get_project_from_entity(entities[0], session)
        project_name = project_entity["full_name"]
        project_settings = self.get_project_settings_from_event(
            event, project_name
        )
        action_settings = self._extract_action_settings(project_settings)
        action_lists = copy.deepcopy(action_settings["lists"])
        if not action_lists:
            return {
                "type": "message",
                "success": False,
                "message": "There are no list presets in settings to create."
            }

        items = [
            {
                "type": "label",
                "value": "# Select lists to create:",
            },
        ]
        for action_list in action_lists:
            action_id = uuid.uuid4().hex
            action_list["id"] = action_id
            label = "## {} - <b>{}</b>".format(
                action_list["category"],
                action_list["name_template"],
            )
            items.extend([
                {"type": "label", "value": "---"},
                {"type": "label", "value": label},
                {
                    "type": "boolean",
                    "name": action_id,
                    "value": False,
                },
            ])
        items.append({
            "type": "hidden",
            "value": json.dumps(action_lists),
            "name": "action_lists",
        })

        return {
            "title": "Create Lists",
            "items": items,
            "submit_button_label": "Confirm",
        }

    def launch(self, session, entities, event):
        values = event["data"].get("values")
        if not values:
            return

        action_lists = json.loads(values["action_lists"])
        selected_action_lists = [
            action_list
            for action_list in action_lists
            if values.get(action_list["id"])
        ]

        project_entity = self.get_project_from_entity(entities[0], session)
        project_name = project_entity["full_name"]
        project_name_by_id = {
            project_entity["id"]: project_name
        }
        list_defs_by_id = {
            project_entity["id"]: selected_action_lists
        }

        self._process_lists_creation(
            session, list_defs_by_id, project_name_by_id
        )
        return True

    def _calculate_next_cycle_delta(self, action_settings=None):
        if action_settings is None:
            service_settings = get_service_addon_settings()
            action_settings = (
                service_settings
                [self.settings_frack_subkey]
                [self.settings_key]
            )
        cycle_hour_start = action_settings.get("cycle_hour_start")
        if not cycle_hour_start:
            h = m = s = 0
        else:
            h, m, s = [int(v) for v in cycle_hour_start.split(":")]

        # Create threading timer which will trigger creation of report
        #   at the 00:00:01 of next day
        # - callback will trigger another timer which will have 1 day offset
        now = datetime.datetime.now()
        # Create object of today morning
        expected_next_trigger = datetime.datetime(
            now.year, now.month, now.day, h, m, s
        )
        if expected_next_trigger > now:
            seconds = (expected_next_trigger - now).total_seconds()
        else:
            expected_next_trigger += self._day_delta
            seconds = (expected_next_trigger - now).total_seconds()
        return seconds, expected_next_trigger

    def register(self, *args, **kwargs):
        """Override register to be able trigger """
        # Register server action as would be normally
        super(CreateDailyListServerAction, self).register(
            *args, **kwargs
        )

        self.session.event_hub.subscribe(
            "topic={}".format(self.automated_topic),
            self._automated_run,
            priority=self.priority
        )

        seconds_delta, cycle_time = self._calculate_next_cycle_delta()

        # Store cycle time which will be used to create next timer
        self._last_cyle_time = cycle_time
        # Create timer thread
        self._cycle_timer = threading.Timer(
            seconds_delta, self._timer_callback
        )
        self._cycle_timer.start()

    def _timer_callback(self):
        # Stop chrono callbacks if session is closed
        if self.session.closed:
            return

        service_settings = get_service_addon_settings()
        action_settings = (
            service_settings
            [self.settings_frack_subkey]
            [self.settings_key]
        )
        seconds_delta, cycle_time = self._calculate_next_cycle_delta(
            action_settings
        )
        self._last_cyle_time = cycle_time

        self._cycle_timer = threading.Timer(
            seconds_delta, self._timer_callback
        )
        self._cycle_timer.start()

        datetime_obj = datetime.datetime.now()
        weekday = WEEKDAY_MAPPING[datetime_obj.weekday()]

        if weekday not in action_settings["cycle_days"]:
            self.log.debug(
                f"Automated run on day {weekday}"
                f" skipped by settings definition."
            )
            return

        timestamp = datetime_obj.strftime("%Y%m%d")
        user = self.session.query(
            f"User where username is '{self.session.api_user}'"
        ).one()

        event_hash = f"{self.automated_topic}{timestamp}"
        event_ui = uuid.uuid4().hex
        event_data = {
            "id": event_ui,
            "data": {},
            "sent": None,
            "topic": self.automated_topic,
            "source": {
                "user": {
                    "id": user["id"],
                    "username": user["username"]
                }
            },
            "target": "",
            "in_reply_to_event": None
        }
        response = ayon_api.post(
            "events",
            topic="ftrack.leech",
            sender=ayon_api.ServiceContext.service_name,
            hash=event_hash,
            payload=event_data,
            description=f"Automated chrono action '{self.automated_topic}'"
        )
        try:
            response.raise_for_status()
            self.log.debug(f"Created automated task {self.automated_topic}.")
        except Exception:
            self.log.debug(
                f"Failed to created automated task {self.automated_topic}."
                " Probably was already created by another running service."
            )

    def _automated_run(self, event):
        session = self.session
        ayon_project_names = {
            project["name"]
            for project in ayon_api.get_projects(fields=["name"])
        }
        project_entities = session.query(
            "select id, full_name from Project"
        ).all()
        project_names_by_id = {
            project_entity["id"]: project_entity["full_name"]
            for project_entity in project_entities
            if project_entity["full_name"] in ayon_project_names
        }

        action_settings_by_project_id = self._get_action_settings(
            project_names_by_id
        )
        lists_by_project_id = {}
        for item in action_settings_by_project_id.items():
            project_id, action_settings = item
            if not action_settings.get("enabled"):
                continue
            action_lists = [
                item
                for item in action_settings["lists"]
                if item["cycle_enabled"]
            ]
            if action_lists:
                lists_by_project_id[project_id] = action_lists

        if not lists_by_project_id:
            self.log.info((
                "There are no projects that have enabled"
                " cycle review sesison creation"
            ))

        else:
            self._process_lists_creation(
                session,
                lists_by_project_id,
                project_names_by_id
            )

        session.close()

    def _process_lists_creation(
        self,
        session: ftrack_api.Session,
        list_defs_by_project_id: dict[str, list[dict[str, Any]]],
        project_names_by_id: dict[str, str]
    ):
        project_lists = session.query((
            "select id, name, project_id"
            " from List where project_id in ({})"
            " and system_type is 'assetversion'"
        ).format(self.join_query_keys(list_defs_by_project_id))).all()

        project_lists_by_project_id = collections.defaultdict(list)
        for project_list in project_lists:
            project_id = project_list["project_id"]
            project_lists_by_project_id[project_id].append(project_list)

        statuses = session.query("select id, name from Status").all()
        status_id_by_low_name = {
            status["name"].lower(): status["id"]
            for status in statuses
        }

        cust_attrs = session.query(
            "select id, key, default, is_hierarchical"
            " from CustomAttributeConfiguration"
            " where entity_type is AssetVersion or is_hierarchical is True"
        ).all()
        cust_attrs_by_key = collections.defaultdict(list)
        for cust_attr in cust_attrs:
            cust_attrs_by_key[cust_attr["key"]].append(cust_attr)

        # Prepare fill data for today's review sesison and yesterdays
        now = datetime.datetime.now()
        today_obj = datetime.datetime(
            now.year, now.month, now.day, 0, 0, 0
        )

        fill_data = get_datetime_data(today_obj)
        for project_id, list_defs in list_defs_by_project_id.items():
            project_lists = project_lists_by_project_id[project_id]
            project_name = project_names_by_id[project_id]
            self._create_lists(
                session,
                project_name,
                project_id,
                list_defs,
                project_lists,
                cust_attrs_by_key,
                status_id_by_low_name,
                fill_data,
            )

    def _query_all_project_entity_ids(
        self, session: ftrack_api.Session, project_id: str
    ) -> set[str]:
        """Query all entity ids in ftrack project hierarchy.

        All 'TypedContext' entities except 'Task'.

        Args:
            session (ftrack_api.Session): Connected ftrack session.
            project_id (str): Project id.

        Returns:
            set[str]: All entity ids in project hierarchy.
        """

        chunk_size = 100
        query_parent_ids = [project_id]
        task_type = session.query(
            "select id from ObjectType where name is 'Task'"
        ).one()
        task_type_id = task_type["id"]
        entity_ids = set()
        while query_parent_ids:
            _chunk_size = chunk_size
            if len(query_parent_ids) < chunk_size:
                _chunk_size = len(query_parent_ids)
            query_parent_ids_chunk = query_parent_ids[:_chunk_size]
            query_parent_ids = query_parent_ids[_chunk_size:]
            joined_parent_ids = self.join_query_keys(query_parent_ids_chunk)
            entities = session.query(
                "select id from TypedContext"
                f" where parent_id in ({joined_parent_ids})"
                f" and object_type_id != '{task_type_id}' "
            )
            for entity in entities:
                entity_id = entity["id"]
                entity_ids.add(entity_id)
                query_parent_ids.append(entity_id)
        return entity_ids

    def _query_asset_versions(
        self,
        session: ftrack_api.Session,
        project_id: str,
        all_filter_statuses: set[str],
        all_statuses: bool
    ) -> list[ftrack_api.entity.base.Entity]:
        """Query all asset versions in project hierarchy based on filters.

        Using status filter makes this function much faster. Anyway this
        is still querying all entities in project with all their Asset
        entities.

        Args:
            session (ftrack_api.Session): Connected ftrack session.
            project_id (str): Project id.
            all_filter_statuses (set[str]): All filter statuses.
            all_statuses (bool): All statuses enabled.

        Returns:
            list[ftrack_api.entity.base.Entity]: All queried versions.
        """

        entity_ids = self._query_all_project_entity_ids(session, project_id)
        status_filter = ""
        if not all_statuses:
            joined_status_ids = self.join_query_keys(all_filter_statuses)
            status_filter = f" and status_id in ({joined_status_ids})"

        asset_ids = set()
        for chunk in create_chunks(entity_ids):
            joined_parent_ids = self.join_query_keys(chunk)
            assets = session.query(
                "select id, context_id from Asset"
                f" where context_id in ({joined_parent_ids})"
            ).all()
            asset_ids |= {asset["id"] for asset in assets}

        all_asset_versions = []
        for chunk in create_chunks(asset_ids):
            joined_asset_ids = self.join_query_keys(chunk)
            asset_versions = session.query(
                "select id, status_id from AssetVersion"
                f" where asset_id in ({joined_asset_ids}){status_filter}"
            ).all()
            all_asset_versions.extend(asset_versions)
        return all_asset_versions

    def _query_attr_values(
        self,
        session: ftrack_api.Session,
        attr_confs: list[ftrack_api.entity.base.Entity],
        asset_versions: list[ftrack_api.entity.base.Entity],
    ) -> dict[str, dict[str, Any]]:
        """Query non-hierarchical attribute values for asset versions.

        Args:
            session (ftrack_api.Session): Connected ftrack session.
            attr_confs (list[ftrack_api.entity.base.Entity]): Attribute
                configurations.
            asset_versions (list[ftrack_api.entity.base.Entity]): Asset
                versions.

        Returns:
            dict[str, dict[str, Any]]: Values by asset version id
                and attribute configuration id.
        """

        attr_name_by_id = {
            attr_conf["id"]: attr_conf["key"]
            for attr_conf in attr_confs
        }
        default_value_by_attr_key = {
            attr_conf["key"]: attr_conf["default"]
            for attr_conf in attr_confs
        }
        values_by_attr_conf_key = {
            asset_version["id"]: copy.deepcopy(default_value_by_attr_key)
            for asset_version in asset_versions
        }
        value_items = query_custom_attribute_values(
            session,
            attr_name_by_id.keys(),
            values_by_attr_conf_key.keys()
        )
        for value_item in value_items:
            value = value_item["value"]
            if value is None:
                continue
            conf_id = value_item["configuration_id"]
            attr_name = attr_name_by_id[conf_id]
            entity_id = value_item["entity_id"]
            values_by_attr_conf_key[entity_id][attr_name] = value
        return values_by_attr_conf_key

    def _filter_asset_versions_for_list_def(
        self,
        session: ftrack_api.Session,
        list_def: dict[str, Any],
        asset_versions: list[ftrack_api.entity.base.Entity],
        status_id_by_low_name: dict[str, str],
        cust_attrs_by_key: dict[str, ftrack_api.entity.base.Entity],
    ) -> list[ftrack_api.entity.base.Entity]:
        av_filters = list_def["filters"]
        for av_filter in av_filters:
            status_names = av_filter["statuses"]
            custom_attribute_filters = av_filter["custom_attributes"]
            if not status_names and not custom_attribute_filters:
                return asset_versions

        all_filtered_asset_versions = {}
        for av_filter in av_filters:
            filtered_asset_versions = self._filter_avs_by_list_filter(
                session,
                asset_versions,
                av_filter,
                status_id_by_low_name,
                cust_attrs_by_key,
            )
            for asset_version in filtered_asset_versions:
                asset_version_id = asset_version["id"]
                all_filtered_asset_versions[asset_version_id] = asset_version
        return list(all_filtered_asset_versions.values())

    def _filter_avs_by_list_filter(
        self,
        session: ftrack_api.Session,
        asset_versions: list,
        av_filter: dict,
        status_id_by_low_name: dict,
        cust_attrs_by_key,
    ) -> list[ftrack_api.entity.base.Entity]:
        status_names = av_filter["statuses"]
        custom_attribute_filters = av_filter["custom_attributes"]
        list_versions = list(asset_versions)
        if status_names:
            status_ids = set()
            for status_name in status_names:
                status_id = status_id_by_low_name.get(status_name.lower())
                if status_id:
                    status_ids.add(status_id)
            # Skip, none of requested status names are available
            if not status_ids:
                return []
            list_versions = [
                asset_version
                for asset_version in list_versions
                if asset_version["status_id"] in status_ids
            ]

        if not list_versions or not custom_attribute_filters:
            return list_versions

        attr_names = {
            attr_info["attr_name"]
            for attr_info in custom_attribute_filters
        }

        expected_values = {
            attr_info["attr_name"]: attr_info[attr_info["attr_type"]]
            for attr_info in custom_attribute_filters
        }

        attr_conf_by_name = {}
        hier_attr_conf_by_name = {}
        for attr_name in attr_names:
            attr_confs = cust_attrs_by_key.get(attr_name, [])
            matching_attr_conf = None
            for attr_conf in attr_confs:
                if (
                    matching_attr_conf is None
                    # Prefer non-hierarchical over hierarchical attributes
                    or not attr_conf["is_hierarchical"]
                ):
                    matching_attr_conf = attr_conf

            # Attribute not found -> filter not met skipping
            if not matching_attr_conf:
                self.log.info(f"Attribute '{attr_name}' not found")
                return []

            if matching_attr_conf["is_hierarchical"]:
                hier_attr_conf_by_name[attr_name] = matching_attr_conf
            else:
                attr_conf_by_name[attr_name] = matching_attr_conf

        # Standard attributes
        if attr_conf_by_name:
            attr_values_by_id = self._query_attr_values(
                session, list(attr_conf_by_name.values()), list_versions
            )
            new_list_versions = []
            for asset_version in list_versions:
                version_id = asset_version["id"]
                values = attr_values_by_id[version_id]
                valid = True
                for attr_name in attr_conf_by_name.keys():
                    expected_value = expected_values[attr_name]
                    if values[attr_name] != expected_value:
                        valid = False
                        break
                if valid:
                    new_list_versions.append(asset_version)
            list_versions = new_list_versions

        if not list_versions:
            return list_versions

        # Hierarchical attributes
        # - are a little bit more complicated
        hierarchy_ids = set()
        for asset_version in list_versions:
            for item in asset_version["link"]:
                hierarchy_ids.add(item["id"])

        default_values_by_id = {
            attr["id"]: attr["default"]
            for attr in hier_attr_conf_by_name.values()
        }
        value_items = query_custom_attribute_values(
            session,
            default_values_by_id.keys(),
            hierarchy_ids
        )
        values_by_ids = {
            attr_id: {}
            for attr_id in default_values_by_id
        }
        for value_item in value_items:
            value = value_item["value"]
            if value is None:
                continue
            conf_id = value_item["configuration_id"]
            entity_id = value_item["entity_id"]
            values_by_ids[conf_id][entity_id] = value

        output = []
        for asset_version in list_versions:
            valid = True
            for attr_name, attr in hier_attr_conf_by_name.items():
                expected_value = expected_values[attr_name]
                attr_id = attr["id"]
                attr_values = values_by_ids[attr_id]
                value = default_values_by_id[attr_id]
                for item in asset_version["link"]:
                    item_id = item["id"]
                    if item_id in attr_values:
                        value = attr_values[item_id]

                if expected_value != value:
                    valid = False
                    break

            if valid:
                output.append(asset_version)
        return output

    def _create_project_list(
        self, session, project_id, name, category_id
    ):
        session.create(
            "List",
            {
                "project_id": project_id,
                "name": name,
                "is_open": True,
                "category_id": category_id,
                "system_type": "assetversion",
            }
        )
        session.commit()
        return session.query(
            "select id, name, category_id from List"
            f" where project_id is '{project_id}'"
            f" and category_id is '{category_id}'"
            f" and name is '{name}'"
        ).one()

    def _create_list_object(
        self,
        session: ftrack_api.Session,
        list_entity: ftrack_api.entity.base.Entity,
        asset_versions: list[ftrack_api.entity.base.Entity]
    ):
        list_id = list_entity["id"]
        list_objects = session.query(
            f"select entity_id from ListObject where list_id is '{list_id}'"
        ).all()
        list_object_ids = {obj["entity_id"] for obj in list_objects}
        asset_version_ids = {
            asset_version["id"]
            for asset_version in asset_versions
        }
        for entity_id in (asset_version_ids - list_object_ids):
            session.create(
                "ListObject",
                {
                    "list_id": list_id,
                    "entity_id": entity_id,
                }
            )
        session.commit()

    def _create_lists(
        self,
        session: ftrack_api.Session,
        project_name: str,
        project_id: str,
        list_defs: list[dict[str, Any]],
        project_lists: list[ftrack_api.entity.base.Entity],
        cust_attrs_by_key: dict[str, ftrack_api.entity.base.Entity],
        status_id_by_low_name: dict[str, str],
        fill_data: dict[str, Any],
    ):
        # Find out status filters for '_query_asset_versions'
        all_statuses = False
        all_filter_statuses = set()
        for list_def in list_defs:
            av_filters = list_def["filters"]
            for av_filter in av_filters:
                status_names = av_filter["statuses"]
                if not status_names:
                    all_statuses = True
                    break

                for status_name in status_names:
                    status_id = status_id_by_low_name.get(status_name.lower())
                    if status_id:
                        all_filter_statuses.add(status_id)

        asset_versions = self._query_asset_versions(
            session, project_id, all_filter_statuses, all_statuses
        )

        category_objects = session.query(
            "select id, name from ListCategory"
        ).all()
        category_id_by_name = {
            obj["name"]: obj["id"]
            for obj in category_objects
        }

        for list_def in list_defs:
            name_template = list_def["name_template"]
            list_name = self._fill_list_name_template(name_template, fill_data)
            if list_name is None:
                continue

            category_name = list_def["category"]
            category_id = category_id_by_name[category_name]
            asset_versions = self._filter_asset_versions_for_list_def(
                session,
                list_def,
                asset_versions,
                status_id_by_low_name,
                cust_attrs_by_key,
            )
            if not asset_versions:
                self.log.debug(
                    "There are no asset versions matching list definition"
                    f" filters in project '{project_name}'."
                )
                continue

            existing_list = next(
                (
                    project_list
                    for project_list in project_lists
                    if (
                        project_list["name"] == list_name
                        and project_list["category_id"] == category_id
                    )
                ),
                None
            )
            if existing_list is None:
                existing_list = self._create_project_list(
                    session, project_id, list_name, category_id
                )
                project_lists.append(existing_list)

            self._create_list_object(session, existing_list, asset_versions)

    def _get_action_settings(
        self, project_names_by_id: dict[str, str]
    ) -> dict[str, Any]:
        settings_by_project_id = {}
        for project_id, project_name in project_names_by_id.items():
            ftrack_project_settings = get_addon_settings(
                get_service_addon_name(),
                get_service_addon_version(),
                project_name,
            )
            project_settings = {"ftrack": ftrack_project_settings}
            if is_ftrack_enabled_in_settings(ftrack_project_settings):
                action_settings = self._extract_action_settings(project_settings)
            else:
                action_settings = {}
            settings_by_project_id[project_id] = action_settings
        return settings_by_project_id

    def _extract_action_settings(
        self, project_settings: dict[str, Any]
    ) -> dict[str, Any]:
        return (
            project_settings
            .get("ftrack", {})
            .get(self.settings_frack_subkey, {})
            .get(self.settings_key)
        ) or {}

    def _fill_list_name_template(
        self, template: str, data: dict[str, Any]
    ) -> Union[str, None]:
        output = None
        try:
            output = template.format(**data)
        except Exception:
            self.log.warning(
                (
                    "Failed to fill list template {} with data {}"
                ).format(template, data),
                exc_info=True
            )
        return output
