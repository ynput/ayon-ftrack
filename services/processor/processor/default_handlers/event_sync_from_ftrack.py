import re
import collections
import json
import time
import atexit
from typing import Optional, Any

import arrow
import ftrack_api

import ayon_api
from ayon_api import (
    get_project,
    get_folders,
    get_tasks,
    get_folders_links,
    slugify_string,
    create_link,
    delete_link,
)

from ayon_api.entity_hub import EntityHub

from ftrack_common import (
    BaseEventHandler,

    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
    REMOVED_ID_VALUE,

    CUST_ATTR_KEY_SERVER_ID,
    CUST_ATTR_KEY_SERVER_PATH,
    CUST_ATTR_KEY_LIST_TYPE,
    CUST_ATTR_KEY_SYNC_FAIL,

    CUST_ATTR_AUTO_SYNC,

    MappedAYONAttribute,
    CustomAttributesMapping,
    is_ftrack_enabled_in_settings,
    get_custom_attributes_mapping,
    query_custom_attribute_values,

    convert_to_fps,

    create_chunks,
    join_filter_values,
    map_ftrack_users_to_ayon_users,
)

UNKNOWN_VALUE = object()

DEFAULT_ATTRS_MAPPING = {
    "startdate": "startDate",
    "enddate": "endDate",
    "description": "description",
}


class SyncProcess:
    interest_base_types = ["show", "task"]
    ignore_ent_types = ["Milestone"]
    ignore_change_keys = [
        "thumbid",
        "priorityid",
    ]

    project_query = (
        "select id, full_name, name, custom_attributes,"
        " project_schema._task_type_schema.types.name"
        " from Project where id is \"{}\""
    )
    entities_columns = (
        "id",
        "name",
        "object_type_id",
        "type_id",
        "parent_id",
        "link",
        "description",
    )
    entities_columns_str = ", ".join(entities_columns)
    entities_query_by_id = (
        f"select {entities_columns_str} from TypedContext"
        " where project_id is \"{}\" and id in ({})"
    )
    cust_attr_query_keys = [
        "id",
        "key",
        "entity_type",
        "object_type_id",
        "is_hierarchical",
        "config",
        "default"
    ]

    def __init__(self, event_handler, session, event, log):
        self.event_handler = event_handler
        self.event = event
        self.session = session
        self.log = log

        self._ft_project_id = UNKNOWN_VALUE
        self._ft_project = UNKNOWN_VALUE
        self._project_name = UNKNOWN_VALUE
        self._project_entity = UNKNOWN_VALUE
        self._project_settings = None

        self._is_event_valid = UNKNOWN_VALUE
        self._ft_project_removed = UNKNOWN_VALUE
        self._entities_by_action = UNKNOWN_VALUE
        self._found_actions = UNKNOWN_VALUE
        self._project_changed_autosync = UNKNOWN_VALUE
        self._trigger_project_sync = UNKNOWN_VALUE

        self._ftrack_entities_by_id = {}

        # Server entity cache
        self._entity_hub = None
        self._folder_ids_by_ftrack_id = None
        self._task_ids_by_ftrack_id = None
        self._has_valid_entity_types = None

        # Caches from ftrack
        self._cust_attr_mapping = None
        self._ft_cust_attr_types_by_id = None
        self._ft_cust_attrs = None
        self._ft_object_type_name_by_id = None
        self._ft_task_type_name_by_id = None
        self._ft_status_names_by_id = None

        self._created_entity_by_ftrack_id = {}
        self._hierarchy_changed_by_ftrack_id = {}
        self._remapped_entity_by_ftrack_id = {}
        self._ft_failed_sync_ids = set()

    def get_ftrack_entity_by_ids(self, entity_ids):
        """Get or query ftrack entity by id.

        Method is caching already queried entities.

        Args:
            entity_ids (List[str]): Id of ftrack entity.

        Returns:
            Dict[str, Union[ftrack_api.Entity, None]]: Mapping of ftrack entity
                by it's id.
        """

        if not entity_ids:
            return {}

        entity_ids = set(entity_ids)
        output = {
            entity_id: None
            for entity_id in entity_ids
        }
        entity_ids.discard(None)
        if self.ft_project_id in entity_ids:
            output[self.ft_project_id] = self.ft_project
            entity_ids.remove(self.ft_project_id)

        for entity_id in tuple(entity_ids):
            if entity_id not in self._ftrack_entities_by_id:
                continue
            output[entity_id] = self._ftrack_entities_by_id[entity_id]
            entity_ids.remove(entity_id)

        entities = []
        if entity_ids:
            entities = self.session.query(
                self.entities_query_by_id.format(
                    self.ft_project_id,
                    join_filter_values(entity_ids)
                )
            ).all()

        for entity in entities:
            entity_id = entity["id"]
            self._ftrack_entities_by_id[entity_id] = entity
            output[entity_id] = entity

        return output

    def get_ftrack_entity_by_id(self, entity_id):
        if not entity_id:
            return None
        return self.get_ftrack_entity_by_ids([entity_id])[entity_id]

    def get_ayon_project(self) -> Optional[dict[str, Any]]:
        if self._project_entity is UNKNOWN_VALUE:
            self._project_entity = get_project(self.project_name)
        return self._project_entity

    @property
    def project_name(self):
        """

        Returns:
            str: Name of project on which happened changes in processed event.
        """

        if self._project_name is UNKNOWN_VALUE:
            project_name = None
            if self.ft_project is not None:
                project_name = self.ft_project["full_name"]
            self._project_name = project_name
        return self._project_name

    @property
    def project_settings(self):
        if self._project_settings is None:
            self._project_settings = (
                self.event_handler.get_project_settings_from_event(
                    self.event, self.project_name
                )
            )
        return self._project_settings

    @property
    def ft_project_id(self):
        """

        Returns:
            Union[str, None]: Id of ftrack project based on information in
                processed event.
        """

        if self._ft_project_id is UNKNOWN_VALUE:
            found_id = None
            for ent_info in self.event["data"].get("entities", []):
                if found_id is not None:
                    break
                parents = ent_info.get("parents") or []
                for parent in parents:
                    if parent.get("entityType") == "show":
                        found_id = parent.get("entityId")
                        break

            self._ft_project_id = found_id
        return self._ft_project_id

    @property
    def ft_project(self):
        """

        Returns:
            ftrack_api.Entity: ftrack project entity.
        """

        if self._ft_project is UNKNOWN_VALUE:
            project_id = self.ft_project_id
            project = None
            if project_id:
                project = self.session.query(
                    self.project_query.format(project_id)
                ).first()

            self._ft_project = project
        return self._ft_project

    @property
    def is_event_valid(self):
        """

        Returns:
            bool: Data from event are important for synchronization.
        """

        if self._is_event_valid is UNKNOWN_VALUE:
            self.initial_event_processing()
        return self._is_event_valid

    @property
    def ft_project_removed(self):
        """

        Returns:
            bool: Project was removed from ftrack.
        """

        if self._ft_project_removed is UNKNOWN_VALUE:
            self.initial_event_processing()
        return self._ft_project_removed

    @property
    def entities_by_action(self):
        """

        Returns:
            Dict[str, Dict[str, Any]]: Entity information from ftrack event
                byt action happened on them.
        """

        if self._entities_by_action is UNKNOWN_VALUE:
            self.initial_event_processing()
        return self._entities_by_action

    @property
    def found_actions(self):
        """

        Returns:
            Set[str]: Actions that happened in processed ftrack event.
        """

        if self._found_actions is UNKNOWN_VALUE:
            self.initial_event_processing()
        return self._found_actions

    @property
    def project_changed_autosync(self):
        """

        Returns:
            bool: Autosync value has changed.
        """

        if self._project_changed_autosync is UNKNOWN_VALUE:
            self.initial_event_processing()
        return self._project_changed_autosync

    @property
    def trigger_project_sync(self):
        """

        Returns:
            bool: Autosync was turned on so a project sync action should be
                triggered.
        """

        if self._trigger_project_sync is UNKNOWN_VALUE:
            self.initial_event_processing()
        return self._trigger_project_sync

    @property
    def entity_hub(self):
        if self._entity_hub is None:
            self._entity_hub = EntityHub(self.project_name)
        return self._entity_hub

    @property
    def task_ids_by_ftrack_id(self):
        if self._task_ids_by_ftrack_id is None:
            task_ids_by_ftrack_id = collections.defaultdict(list)
            tasks = get_tasks(
                self.project_name,
                fields=["id", f"attrib.{FTRACK_ID_ATTRIB}"]
            )
            for task in tasks:
                ftrack_id = task.get("attrib", {}).get(FTRACK_ID_ATTRIB)
                task_ids_by_ftrack_id[ftrack_id].append(task["id"])

            self._task_ids_by_ftrack_id = task_ids_by_ftrack_id
        return self._task_ids_by_ftrack_id

    @property
    def folder_ids_by_ftrack_id(self):
        if self._folder_ids_by_ftrack_id is None:
            folder_ids_by_ftrack_id = collections.defaultdict(list)
            folders = get_folders(
                self.project_name,
                fields=["id", f"attrib.{FTRACK_ID_ATTRIB}"]
            )
            for folder in folders:
                ftrack_id = folder.get("attrib", {}).get(FTRACK_ID_ATTRIB)
                folder_ids_by_ftrack_id[ftrack_id].append(folder["id"])

            self._folder_ids_by_ftrack_id = folder_ids_by_ftrack_id
        return self._folder_ids_by_ftrack_id

    @property
    def ft_cust_attr_types_by_id(self):
        if self._ft_cust_attr_types_by_id is None:
            cust_attr_types = self.session.query(
                "select id, name from CustomAttributeType"
            ).all()
            self._ft_cust_attr_types_by_id = {
                cust_attr_type["id"]: cust_attr_type
                for cust_attr_type in cust_attr_types
            }
        return self._ft_cust_attr_types_by_id

    @property
    def cust_attr_mapping(self) -> CustomAttributesMapping:
        if self._cust_attr_mapping is None:
            self._cust_attr_mapping = get_custom_attributes_mapping(
                self.session,
                self.project_settings["ftrack"],
                self.ft_cust_attrs,
            )
        return self._cust_attr_mapping

    @property
    def ft_cust_attrs(self):
        if self._ft_cust_attrs is None:
            fields = ", ".join(self.cust_attr_query_keys)
            self._ft_cust_attrs = self.session.query(
                    f"select {fields} from CustomAttributeConfiguration"
            ).all()
        return self._ft_cust_attrs

    @property
    def ft_object_type_name_by_id(self):
        if self._ft_object_type_name_by_id is None:
            object_types = self.session.query(
                "select id, name from ObjectType").all()
            self._ft_object_type_name_by_id = {
                object_type["id"]: object_type["name"]
                for object_type in object_types
            }

        return self._ft_object_type_name_by_id

    @property
    def ft_task_type_name_by_id(self):
        if self._ft_task_type_name_by_id is None:
            task_types = self.session.query("select id, name from Type").all()
            self._ft_task_type_name_by_id = {
                task_type["id"]: task_type["name"]
                for task_type in task_types
            }
        return self._ft_task_type_name_by_id

    @property
    def ft_status_names_by_id(self):
        if self._ft_status_names_by_id is None:
            statuses = self.session.query("select id, name from Status").all()
            self._ft_status_names_by_id = {
                statuse["id"]: statuse["name"]
                for statuse in statuses
            }
        return self._ft_status_names_by_id

    @property
    def has_valid_entity_types(self):
        return self._has_valid_entity_types

    def initial_event_processing(self):
        """First processing of data on event.

        This part decide if event contain data important for synchronization.
        """

        if self._ft_project_removed is not UNKNOWN_VALUE:
            return

        # Set default values
        self._is_event_valid = False
        self._project_changed_autosync = False
        self._trigger_project_sync = False

        self._has_valid_entity_types = True
        self._split_event_entity_info()

        # If project was removed then skip rest of event processing
        if (
            self._ft_project_removed
            or not self._found_actions
        ):
            self._has_valid_entity_types = False
            return

        self._check_enabled_auto_sync()
        if self._project_changed_autosync:
            if not self._project_enabled_validation():
                # Make sure that sync is not triggered if project is not
                #   available or disabled
                self._trigger_project_sync = False
            return

        self._filter_update_actions()

        if not self._found_actions:
            self.log.debug("Skipping. Nothing to update.")
            self._has_valid_entity_types = False
            return

        if not self._project_enabled_validation():
            return

        # Skip if auto-sync is not set
        auto_sync = self.ft_project["custom_attributes"][CUST_ATTR_AUTO_SYNC]
        is_event_valid = auto_sync is True
        if is_event_valid:
            # TODO probably should be handled
            # TODO add logs - with detail what is wrong
            # - project is not available on server
            if not self.entity_hub.project_entity:
                is_event_valid = False
        self._is_event_valid = is_event_valid

    def _split_event_entity_info(self):
        entities_by_action = {
            "remove": {},
            "update": {},
            "add": {},
            "assignee_change": {},
            "link_change": [],
            "list_added": [],
            "list_removed": [],
            "list_changed": [],
            "list_item_change": {},
        }
        found_actions = set()
        ft_project_removed = False
        for ent_info in self.event["data"]["entities"]:
            base_type = ent_info["entityType"]
            if base_type == "list":
                action = ent_info["action"]
                if action == "remove":
                    action = "list_removed"
                elif action == "add":
                    action = "list_added"

                elif action == "update":
                    action = "list_changed"

                    valid = "name" in ent_info["changes"]

                    entity_type = ent_info["entity_type"]
                    if (
                        entity_type == "TypedContextList"
                        and CUST_ATTR_KEY_LIST_TYPE in ent_info["changes"]
                    ):
                        valid = True

                    if not valid:
                        continue

                else:
                    continue

                found_actions.add(action)
                entities_by_action[action].append(ent_info)
                continue

            if base_type == "listobject":
                action = ent_info["action"]
                if action not in ("remove", "add"):
                    continue

                if action == "add":
                    list_id = ent_info["changes"]["listid"]["new"]
                else:
                    list_id = ent_info["changes"]["listid"]["old"]

                action = "list_item_change"
                found_actions.add(action)
                list_entities = entities_by_action[action].setdefault(
                    list_id, []
                )
                list_entities.append(ent_info)
                continue

            if base_type == "appointment":
                if ent_info["action"] not in ("remove", "add"):
                    continue
                type_changes = ent_info["changes"]["type"]
                appointment_type = type_changes["new"] or type_changes["old"]
                if appointment_type != "assignment":
                    continue
                action = "assignee_change"
                ftrack_id = ent_info["entityId"]
                found_actions.add(action)
                entities_by_action[action][ftrack_id] = ent_info
                continue

            if base_type == "dependency":
                # NOTE we're not handling 'update'
                # - hopefully nobody is changing ids in existing links?
                if ent_info["action"] not in ("remove", "add"):
                    continue
                action = "link_change"
                found_actions.add(action)
                entities_by_action[action].append(ent_info)
                continue

            if base_type not in self.interest_base_types:
                continue

            entity_type = ent_info.get("entity_type")
            if not entity_type or entity_type in self.ignore_ent_types:
                continue

            action = ent_info["action"]
            ftrack_id = ent_info["entityId"]

            # Skip deleted projects
            if action == "remove" and base_type == "show":
                ft_project_removed = True

            # Change 'move' events to 'update'
            # - they may contain more changes than just 'parent_id'
            if action == "move":
                action = "update"

            # regular change process handles all other than Tasks
            found_actions.add(action)
            entities_by_action[action][ftrack_id] = ent_info

        self._ft_project_removed = ft_project_removed
        self._entities_by_action = entities_by_action
        self._found_actions = found_actions

    def _check_enabled_auto_sync(self):
        updates = self._entities_by_action["update"]
        for ftrack_id, ent_info in updates.items():
            # filter project
            if ent_info["entityType"] != "show":
                continue

            changes = ent_info["changes"]
            if CUST_ATTR_AUTO_SYNC not in changes:
                continue

            auto_sync = changes[CUST_ATTR_AUTO_SYNC]["new"]
            if auto_sync == "1":
                self._trigger_project_sync = True

            self._project_changed_autosync = True

    def _project_enabled_validation(self):
        # NOTE This is first part of code which should query entity from
        #   ftrack.
        # Query project and check if can be actually queried and if has
        #   available custom attribute that is used to identify if project
        #   should be autosynced.
        ft_project = self.ft_project
        if ft_project is None:
            self.log.error("Failed to query ftrack project. Skipping event")
            return False

        if CUST_ATTR_AUTO_SYNC not in ft_project["custom_attributes"]:
            # TODO should we sent message to someone?
            self.log.error((
                f"Custom attribute \"{CUST_ATTR_AUTO_SYNC}\" is not created"
                f" or user \"{self.session.api_user}\" used"
                " for Event server don't have permissions to access it!"
            ))
            return False

        if not is_ftrack_enabled_in_settings(self.project_settings["ftrack"]):
            self.log.debug(
                f"ftrack is disabled for project \"{self.project_name}\""
            )
            return False
        return True

    def _filter_update_actions(self):
        updates = self.entities_by_action["update"]
        filtered_updates = {}
        for ftrack_id, ent_info in updates.items():
            changed_keys = list(ent_info.get("keys") or [])
            changes = dict((ent_info.get("changes") or {}).items())

            for _key in self.ignore_change_keys:
                if _key in changed_keys:
                    changed_keys.remove(_key)
                    changes.pop(_key, None)

            if not changed_keys:
                continue

            ent_info["keys"] = changed_keys
            ent_info["changes"] = changes
            filtered_updates[ftrack_id] = ent_info

        self._entities_by_action["update"] = filtered_updates
        if not filtered_updates:
            self._found_actions.discard("update")

    def _get_folder_hierachy_changes(self):
        output = dict(self.entities_by_action["remove"].items())
        for ftrack_id, info in self.entities_by_action["update"].items():
            changed_keys = info["keys"]
            if "parent_id" in changed_keys or "name" in changed_keys:
                output[ftrack_id] = info
        return {
            ftrack_id: info
            for ftrack_id, info in output.items()
            if info["entityType"] != "show" and info["entity_type"] != "Task"
        }

    def _try_create_entity(self, ftrack_id):
        # Skip creation if it was already tried
        if ftrack_id in self._ft_failed_sync_ids:
            return None

        # Try to find ftrack entity (as reference for creation)
        # - without existing ftrack entity can't be created server as are
        ft_entity = self.get_ftrack_entity_by_id(ftrack_id)
        if ft_entity is None:
            self._ft_failed_sync_ids.add(ftrack_id)
            return None

        # Get entity type to create (folder/task)
        if ft_entity.entity_type == "Task":
            entity_type = "task"
            entity_ids = self.task_ids_by_ftrack_id[ftrack_id]
        else:
            entity_type = "folder"
            entity_ids = self.folder_ids_by_ftrack_id[ftrack_id]

        # Skip if there is more than one server entity id matching
        #   ftrack id
        if len(entity_ids) > 1:
            # TODO find out what to do in that case
            # TODO handle this case somehow
            self._ft_failed_sync_ids.add(ftrack_id)
            self.log.warning((
                "Found more then one matching entity on server for"
                f" ftrack id {ftrack_id} ({entity_ids}). Skipping"
            ))
            return None

        # Just return entity if already exists
        if entity_ids:
            entity = self.entity_hub.get_or_fetch_entity_by_id(
                entity_ids[0], [entity_type]
            )
            if entity is not None:
                return entity

        # Find ftrack parent to find server parent under which the entity can
        #   be created
        ft_parent_id = ft_entity["parent_id"]
        # If parent is project then just get project entity
        parent = None
        if ft_parent_id == self.ft_project_id:
            parent = self.entity_hub.project_entity
            # Skip creation if task was created under project
            if entity_type == "task":
                self._ft_failed_sync_ids.add(ftrack_id)
                return None

        if parent is None:
            # TODO missing check if there are multiple mathching entity ids
            parent_ids = self.folder_ids_by_ftrack_id[ft_parent_id]
            if len(parent_ids) == 1:
                parent = self.entity_hub.get_or_fetch_entity_by_id(
                    parent_ids[0], ["folder"]
                )

            # Try to create parent if is not available
            elif not parent_ids:
                parent = self._try_create_entity(ft_parent_id)

        # Entity can't be created without parent
        if parent is None:
            self._ft_failed_sync_ids.add(ftrack_id)
            return None

        label = ft_entity["name"]
        name = slugify_string(label)
        # Try to find matching entity by name in same parent
        matching_entity = None
        for child in parent.children:
            # Find matching entity by entity type and name
            # - this is to avoid duplication or to find entity without
            #   ftrack id in attributes (or old ftrack id)
            # TODO add lower cased comparison for entities which are not
            #   immutable
            if (
                child.entity_type == entity_type
                and child.name == name
            ):
                matching_entity = child
                break

        # Handle cases when there already is entity with matching name and
        #   type
        if matching_entity is not None:
            # WARNING this won't update entity attributes!!!
            # TODO if there is entity that can be used "instead" also fill
            #   it's attributes as expected
            # - all created entities should have filled all attributes

            # Check if entity has set ftrack id
            # - when does not have set then we can match it to just processed
            #   ftrack entity
            matching_entity_id = matching_entity.id
            matching_ftrack_id = matching_entity.attribs[FTRACK_ID_ATTRIB]
            # When ftrack id is not empty then make sure the ftrack id leads
            #   to ftrack existing entity and unset the id if it does not
            if matching_ftrack_id is not None:
                matching_ft_entity = self.get_ftrack_entity_by_id(
                    matching_ftrack_id)
                entity_ids = self.folder_ids_by_ftrack_id[matching_ftrack_id]
                if matching_ft_entity is None:
                    if matching_entity_id in entity_ids:
                        entity_ids.remove(matching_entity_id)
                    matching_ftrack_id = None

            # If ftrack id on matching entity does not exist we can "reuse" it
            if matching_ftrack_id is not None:
                self._ft_failed_sync_ids.add(ftrack_id)
                return None

        ft_path = "/".join([
            item["name"]
            for item in ft_entity["link"]
            if item["type"] != "Project"
        ])

        mapping_items_by_id = {}
        for mapping_item in self.cust_attr_mapping.values():
            attr_conf = mapping_item.get_attr_conf_for_entity(ft_entity)
            if attr_conf is not None:
                mapping_items_by_id[attr_conf["id"]] = mapping_item

        value_items = query_custom_attribute_values(
            self.session, mapping_items_by_id.keys(), [ftrack_id]
        )
        attr_values_by_key = {}
        for item in value_items:
            value = item["value"]
            if value is None:
                continue
            attr_id = item["configuration_id"]
            mapping_item: MappedAYONAttribute = mapping_items_by_id[attr_id]
            key = mapping_item.ayon_attribute_name
            if key not in attr_values_by_key or mapping_item.is_hierarchical:
                attr_values_by_key[key] = value

        if matching_entity is not None:
            entity = matching_entity
            entity.label = label

        elif entity_type == "folder":
            object_type_id = ft_entity["object_type_id"]
            folder_type = self.ft_object_type_name_by_id[object_type_id]
            entity = self.entity_hub.add_new_folder(
                folder_type=folder_type,
                name=name,
                label=label,
                parent_id=parent.id
            )

        else:
            task_type_id = ft_entity["type_id"]
            task_type = self.ft_task_type_name_by_id[task_type_id]
            entity = self.entity_hub.add_new_task(
                task_type=task_type,
                name=name,
                label=label,
                parent_id=parent.id
            )

        if entity_type == "folder":
            entity_id_mapping = self.folder_ids_by_ftrack_id[ftrack_id]
        else:
            entity_id_mapping = self.task_ids_by_ftrack_id[ftrack_id]

        if entity.id not in entity_id_mapping:
            entity_id_mapping.append(entity.id)

        entity.attribs[FTRACK_ID_ATTRIB] = ftrack_id
        entity.attribs[FTRACK_PATH_ATTRIB] = ft_path
        for key, value in attr_values_by_key.items():
            if key in entity.attribs:
                entity.attribs[key] = value
        self._created_entity_by_ftrack_id[ftrack_id] = entity
        return entity

    def _try_find_other_match(self, info, entity):
        parent_id = info["parentId"]
        if parent_id == self.ft_project_id:
            ft_parent = self.ft_project
        else:
            ft_parent = self.session.query((
                "select id, name from TypedContext"
                f" where id is {parent_id}"
            )).first()

        if ft_parent is None:
            return False

        new_ft_match = None
        ft_other_children = self.session.query((
            f"select {self.entities_columns_str} from TypedContext"
            f" where parent_id is {parent_id}"
        )).all()
        for child in ft_other_children:
            label = child["name"]
            name = slugify_string(label)
            if name == entity.name:
                new_ft_match = child
                break

        if new_ft_match is None:
            return False

        ft_is_task = new_ft_match.entity_type == "Task"
        entity_is_task = entity.entity_type == "task"
        if ft_is_task != entity_is_task:
            return False

        ftrack_id = new_ft_match["id"]
        entity_id = self.folder_ids_by_ftrack_id.get(ftrack_id)
        if entity_id == entity.id:
            return True

        if entity_id is not None:
            return False

        entity.attribs[FTRACK_ID_ATTRIB] = ftrack_id
        # TODO add task path?
        if not entity_is_task:
            entity.attribs[FTRACK_PATH_ATTRIB] = "/".join([
                item["name"]
                for item in new_ft_match["link"]
                if item["type"] != "Project"
            ])
        self._remapped_entity_by_ftrack_id[ftrack_id] = entity
        return True

    def _prepare_folder_allowed_hierarchy_changes(
        self, folder_hierarchy_changes, ftrack_ids_to_create
    ):
        allowed_changes = {}
        for ftrack_id, info in folder_hierarchy_changes.items():
            # Do not exist in current folders (nothing to validate)
            entity_ids = self.folder_ids_by_ftrack_id[ftrack_id]
            if not entity_ids:
                ftrack_ids_to_create.add(ftrack_id)
                continue

            if len(entity_ids) != 1:
                # TODO handle this cases somehow
                self.log.warning((
                    "Found more then one matching entity on server for"
                    f" ftrack id {ftrack_id} ({entity_ids}). Skipping"
                ))
                continue

            entity = self.entity_hub.get_or_fetch_entity_by_id(
                entity_ids[0], ["folder"])

            allow_change = not entity.immutable_for_hierarchy
            if not allow_change:
                changes = info["changes"]
                # It is valid change if parent did not change and name
                #   did not change for server (after slugify)
                if "parent_id" not in changes and "name" in changes:
                    new_name = changes["name"]["new"]
                    if slugify_string(new_name) == entity.name:
                        allow_change = True

            if allow_change:
                allowed_changes[ftrack_id] = (info, entity)
                continue

            self._ft_failed_sync_ids.add(ftrack_id)
            self.log.warning(
                f"Hierarchy changes are not allow on entity {entity.path}"
            )

        return allowed_changes

    def _clear_ayon_id_in_created_entities(self, ftrack_ids: set[str]):
        ayon_id_by_ftrack_id = self._get_server_id_by_ftrack_ids(
            ftrack_ids
        )
        filtered_mapping = {
            ftrack_id: ayon_id
            for ftrack_id, ayon_id in ayon_id_by_ftrack_id.items()
            if ayon_id
        }
        ayon_ids = set(filtered_mapping.values())
        if not ayon_ids:
            return

        ayon_id_attr_conf = next(
            (
                attr
                for attr in self.ft_cust_attrs
                if attr["key"] == CUST_ATTR_KEY_SERVER_ID
            ),
            None
        )
        if ayon_id_attr_conf is None:
            return

        ayon_id_attr_conf_id = ayon_id_attr_conf["id"]
        current_mapping_ay = collections.defaultdict(set)
        current_mapping_ft = {}
        for chunk in create_chunks(ayon_ids, 100):
            entity_ids_joined = join_filter_values(chunk)
            for item in self.session.query(
                "select value, entity_id"
                " from CustomAttributeValue"
                f" where configuration_id in ({ayon_id_attr_conf_id})"
                f" and value in ({entity_ids_joined})"
            ).all():
                current_mapping_ay[item["value"]].add(item["entity_id"])
                current_mapping_ft[item["entity_id"]] = item["value"]

        operations = []
        cleared_ftrack_ids = set()
        for ayon_id in ayon_ids:
            ftrack_ids = current_mapping_ay[ayon_id]
            if len(ftrack_ids) < 2:
                continue
            entity = self.entity_hub.get_or_fetch_entity_by_id(
                ayon_id, ["folder", "task"]
            )
            current_ftrack_id = entity.attrib.get(FTRACK_ID_ATTRIB, None)
            if current_ftrack_id and current_ftrack_id in ftrack_ids:
                ftrack_ids.discard(current_ftrack_id)
            else:
                ftrack_ids = set()
                for ftrack_id, _ayon_id in filtered_mapping.items():
                    if _ayon_id == ayon_id:
                        ftrack_ids.add(ftrack_id)

            cleared_ftrack_ids |= ftrack_ids
            for ftrack_id in ftrack_ids:
                entity_key = collections.OrderedDict((
                    ("configuration_id", ayon_id_attr_conf_id),
                    ("entity_id", ftrack_id)
                ))
                operations.append(
                    ftrack_api.operation.DeleteEntityOperation(
                        "CustomAttributeValue",
                        entity_key
                    )
                )

        ayon_path_attr_conf = next(
            (
                attr
                for attr in self.ft_cust_attrs
                if attr["key"] == CUST_ATTR_KEY_SERVER_PATH
            ),
            None
        )
        if ayon_path_attr_conf is not None:
            ayon_path_attr_conf_id = ayon_path_attr_conf["id"]
            for value_item in query_custom_attribute_values(
                self.session,
                {ayon_path_attr_conf_id},
                ftrack_ids,
            ):
                if value_item["value"] is None:
                    continue
                entity_key = collections.OrderedDict((
                    ("configuration_id", ayon_path_attr_conf_id),
                    ("entity_id", value_item["entity_id"])
                ))
                operations.append(
                    ftrack_api.operation.DeleteEntityOperation(
                        "CustomAttributeValue",
                        entity_key
                    )
                )

        if not operations:
            return

        for op in operations:
            self.session.recorded_operations.push(op)
        self.session.commit()

    def _process_folder_hierarchy_changes(
        self,
        folder_hierarchy_changes,
        ftrack_ids_to_create
    ):
        if not folder_hierarchy_changes:
            return

        changes_count = len(folder_hierarchy_changes)
        self.log.debug(
            f"Looking into {changes_count} folder hierarchy changes"
        )

        allowed_changes = self._prepare_folder_allowed_hierarchy_changes(
            folder_hierarchy_changes,
            ftrack_ids_to_create
        )
        if not allowed_changes:
            self.log.debug("All folder hierarchy changes are not possible")
            return

        diff_count = changes_count - len(allowed_changes)
        if diff_count:
            self.log.debug(f"Filtered {diff_count} changes.")

        changes_queue = collections.deque()
        for item in allowed_changes.items():
            changes_queue.append(item)
        changes_queue.append(len(changes_queue))

        self.log.debug("Starting folder changes queue")
        while changes_queue:
            item = changes_queue.popleft()
            if isinstance(item, int):
                current_len = len(changes_queue)
                if current_len == item:
                    # Mark all remaining items as failed
                    while changes_queue:
                        item = changes_queue.popleft()
                        if not isinstance(item, int):
                            self._ft_failed_sync_ids.add(item[0])

                else:
                    # Add current len for next iteration
                    changes_queue.append(current_len)
                continue

            ftrack_id, (info, entity) = item
            if ftrack_id in self._ft_failed_sync_ids:
                continue

            self.log.debug(
                f"Trying to apply hierarchy changes of {ftrack_id}"
            )

            changes = info["changes"]
            if "name" in changes:
                label = changes["name"]["new"]
                name = slugify_string(label)
            else:
                name = entity.name

            parent = None
            if "parent_id" not in changes:
                parent = entity.parent
            else:
                ft_parent_id = changes["parent_id"]["new"]
                if ft_parent_id == self.ft_project_id:
                    parent = self.entity_hub.project_entity

                elif ft_parent_id in ftrack_ids_to_create:
                    if ft_parent_id not in self._ft_failed_sync_ids:
                        parent = self._try_create_entity(ft_parent_id)

                elif ft_parent_id in folder_hierarchy_changes:
                    # If parent is also in hierarchy changes then make
                    #   sure the parent is already processed
                    # WARNING This may cause infinite loop or skip entities
                    #   accidentally if process of parent is missing
                    sources = (
                        self._ft_failed_sync_ids,
                        self._hierarchy_changed_by_ftrack_id,
                        self._created_entity_by_ftrack_id,
                    )
                    if all(
                        ft_parent_id not in source
                        for source in sources
                    ):
                        changes_queue.append(item)
                        continue

                else:
                    entity_ids = self.folder_ids_by_ftrack_id[ft_parent_id]
                    if len(entity_ids) == 1:
                        parent = self.entity_hub.get_or_fetch_entity_by_id(
                            entity_ids[0], ["folder"])

            if parent is None:
                self.log.info(f"Couldn't define parent of entity {entity.id}")
                self._ft_failed_sync_ids.add(ftrack_id)
                continue

            matching_entities = [
                child
                for child in parent.children
                if child.name == name
            ]
            if entity in matching_entities:
                matching_entities.remove(entity)

            if matching_entities:
                self.log.warning((
                    "Found more then one children with same name"
                    f" \"{entity.name}\" under parent {parent.path}."
                ))
                self._ft_failed_sync_ids.add(ftrack_id)
                continue

            self._hierarchy_changed_by_ftrack_id[ftrack_id] = entity
            if "name" in changes:
                entity.name = name
                entity.label = label

            if "parent_id" in changes:
                entity.parent_id = parent.id

            ft_entity = self.get_ftrack_entity_by_id(ftrack_id)
            entity.attribs[FTRACK_ID_ATTRIB] = ftrack_id
            entity.attribs[FTRACK_PATH_ATTRIB] = "/".join([
                item["name"]
                for item in ft_entity["link"]
                if item["type"] != "Project"
            ])
            self.log.debug(f"Updated hierarchy of {entity.path}")

        self.log.debug("Folder changes queue finished")

    def _process_removed_hierarchy_changes(self):
        # Handle removed entities
        # TODO it is possible to look for parent's children that can replace
        #   previously synchronized entity
        # - if removed entity has equivalent we can not remove it directly but
        #       look for "same name" (slugified) next to it if parent still
        #       exists (only if was not already synchronized).
        removed_items = list(self.entities_by_action["remove"].values())
        removed_items.sort(key=lambda info: len(info["parents"]))
        for info in reversed(removed_items):
            ftrack_id = info["entityId"]
            if info["entity_type"] == "Task":
                entity_ids = self.task_ids_by_ftrack_id[ftrack_id]
                entity_type = "task"
            else:
                entity_ids = self.folder_ids_by_ftrack_id[ftrack_id]
                entity_type = "folder"

            # We don't change if entity was not found
            if not entity_ids:
                continue

            # This can happen in some weird cases
            if len(entity_ids) > 1:
                # TODO handle this case somehow
                self.log.warning((
                    "Found more then one matching entity on server for"
                    f" ftrack id {ftrack_id} ({entity_ids}). Skipping"
                ))
                continue

            entity_id = entity_ids[0]
            entity = self.entity_hub.get_or_fetch_entity_by_id(
                entity_id, [entity_type])
            # Skip if entity was not found
            if entity is None:
                continue

            # First try find different ftrack entity that can "replace" the
            #   entity instead of previous
            #   - e.g. 'sh-01' was removed but 'sh_01' is there
            if self._try_find_other_match(info, entity):
                continue

            if (
                entity.entity_type == "folder"
                and entity.immutable_for_hierarchy
            ):
                # Change ftrack id to something else
                entity.attribs[FTRACK_ID_ATTRIB] = REMOVED_ID_VALUE
                continue

            # This will remove the entity
            path = entity.name
            if entity.entity_type == "folder":
                path = entity.path
            elif entity.entity_type == "task":
                if entity.parent:
                    path = f"{entity.parent.path}/{entity.name}"
            self.log.debug(f"Removing entity {path}")
            entity.parent_id = None

    def _process_created_hierarchy_changes(
        self, ftrack_ids_to_create
    ):
        created_ftrack_ids = set(self._created_entity_by_ftrack_id.keys())
        filtered_ftrack_ids_to_create = {
            ftrack_id
            for ftrack_id in ftrack_ids_to_create
            if (
                ftrack_id not in created_ftrack_ids
                and ftrack_id not in self._ft_failed_sync_ids
            )
        }
        if not filtered_ftrack_ids_to_create:
            return

        # TODO query entity id for entities from custom attributes
        #   they may be already filled there even if the entity is new
        for ftrack_id in filtered_ftrack_ids_to_create:
            self._try_create_entity(ftrack_id)

    def _get_server_id_by_ftrack_ids(
        self, ftrack_ids: set[str]
    ) -> dict[str, Optional[str]]:
        ayon_id_attr = self._get_server_id_attribute()
        value_items = query_custom_attribute_values(
            self.session,
            {ayon_id_attr["id"]},
            ftrack_ids,
        )
        ayon_id_by_ftrack_id = {ftrack_id: None for ftrack_id in ftrack_ids}
        ayon_id_by_ftrack_id.update({
            item["entity_id"]: item["value"]
            for item in value_items
            if item["value"]
        })
        return ayon_id_by_ftrack_id

    def _process_task_hierarchy_changes(self, task_hierarchy_changes):
        # TODO finish task name and parent changes
        for ftrack_id, info in task_hierarchy_changes.items():
            entity_ids = self.task_ids_by_ftrack_id[ftrack_id]
            if len(entity_ids) != 1:
                continue
            entity = self.entity_hub.get_or_fetch_entity_by_id(
                entity_ids[0], ["task"])

            if entity is None:
                continue

            changes = info["changes"]
            if "name" in changes:
                label = changes["name"]["new"]
                name = slugify_string(label)
            else:
                name = entity.name

            parent = None
            if "parent_id" not in changes:
                parent = entity.parent
            else:
                ft_parent_id = changes["parent_id"]["new"]
                # Cannot add task under project
                if ft_parent_id == self.ft_project_id:
                    self._ft_failed_sync_ids.add(ftrack_id)
                    continue

                entity_ids = self.folder_ids_by_ftrack_id[ft_parent_id]
                if len(entity_ids) == 1:
                    parent = self.entity_hub.get_or_fetch_entity_by_id(
                        entity_ids[0], ["folder"])

            if parent is None:
                self._ft_failed_sync_ids.add(ftrack_id)
                continue

            matching_entities = [
                child
                for child in parent.children
                if child.name == name
            ]
            if entity in matching_entities:
                matching_entities.remove(entity)

            # TODO if this happens we should maybe check if other matching task
            #   has set ftrack id?
            if matching_entities:
                self._ft_failed_sync_ids.add(ftrack_id)
                continue

            self._hierarchy_changed_by_ftrack_id[ftrack_id] = entity
            if "name" in changes:
                entity.name = name
                entity.label = label

            if "parent_id" in changes:
                entity.parent_id = parent.id

            entity.attribs[FTRACK_ID_ATTRIB] = ftrack_id

    def _process_hierarchy_changes(self):
        """Handle all hierarchy changes.

        Hierarchy change is creation of entity, removement of entity of change
        of parent id or name. In all these cases the changes may not be
        propagated.

        - created enity can have duplicated name as any already existing entity
            All names of entities from ftrack are "slugified" because of
                strict name regex.
        - changed or removed entities may already contain published content
            In case there is something published we don't allow to remove
                the entity. For removement of these entities is required to
                trigger special actions.
        """

        # Separate folder and task changes
        folder_hierarchy_changes = {}
        task_hierarchy_changes = {}

        for ftrack_id, info in self.entities_by_action["update"].items():
            if info["entityType"] == "show":
                continue

            changed_keys = info["keys"]
            if "parent_id" in changed_keys or "name" in changed_keys:
                if info["entity_type"] == "Task":
                    task_hierarchy_changes[ftrack_id] = info
                else:
                    folder_hierarchy_changes[ftrack_id] = info

        ftrack_ids_to_create = set(self.entities_by_action["add"].keys())
        self._clear_ayon_id_in_created_entities(ftrack_ids_to_create)
        self._process_folder_hierarchy_changes(
            folder_hierarchy_changes,
            ftrack_ids_to_create
        )
        self._process_removed_hierarchy_changes()
        self._process_created_hierarchy_changes(
            ftrack_ids_to_create
        )
        self._process_task_hierarchy_changes(
            task_hierarchy_changes
        )

    def _convert_value_by_cust_attr_conf(self, value, cust_attr_conf):
        type_id = cust_attr_conf["type_id"]
        cust_attr_type_name = self.ft_cust_attr_types_by_id[type_id]["name"]
        ignored = (
            "expression", "notificationtype", "dynamic enumerator"
        )
        if cust_attr_type_name in ignored:
            return None

        if cust_attr_type_name == "text":
            return value

        if cust_attr_type_name == "boolean":
            if value == "1":
                return True
            if value == "0":
                return False
            return bool(value)

        if cust_attr_type_name == "date":
            return arrow.get(value)

        cust_attr_config = json.loads(cust_attr_conf["config"])

        if cust_attr_type_name == "number":
            # Always convert to float ('1001.0' -> 1001.0) first
            #   - int('1001.0') -> is crashing
            value = float(value)
            if cust_attr_config["isdecimal"]:
                return value
            return int(value)

        if cust_attr_type_name == "enumerator":
            if not cust_attr_config["multiSelect"]:
                return value
            return value.split(", ")
        return value

    def _update_project_task_types(self):
        project_entity = self.entity_hub.project_entity
        src_task_types = {
            task_type["name"]: task_type
            for task_type in project_entity.task_types
        }

        new_task_types = []
        project_schema = self.ft_project["project_schema"]
        for task_type in project_schema["task_type_schema"]["types"]:
            task_type_name = task_type["name"]
            if task_type_name in src_task_types:
                new_task_types.append(src_task_types[task_type_name])
            else:
                new_task_types.append({
                    "name": task_type_name,
                    "shortName": re.sub(r"\W+", "", task_type_name.lower())
                })

        project_entity.task_types = new_task_types

    def _update_project_statuses(self):
        ft_project = self.ft_project
        ft_session = self.session
        fields = {
            "asset_version_workflow_schema",
            "task_workflow_schema",
            "task_workflow_schema_overrides",
            "object_type_schemas",
        }
        project_schema_id = ft_project["project_schema_id"]

        joined_fields = ", ".join(fields)
        project_schema = ft_session.query(
            f"select {joined_fields} from ProjectSchema"
            f" where id is '{project_schema_id}'"
        ).first()

        # Folder statuses
        schema_ids = {
            schema["id"]
            for schema in project_schema["object_type_schemas"]
        }
        object_type_schemas = []
        if schema_ids:
            joined_schema_ids = join_filter_values(schema_ids)
            object_type_schemas = ft_session.query(
                "select id, object_type_id from Schema"
                f" where id in ({joined_schema_ids})"
            ).all()

        object_type_schema_ids = {
            schema["id"]
            for schema in object_type_schemas
        }
        folder_statuses_ids = set()
        if object_type_schema_ids:
            joined_ot_schema_ids = join_filter_values(object_type_schema_ids)
            schema_statuses = ft_session.query(
                "select status_id from SchemaStatus"
                f" where schema_id in ({joined_ot_schema_ids})"
            ).all()
            folder_statuses_ids = {
                status["status_id"]
                for status in schema_statuses
            }

        # Task statues
        task_workflow_override_ids = {
            task_override["id"]
            for task_override in (
                project_schema["task_workflow_schema_overrides"]
            )
        }
        workflow_ids = set()
        if task_workflow_override_ids:
            joined_ids = join_filter_values(task_workflow_override_ids)
            override_schemas = ft_session.query(
                "select workflow_schema_id"
                f" from ProjectSchemaOverride"
                f" where id in ({joined_ids})"
            ).all()
            workflow_ids = {
                override_schema["workflow_schema_id"]
                for override_schema in override_schemas
            }

        workflow_ids.add(project_schema["task_workflow_schema"]["id"])
        joined_workflow_ids = join_filter_values(workflow_ids)
        workflow_statuses = ft_session.query(
            "select status_id"
            " from WorkflowSchemaStatus"
            f" where workflow_schema_id in ({joined_workflow_ids})"
        ).all()
        task_status_ids = {
            item["status_id"]
            for item in workflow_statuses
        }

        # Version statuses
        av_workflow_schema_id = (
            project_schema["asset_version_workflow_schema"]["id"]
        )
        version_statuse_ids = {
            item["status_id"]
            for item in ft_session.query(
                "select status_id"
                " from WorkflowSchemaStatus"
                f" where workflow_schema_id is '{av_workflow_schema_id}'"
            ).all()
        }

        statuses_by_id = {
            status["id"]: status
            for status in ft_session.query(
                "select id, name, color, state, sort from Status"
            ).all()
        }
        all_status_ids = (
            folder_statuses_ids
            | task_status_ids
            | version_statuse_ids
        )
        state_mapping = {
            "Blocked": "blocked",
            "Not Started": "not_started",
            "In Progress": "in_progress",
            "Done": "done",
        }
        statuses_data = []
        for status_id in all_status_ids:
            status = statuses_by_id[status_id]
            scope = ["representation", "workfile"]
            if status_id in folder_statuses_ids:
                scope.append("folder")
            if status_id in task_status_ids:
                scope.append("task")
            if status_id in version_statuse_ids:
                scope.append("product")
                scope.append("version")

            ft_state = status["state"]["name"]
            ayon_state = state_mapping[ft_state]
            statuses_data.append({
                "name": status["name"],
                "color": status["color"],
                "state": ayon_state,
                "scope": scope,
                "sort": status["sort"],
            })
        statuses_data.sort(key=lambda i: i["sort"])

        statuses = self._entity_hub.project_entity.statuses
        for idx, status_data in enumerate(statuses_data):
            status_item = statuses.get_status_by_slugified_name(
                status_data["name"]
            )
            if status_item is None:
                statuses.insert(idx, status_data)
                continue
            status_item.name = status_data["name"]
            status_item.color = status_data["color"]
            status_item.state = status_data["state"]
            status_item.scope = status_data["scope"]
            statuses.insert(idx, status_item)
        self._entity_hub.commit_changes()

    def _propagate_task_type_changes(self, task_type_changes):
        if not task_type_changes:
            return

        project_entity = self.entity_hub.project_entity
        task_types_names = {
            task_type["name"]
            for task_type in project_entity.task_types
        }
        task_types = self.ft_task_type_name_by_id

        to_change = []
        project_need_update = False
        for ftrack_id, (entity, info) in task_type_changes.items():
            new_type_id = info["changes"]["typeid"]["new"]
            new_type_name = task_types[new_type_id]
            if entity.task_type == new_type_name:
                continue

            if new_type_name not in task_types_names:
                project_need_update = True

            to_change.append((entity, new_type_name))

        if project_need_update:
            self._update_project_task_types()

        for entity, new_type_name in to_change:
            prev_task_type = entity.task_type
            entity.task_type = new_type_name
            self.log.debug(
                f"Changed task type {prev_task_type} -> {new_type_name}")

    def _propagate_status_changes(self, status_changes):
        if not status_changes:
            return

        project_entity = self.entity_hub.project_entity
        ayon_statuses_by_name = {
            status.name.lower(): status
            for status in project_entity.statuses
        }
        ft_status_names_by_id = self.ft_status_names_by_id
        to_change = []
        project_need_update = False
        for ftrack_id, (entity, info) in status_changes.items():
            new_status_id = info["changes"]["statusid"]["new"]
            new_status_name = ft_status_names_by_id[new_status_id]
            if entity.status.lower() == new_status_name.lower():
                continue

            ayon_status = ayon_statuses_by_name.get(new_status_name.lower())
            if (
                ayon_status is None
                or entity.entity_type not in ayon_status.scope
            ):
                project_need_update = True

            to_change.append((entity, ayon_status.name))

        if project_need_update:
            self._update_project_statuses()
            # Recalculate 'ayon_statuses_by_name' variable with new statuses
            project_entity = self.entity_hub.project_entity
            ayon_statuses_by_name = {
                status.name.lower(): status
                for status in project_entity.statuses
            }

        for entity, new_status_name in to_change:
            ayon_status = ayon_statuses_by_name.get(new_status_name.lower())
            if (
                ayon_status is None
                or entity.entity_type not in ayon_status.scope
            ):
                self.log.debug(
                    f"Status '{new_status_name}' not found on AYON project"
                )
                continue

            prev_status_name = entity.status
            entity.status = new_status_name
            self.log.debug(
                f"Changed status {prev_status_name} -> {new_status_name}")

    def _propagate_attrib_changes(self):
        # Prepare all created ftrack ids
        # - in that case it is not needed to update attributes as they have
        #   set all attributes from ftrack
        created_ftrack_ids = set(self._created_entity_by_ftrack_id.keys())
        task_type_changes = {}
        status_changes = {}
        for ftrack_id, info in self.entities_by_action["update"].items():
            if ftrack_id in created_ftrack_ids:
                continue

            entity = None
            if info["entityType"] == "show":
                entity = self.entity_hub.project_entity

            elif info["entityType"] == "task":
                if info["entity_type"] == "Task":
                    entity_ids = self.task_ids_by_ftrack_id[ftrack_id]
                    entity_types = ["task"]
                else:
                    entity_ids = self.folder_ids_by_ftrack_id[ftrack_id]
                    entity_types = ["folder"]

                if len(entity_ids) == 1:
                    entity_id = entity_ids[0]
                    entity = self.entity_hub.get_or_fetch_entity_by_id(
                        entity_id, entity_types
                    )

            if entity is None:
                continue

            attrib_changes = {}
            for key, change_info in info["changes"].items():
                value = change_info["new"]
                if key == "typeid":
                    if entity.entity_type == "task":
                        task_type_changes[ftrack_id] = (entity, info)
                    continue

                if key == "statusid":
                    status_changes[ftrack_id] = (entity, info)
                    continue

                if key in DEFAULT_ATTRS_MAPPING:
                    dst_key = DEFAULT_ATTRS_MAPPING[key]
                    if dst_key not in entity.attribs:
                        continue

                    if value is not None and key in ("startdate", "enddate"):
                        date = arrow.get(value)
                        # Shift date to 00:00:00 of the day
                        # - ftrack is returning e.g. '2024-10-29T22:00:00'
                        #  for '2024-10-30'
                        value = str(date.shift(hours=24 - date.hour))

                    entity.attribs[dst_key] = value
                    continue

                attrib_changes[key] = value

            if not attrib_changes:
                continue

            attrs_mapping: CustomAttributesMapping = self.cust_attr_mapping
            ft_entity = self.get_ftrack_entity_by_id(ftrack_id)

            for key, value in attrib_changes.items():
                mapping_item = attrs_mapping.get_mapping_item_by_key(
                    ft_entity, key
                )
                if mapping_item is None:
                    continue

                dst_key = mapping_item.ayon_attribute_name
                if dst_key not in entity.attribs:
                    continue

                if value is not None:
                    if dst_key == "fps":
                        value = convert_to_fps(value)
                    else:
                        attr = mapping_item.get_attr_conf_for_entity(
                            ft_entity
                        )
                        value = self._convert_value_by_cust_attr_conf(
                            value, attr
                        )

                entity.attribs[dst_key] = value

        self._propagate_task_type_changes(task_type_changes)
        self._propagate_status_changes(status_changes)

    def _propagate_assignee_changes(self):
        assignee_changes = self.entities_by_action["assignee_change"]
        if not assignee_changes:
            return

        # Initial preparation of user entities
        ftrack_users = self.session.query(
            "select id, username, email from User"
        ).all()
        ayon_user_by_ftrack_id = map_ftrack_users_to_ayon_users(ftrack_users)

        ent_info_by_task_id = {}
        for ent_info in assignee_changes.values():
            changes = ent_info["changes"]
            user_id_changes = changes["resource_id"]
            user_id = user_id_changes["new"] or user_id_changes["old"]
            ayon_user = ayon_user_by_ftrack_id.get(user_id)
            if not ayon_user:
                continue
            task_id_changes = changes["context_id"]
            task_id = task_id_changes["new"] or task_id_changes["old"]
            ent_info_by_task_id.setdefault(task_id, []).append(ent_info)

        for task_id, ent_infos in ent_info_by_task_id.items():
            entity_ids = self.task_ids_by_ftrack_id[task_id]
            if len(entity_ids) != 1:
                continue
            task_entity = self.entity_hub.get_or_fetch_entity_by_id(
                entity_ids[0], ["task"]
            )
            if task_entity is None:
                continue

            assignees = task_entity.assignees
            assignees_changed = False
            for ent_info in ent_infos:
                changes = ent_info["changes"]
                user_id_changes = changes["resource_id"]
                added = True
                user_id = user_id_changes["new"]
                if user_id is None:
                    added = False
                    user_id = user_id_changes["old"]

                ayon_user = ayon_user_by_ftrack_id.get(user_id)
                if added:
                    if ayon_user not in assignees:
                        assignees.append(ayon_user)
                        assignees_changed = True
                elif ayon_user in assignees:
                    assignees.remove(ayon_user)
                    assignees_changed = True

            if assignees_changed:
                task_entity.assignees = assignees

    def _propagate_link_changes(self):
        link_change = self.entities_by_action["link_change"]
        if not link_change:
            return

        links_info = []
        ftrack_ids = set()
        for ent_info in link_change:
            to_id_changes = ent_info["changes"]["to_id"]
            from_id_changes = ent_info["changes"]["from_id"]
            action = ent_info["action"]
            if action == "add":
                to_id = to_id_changes["new"]
                from_id = from_id_changes["new"]
            else:
                to_id = to_id_changes["old"]
                from_id = from_id_changes["old"]

            ftrack_ids |= {to_id, from_id}
            links_info.append(
                (from_id, to_id, action)
            )

        ayon_out_ids = set()
        added_links = []
        removed_links = []
        entities_by_id = self.get_ftrack_entity_by_ids(ftrack_ids)
        for (ft_from_id, ft_to_id, action) in links_info:
            ft_from_entity = entities_by_id[ft_to_id]
            ft_to_entity = entities_by_id[ft_to_id]
            if not ft_to_entity or not ft_from_entity:
                continue

            if (
                ft_from_entity.entity_type.lower() == "task"
                or ft_to_entity.entity_type.lower() == "task"
            ):
                continue

            ay_in_entity_ids = self.folder_ids_by_ftrack_id[ft_from_id]
            ay_out_entity_ids = self.folder_ids_by_ftrack_id[ft_to_id]
            if len(ay_in_entity_ids) != 1 or len(ay_out_entity_ids) != 1:
                continue
            ay_in_id = ay_in_entity_ids[0]
            ay_out_id = ay_out_entity_ids[0]
            ayon_out_ids.add(ay_out_id)
            if action == "add":
                added_links.append((ay_in_id, ay_out_id))
            else:
                removed_links.append((ay_in_id, ay_out_id))

        if not added_links and not removed_links:
            return

        ay_link_type = (
            self.project_settings
            ["ftrack"]
            ["service_event_handlers"]
            ["sync_from_ftrack"]
            ["sync_link_type"]
        )
        if ay_link_type == "< Skip >":
            self.log.info("Links sync is not set to be skipped.")
            return

        project_entity = self.get_ayon_project()
        exists = False
        for link_type in project_entity["linkTypes"]:
            if (
                link_type["linkType"] == ay_link_type
                and link_type["inputType"] == "folder"
                and link_type["outputType"] == "folder"
            ):
                exists = True

        if not exists:
            self.log.warning(
                f"Skipping links sync because link type '{ay_link_type}'"
                f" does not exist on project '{self.project_name}'."
            )
            return

        folder_links_by_id = get_folders_links(
            self.project_name,
            folder_ids=ayon_out_ids,
            link_types={ay_link_type},
            link_direction="in",
        )
        folder_link_ids_by_id = {}
        for ayon_id, links in folder_links_by_id.items():
            folder_link_ids_by_id[ayon_id] = {
                link["entityId"]: link
                for link in links
            }

        for (ay_in_id, ay_out_id) in removed_links:
            link = folder_link_ids_by_id[ay_out_id].get(ay_in_id)
            if link:
                delete_link(self.project_name, link["id"])

        for (ay_in_id, ay_out_id) in added_links:
            if ay_in_id not in folder_link_ids_by_id[ay_out_id]:
                create_link(
                    self.project_name,
                    ay_link_type,
                    ay_in_id,
                    "folder",
                    ay_out_id,
                    "folder",
                )

    def _propagate_list_changes(self) -> None:
        list_added = self.entities_by_action["list_added"]
        list_removed = self.entities_by_action["list_removed"]
        list_changed = self.entities_by_action["list_changed"]
        list_item_change = self.entities_by_action["list_item_change"]
        if not list_added and not list_removed and not list_changed:
            self._process_list_item_changes()
            return

        fields = ayon_api.get_default_fields_for_type("entityList")
        fields.add("allAttrib")
        ayon_lists = list(ayon_api.get_entity_lists(
            self.project_name,
            fields=fields
        ))
        ay_lists_by_ftrack_id = {}
        ay_lists_by_label_low = {}
        for ay_list in ayon_lists:
            all_attrib = json.loads(ay_list["allAttrib"] or "{}")
            ay_list["attrib"] = all_attrib
            ftrack_id = all_attrib.get(FTRACK_ID_ATTRIB)
            if ftrack_id:
                ay_lists_by_ftrack_id[ftrack_id] = ay_list
            label_low = ay_list["label"].lower()
            ay_lists_by_label_low[label_low] = ay_list

        for ent_info in list_removed:
            ftrack_id = ent_info["entityId"]
            # We don't care about changes of items related to the removed list
            list_item_change.pop(ftrack_id, None)

            # Try to find AYON list by ftrack id
            ay_list = ay_lists_by_ftrack_id.get(ftrack_id)
            if not ay_list:
                # Try to find AYON list by name
                name = ent_info["changes"]["name"]["old"].lower()
                ay_list = ay_lists_by_label_low.get(name)

            # NOTE we might check if the entity type of AYON list is
            #   actually the same?
            # - in case someone wants to "fix" wrong type of the ftrack list
            #   he will loose AYON list with this
            if ay_list:
                ayon_api.delete_entity_list(self.project_name, ay_list["id"])

        added_ids = set()
        for ent_info in list_added:
            ftrack_id = ent_info["entityId"]
            added_ids.add(ftrack_id)
            entity_type = "version"
            if ent_info["entity_type"] != "AssetVersionList":
                attr_def = self._get_list_type_attribute()
                if attr_def is None:
                    self.log.warning(
                        "Can't sync task List because of missing"
                        f" custom attribute '{CUST_ATTR_KEY_LIST_TYPE}'"
                    )
                    continue

                list_type = None
                for item in query_custom_attribute_values(
                    self.session,
                    {attr_def["id"]},
                    {ftrack_id},
                ):
                    value = item["value"]
                    if value:
                        list_type = value

                if list_type is None:
                    list_type = attr_def["default"]

                if list_type not in ("task", "folder"):
                    continue
                entity_type = list_type

            label = ent_info["changes"]["name"]["new"]
            ay_list = ay_lists_by_ftrack_id.get(ftrack_id)
            if not ay_list:
                # Try to find AYON list by name
                ay_list = ay_lists_by_label_low.get(label.lower())

            if ay_list:
                ay_entity_type = ay_list["entityType"]
                if ay_entity_type == entity_type:
                    self.log.info(f"List '{label}' already exists in AYON")
                else:
                    self.log.warning(
                        f"List '{label}' already exists but for different"
                        f" entity type (Expected '{entity_type}'"
                        f" Current: '{ay_entity_type})"
                    )
                continue

            self.log.info(f"Creating list '{label}' in AYON")
            response = ayon_api.post(
                f"projects/{self.project_name}/lists",
                entityType=entity_type,
                label=label,
                attrib={FTRACK_ID_ATTRIB: ftrack_id},
            )
            response.raise_for_status()
            # Create entity list has/d a bug using wrong endpoint in
            #   ayon_api 1.2.7
            # ayon_api.create_entity_list(
            #     self.project_name,
            #     "version",
            #     ft_list["name"],
            #     items=items,
            #     attrib={FTRACK_ID_ATTRIB: ftrack_id},
            # )

        # Propagate changes of list
        for ent_info in list_changed:
            ftrack_id = ent_info["entityId"]
            # Ignore created lists as all values are fetched anyways
            if ftrack_id in added_ids:
                continue

            # Find ayon list
            ay_list = ay_lists_by_ftrack_id.get(ftrack_id)
            ay_changes = {}
            if not ay_list:
                ft_list = self.session.query(
                    f"select name from List where id is '{ftrack_id}'"
                ).first()
                if ft_list is None:
                    continue
                name = ft_list["name"].lower()
                ay_list = ay_lists_by_label_low.get(name)
                if ay_list:
                    ay_changes["attrib"] = {FTRACK_ID_ATTRIB: ftrack_id}

            if not ay_list:
                continue

            if "name" in ent_info["changes"]:
                name = ent_info["changes"]["name"]["new"]
                if name != ay_list["label"]:
                    ay_changes["label"] = name

            if ay_changes:
                ayon_api.update_entity_list(
                    self.project_name,
                    ay_list["id"],
                    **ay_changes
                )

            # Handle changes of 'ayon_list_type'. Right now it changes value
            #   to entity type set on AYON's list if exists.
            # NOTE it might be possible to change the type, but that would
            #   to delete AYON list and create new
            if (
                CUST_ATTR_KEY_LIST_TYPE not in ent_info["changes"]
                or ent_info["entity_type"] != "TypedContextList"
            ):
                continue

            attr_def = self._get_list_type_attribute()
            new_value = None
            for item in query_custom_attribute_values(
                self.session, {attr_def["id"]}, {ftrack_id}
            ):
                value = item["value"]
                if value is not None:
                    new_value = value

            if new_value == ay_list["entityType"]:
                continue

            op = self._create_ft_attr_operation(
                attr_def["id"],
                ftrack_id,
                new_value is None,
                ay_list["entityType"],
                new_value
            )
            self.session.recorded_operations.push(op)

        self._process_list_item_changes(ayon_lists)

    def _process_list_item_changes(
        self, ayon_lists: Optional[list[dict[str, Any]]] = None
    ) ->None:
        list_item_change: dict[str, list[dict[str, Any]]] = (
            self.entities_by_action["list_item_change"]
        )
        if not list_item_change:
            return

        # TODO implement list item changes propagation
        ft_version_ids = set()
        ft_entity_ids = set()
        ent_info_by_list_id = {}
        for list_id, ent_infos in list_item_change.items():
            list_type = None
            for ent_info in ent_infos:
                if list_type is None:
                    for parent in ent_info["parents"]:
                        if parent["entityType"] == "list":
                            list_type = parent["entity_type"]
                            break

                action = ent_info["action"]
                if action == "add":
                    entity_id = ent_info["changes"]["entityid"]["new"]
                else:
                    entity_id = ent_info["changes"]["entityid"]["old"]

                ent_info_by_list_id.setdefault(list_id, []).append(ent_info)
                if list_type == "AssetVersionList":
                    ft_version_ids.add(entity_id)
                else:
                    ft_entity_ids.add(entity_id)

        if not ent_info_by_list_id:
            return

        if ayon_lists is None:
            fields = ayon_api.get_default_fields_for_type("entityList")
            fields.add("allAttrib")
            ayon_lists = list(ayon_api.get_entity_lists(
                self.project_name,
                fields=fields
            ))

        # No lists, nothing to update...
        if not ayon_lists:
            return

        ay_lists_by_ftrack_id = {}
        ay_lists_by_label_low = {}
        for ay_list in ayon_lists:
            all_attrib = json.loads(ay_list["allAttrib"] or "{}")
            ay_list["attrib"] = all_attrib
            ftrack_id = all_attrib.get(FTRACK_ID_ATTRIB)
            if ftrack_id:
                ay_lists_by_ftrack_id[ftrack_id] = ay_list
            label_low = ay_list["label"].lower()
            ay_lists_by_label_low[label_low] = ay_list

        ids_mapping = self._get_server_id_by_ftrack_ids(
            ft_entity_ids | ft_version_ids
        )

        missing_ft_version_ids = set()
        for ftrack_id in ft_version_ids:
            ayon_id = ids_mapping[ftrack_id]
            if not ayon_id:
                missing_ft_version_ids.add(ftrack_id)

        ids_mapping.update(self._find_matching_ayon_versions(
            missing_ft_version_ids
        ))

        ft_missing_list_ids = set()
        for ft_list_id in ent_info_by_list_id:
            ay_list = ay_lists_by_ftrack_id.get(ft_list_id)
            if not ay_list:
                ft_missing_list_ids.add(ft_list_id)

        if ft_missing_list_ids:
            joined_list_ids = join_filter_values(ft_missing_list_ids)
            ft_lists = self.session.query(
                f"select id, name from List where id in ({joined_list_ids})"
            ).all()
            for ft_list in ft_lists:
                name = ft_list["name"].lower()
                ay_list = ay_lists_by_label_low.get(name)
                if ay_list:
                    ayon_api.update_entity_list(
                        self.project_name,
                        ay_list["id"],
                        attrib={FTRACK_ID_ATTRIB: ft_list["id"]}
                    )
                    ay_lists_by_ftrack_id[ft_list["id"]] = ay_list

        ft_task_ids = set()
        ft_folder_ids = set()
        if ft_entity_ids:
            joined_entity_ids = join_filter_values(ft_entity_ids)
            task_type = self.session.query(
                "select id from ObjectType where name is 'Task'"
            ).one()
            for entity in self.session.query(
                "select id, object_type_id from TypedContext"
                f" where id in ({joined_entity_ids})"
            ):
                if entity["object_type_id"] == task_type["id"]:
                    ft_task_ids.add(entity["id"])
                else:
                    ft_folder_ids.add(entity["id"])

        ayon_list_ids = set()
        for ft_list_id, ent_infos in ent_info_by_list_id.items():
            ayon_list = ay_lists_by_ftrack_id.get(ft_list_id)
            if ayon_list:
                ayon_list_ids.add(ayon_list["id"])

        if not ayon_list_ids:
            return

        for ft_list_id, ent_infos in ent_info_by_list_id.items():
            ayon_list = ay_lists_by_ftrack_id.get(ft_list_id)
            if not ayon_list:
                # QUESTION should we create the list in AYON?
                continue

            list_type = ayon_list["entityType"]
            items_by_entity_id = {
                item["entityId"]: item
                for item in ayon_list["items"]
            }

            to_add_ids = set()
            to_remove_ids = set()
            for ent_info in ent_infos:
                action = ent_info["action"]
                if action == "add":
                    entity_id = ent_info["changes"]["entityid"]["new"]
                else:
                    entity_id = ent_info["changes"]["entityid"]["old"]

                ayon_id = ids_mapping.get(entity_id)
                if not ayon_id:
                    continue

                # Filter items that have incompatible type for AYON list
                if list_type == "folder":
                    if entity_id not in ft_folder_ids:
                        continue
                elif list_type == "task":
                    if entity_id not in ft_task_ids:
                        continue
                elif list_type == "version":
                    if entity_id not in ft_version_ids:
                        continue

                if action == "add":
                    if ayon_id not in items_by_entity_id:
                        to_add_ids.add(ayon_id)
                else:
                    item = items_by_entity_id.get(ayon_id)
                    if item:
                        to_remove_ids.add(item["id"])

            if to_remove_ids:
                self._update_entity_list_items(
                    self.project_name,
                    ayon_list["id"],
                    items=[{"id": i} for i in to_remove_ids],
                    mode="delete",
                )

            if to_add_ids:
                self._update_entity_list_items(
                    self.project_name,
                    ayon_list["id"],
                    items=[{"entityId": i} for i in to_add_ids],
                    mode="merge",
                )

    def _update_entity_list_items(
        self,
        project_name: str,
        list_id: str,
        items: list[dict[str, Any]],
        mode: str,
    ) -> None:
        # TODO remove when ayon_api has fixed bug (used POST instead of PATCH)
        #   is not fixed in 1.2.7
        response = ayon_api.patch(
            f"projects/{project_name}/lists/{list_id}/items",
            items=items,
            mode=mode,
        )
        response.raise_for_status()

    def _find_matching_ayon_versions(
        self, ftrack_ids: set[str]
    ) -> dict[str, Optional[str]]:
        """Versions did not have stored ayon_id in attribute for some time."""
        output = {i: None for i in ftrack_ids}
        if not ftrack_ids:
            return output

        joined_av_ids = join_filter_values(ftrack_ids)
        asset_versions = self.session.query(
            f"select id, asset_id, version from AssetVersion"
            f" where id in ({joined_av_ids})"
        ).all()
        ft_version_ints = {av["version"] for av in asset_versions}
        asset_ids = {av["asset_id"] for av in asset_versions}
        av_by_asset_id = {a_id: [] for a_id in asset_ids}
        for av in asset_versions:
            av_by_asset_id[av["asset_id"]].append(av)

        joined_asset_id = join_filter_values(asset_ids)
        
        assets = self.session.query(
            "select id, name, context_id from Asset"
            f" where id in ({joined_asset_id})"
        ).all()
        assets_by_id = {a["id"]: a for a in assets}
        context_ids = {a["context_id"] for a in assets}
        assets_by_parent_id = {i: [] for i in context_ids}
        for asset in assets:
            assets_by_parent_id[asset["context_id"]].append(asset)

        parents_mapping = self._get_server_id_by_ftrack_ids(context_ids)

        folder_ids = {
            folder_id
            for folder_id in parents_mapping.values()
            if folder_id
        }
        if not folder_ids:
            return output

        product_ids = set()
        products_by_folder_id = {i: [] for i in folder_ids}
        for product in ayon_api.get_products(
            self.project_name,
            folder_ids=folder_ids,
            fields={"id", "name", "folderId"}
        ):
            folder_id = product["folderId"]
            product_ids.add(product["id"])
            products_by_folder_id[folder_id].append(product)

        version_ids_by_product_id = {i: {} for i in product_ids}
        for version_entity in ayon_api.get_versions(
            self.project_name,
            versions=ft_version_ints,
            product_ids=product_ids,
            fields={"id", "version", "productId"},
        ):
            product_id = version_entity["productId"]
            version = version_entity["version"]
            version_id = version_entity["id"]
            version_ids_by_product_id[product_id][version] = version_id

        for asset_version in asset_versions:
            ft_asset_id = asset_version["asset_id"]
            ft_asset = assets_by_id[ft_asset_id]
            ft_context_id = ft_asset["context_id"]
            folder_id = parents_mapping.get(ft_context_id)
            if not folder_id:
                continue

            products = products_by_folder_id.get(folder_id)
            if not products:
                continue

            ft_asset_name = ft_asset["name"].lower()
            matching_product = None
            alternatives = []
            for product_entity in products:
                product_name = product_entity["name"].lower()
                if product_name == ft_asset_name:
                    matching_product = product_entity
                    break
                if product_name in ft_asset_name:
                    alternatives.append(product_entity)

            if matching_product is None and alternatives:
                matching_product = alternatives[0]

            if not matching_product:
                continue

            product_id = matching_product["id"]
            ft_version = asset_version["version"]
            version_id = version_ids_by_product_id[product_id].get(ft_version)
            if version_id:
                output[asset_version["id"]] = version_id

        return output

    def _create_ft_attr_operation(
        self, conf_id, entity_id, is_new, new_value, old_value=None
    ):
        entity_key = collections.OrderedDict((
            ("configuration_id", conf_id),
            ("entity_id", entity_id)
        ))
        if is_new:
            return ftrack_api.operation.CreateEntityOperation(
                "CustomAttributeValue",
                entity_key,
                {"value": new_value}
            )

        return ftrack_api.operation.UpdateEntityOperation(
            "CustomAttributeValue",
            entity_key,
            "value",
            new_value,
            old_value
        )

    def _propagate_ftrack_attributes(self):
        entities_by_ftrack_id = {}
        for source in (
            self._created_entity_by_ftrack_id,
            self._hierarchy_changed_by_ftrack_id,
            self._remapped_entity_by_ftrack_id,
        ):
            for ftrack_id, entity in source.items():
                entities_by_ftrack_id[ftrack_id] = entity

        ftrack_ids = set(entities_by_ftrack_id.keys())
        ftrack_ids |= self._ft_failed_sync_ids
        if not ftrack_ids:
            return

        # Query ftrack entities to find out which ftrack entities actually
        #   exists
        # - they may be removed meanwhile this event is processed and ftrack
        #   session would crash if we would try to change custom attributes
        #   of not existing entities
        ft_entities = self.session.query((
            "select id from TypedContext"
            f" where id in ({join_filter_values(ftrack_ids)})"
        )).all()
        ftrack_ids = {
            ft_entity["id"]
            for ft_entity in ft_entities
        }
        if not ftrack_ids:
            return

        server_id_attr = path_attr = fail_attr = None
        for attr in self.ft_cust_attrs:
            if not attr["is_hierarchical"]:
                continue
            if attr["key"] == CUST_ATTR_KEY_SERVER_ID:
                server_id_attr = attr
            elif attr["key"] == CUST_ATTR_KEY_SERVER_PATH:
                path_attr = attr
            elif attr["key"] == CUST_ATTR_KEY_SYNC_FAIL:
                fail_attr = attr

        server_id_attr_id = server_id_attr["id"]
        path_attr_id = path_attr["id"]
        fail_attr_id = fail_attr["id"]

        attr_key_by_id = {
            attr["id"]: attr["key"]
            for attr in (server_id_attr, path_attr, fail_attr)
        }

        value_items = query_custom_attribute_values(
            self.session,
            set(attr_key_by_id.keys()),
            ftrack_ids,
        )

        current_values = {
            ftrack_id: {}
            for ftrack_id in ftrack_ids
        }
        for item in value_items:
            attr_id = item["configuration_id"]
            entity_id = item["entity_id"]
            current_values[entity_id][attr_id] = item["value"]

        expected_values = {
            ftrack_id: {}
            for ftrack_id in ftrack_ids
        }
        for ftrack_id in ftrack_ids:
            entity_values = expected_values[ftrack_id]
            entity = entities_by_ftrack_id.get(ftrack_id)
            failed = entity is None
            entity_values[fail_attr_id] = failed
            if failed:
                # Limit attribute updates only to failed boolean if sync failed
                # - we want to keep path and id to potentially fix the issue by
                #   knowing the path (without ftrack path user may have issues
                #   to recreate it)
                # Set default values to avoid inheritance from parent
                current_entity_values = current_values[ftrack_id]
                for key, value in (
                    (path_attr_id, ""),
                    (server_id_attr_id, "")
                ):
                    if not current_entity_values.get(key):
                        entity_values[key] = value
                continue

            # TODO we should probably add path to tasks too
            # - what the format should look like?
            path = ""
            if entity.entity_type == "folder":
                path = entity.path
            entity_values[path_attr_id] = path
            entity_values[server_id_attr_id] = entity.id

        operations = []
        for ftrack_id, entity_values in expected_values.items():
            current_entity_values = current_values[ftrack_id]
            for attr_id, value in entity_values.items():
                cur_value = current_entity_values.get(attr_id)
                if value != cur_value:
                    operations.append(
                        self._create_ft_attr_operation(
                            attr_id,
                            ftrack_id,
                            attr_id not in current_entity_values,
                            value,
                            old_value=cur_value
                        )
                    )

        if operations:
            for chunk in create_chunks(operations, 500):
                for operation in chunk:
                    self.session.recorded_operations.push(operation)
                self.session.commit()

    def _get_server_id_attribute(self):
        for attr in self.ft_cust_attrs:
            if attr["key"] == CUST_ATTR_KEY_SERVER_ID:
                return attr
        return None

    def _get_list_type_attribute(self):
        for attr in self.ft_cust_attrs:
            if attr["key"] == CUST_ATTR_KEY_LIST_TYPE:
                return attr
        return None

    def process_event_data(self):
        # Check if auto-sync custom attribute exists
        entities_by_action = self.entities_by_action
        debug_action_map = {
            "add": "Created",
            "remove": "Removed",
            "update": "Updated",
            "assignee_change": "Assignee changed",
            "link_change": "Link changed",
            "list_added": "List added",
            "list_removed": "List removed",
            "list_changed": "List changed",
            "list_item_change": "List item added/removed",
        }
        debug_msg = "\n".join([
            f"- {debug_action_map[action]}: {len(entities_info)}"
            for action, entities_info in entities_by_action.items()
        ])

        self.log.debug(
            f"Project \"{self.project_name}\" changes\n{debug_msg}")

        # Get ftrack entities - find all ftrack ids first
        ftrack_ids = set()
        for action in {"add", "update"}:
            ftrack_ids |= set(entities_by_action[action].keys())

        # Add task ids from assignees changes
        for ent_info in entities_by_action["assignee_change"].values():
            context_id_changes = ent_info["changes"]["context_id"]
            ftrack_id = context_id_changes["new"] or context_id_changes["old"]
            ftrack_ids.add(ftrack_id)

        for ent_info in entities_by_action["link_change"]:
            to_id_changes = ent_info["changes"]["to_id"]
            from_id_changes = ent_info["changes"]["from_id"]
            action = ent_info["action"]
            if action == "add":
                to_id = to_id_changes["new"]
                from_id = from_id_changes["new"]
            else:
                to_id = to_id_changes["old"]
                from_id = from_id_changes["old"]
            # NOTE this works until 'update' is handled
            ftrack_ids.add(to_id)
            ftrack_ids.add(from_id)

        # Precache entities that will be needed in single call
        if ftrack_ids:
            self.get_ftrack_entity_by_ids(ftrack_ids)

        self.log.debug("Synchronization begins")
        try:
            time_1 = time.time()
            # 1. Process hierarchy changes - may affect all other actions
            # - hierarchy changes => name or parent_id changes
            self._process_hierarchy_changes()
            time_2 = time.time()
            # 2. Propagate custom attribute changes
            self._propagate_attrib_changes()
            time_3 = time.time()
            # 3. Propage assigneess changes
            self._propagate_assignee_changes()
            time_4 = time.time()
            # 4. Propage link changes
            self._propagate_link_changes()
            time_5 = time.time()
            # 5. Propage list changes
            self._propagate_list_changes()
            time_6 = time.time()
            # 6. Commit changes to server
            self.entity_hub.commit_changes()
            # 7. Propagate entity changes to ftack
            time_7 = time.time()
            self._propagate_ftrack_attributes()
            # TODO propagate entities to ftrack
            #  - server id, server path, sync failed
            time_8 = time.time()

            total_time = f"{time_8 - time_1:.2f}"
            mid_times = ", ".join([
                f"{diff:.2f}"
                for diff in (
                    time_2 - time_1,
                    time_3 - time_2,
                    time_4 - time_3,
                    time_5 - time_4,
                    time_6 - time_5,
                    time_7 - time_6,
                    time_8 - time_7,
                )
            ])
            self.log.debug(f"Process time: {total_time} <{mid_times}>")

        except Exception:
            msg = "An error has happened during synchronization"
            self.log.warning(msg, exc_info=True)
            # self.report_items["error"][msg].append((
            #     str(traceback.format_exc()).replace("\n", "<br>")
            # ).replace(" ", "&nbsp;"))

        # self.report()
        return True


class AutoSyncFromFtrack(BaseEventHandler):
    report_splitter = {"type": "label", "value": "---"}

    def __init__(self, session):
        '''Expects a ftrack_api.Session instance'''

        # Set processing session to not use global
        self.set_process_session(session)
        super().__init__(session)

    def set_process_session(self, session):
        try:
            self.process_session.close()
        except Exception:
            pass
        self.process_session = ftrack_api.Session(
            server_url=session.server_url,
            api_key=session.api_key,
            api_user=session.api_user,
            auto_connect_event_hub=True
        )
        atexit.register(lambda: self.process_session.close())

    def launch(self, session, event):
        """
            Main entry port for synchronization.
            Goes through event (can contain multiple changes) and decides if
            the event is interesting for us (interest_base_types).
            It separates changes into add|remove|update.
            All task changes are handled together by refresh from ftrack.
        Args:
            session (ftrack_api.Session): session to ftrack
            event (dictionary): event content

        Returns:
            (boolean or None)
        """
        # Try to commit and if any error happen then recreate session
        try:
            self.process_session.commit()
        except Exception:
            self.set_process_session(session)

        sync_process = SyncProcess(
            self, self.process_session, event, self.log
        )

        sync_process.initial_event_processing()
        if sync_process.project_changed_autosync:
            username = self._get_username(
                sync_process.session, sync_process.event)
            on_state = "off"
            sub_msg = ""
            if sync_process.trigger_project_sync:
                on_state = "on"
                sub_msg = " Triggering sync from ftrack action."

            self.log.debug((
                f"Auto sync was turned {on_state} for project"
                f" \"{sync_process.project_name}\" by \"{username}\".{sub_msg}"
            ))
            if sync_process.trigger_project_sync:
                # Trigger sync to AYON action if auto sync was turned on
                selection = [{
                    "entityId": sync_process.ft_project_id,
                    "entityType": "show"
                }]
                # TODO uncomment when out of testing stage
                self.trigger_action(
                    action_identifier="sync.from.ftrack.to.ayon",
                    event=sync_process.event,
                    selection=selection
                )

        if not sync_process.has_valid_entity_types:
            return

        if sync_process.ft_project is None:
            self.log.warning(
                "Project was not found. Skipping."
                f"\nEvent data: {event['data']}\n"
            )
            return

        if not self.get_ayon_project_from_event(
            event, sync_process.project_name
        ):
            self.log.debug(
                f"Project '{sync_process.project_name}' was not"
                " found in AYON. Skipping."
            )
            return

        if not sync_process.is_event_valid:
            self.log.debug(
                f"Project '{sync_process.project_name}' has disabled"
                " autosync. Skipping."
            )
            return

        sync_process.process_event_data()

    def _get_username(self, session, event):
        username = "Unknown"
        event_source = event.get("source")
        if not event_source:
            return username
        user_info = event_source.get("user")
        if not user_info:
            return username
        user_id = user_info.get("id")
        if not user_id:
            return username

        user_entity = session.query(
            "User where id is {}".format(user_id)
        ).first()
        if user_entity:
            username = user_entity["username"] or username
        return username

    # @property
    # def duplicated_report(self):
    #     if not self.duplicated:
    #         return []
    #
    #     ft_project = self.cur_project
    #     duplicated_names = []
    #     for ftrack_id in self.duplicated:
    #         ftrack_ent = self.ftrack_ents_by_id.get(ftrack_id)
    #         if not ftrack_ent:
    #             ftrack_ent = self.process_session.query(
    #                 self.entities_query_by_id.format(
    #                     ft_project["id"], ftrack_id
    #                 )
    #             ).one()
    #             self.ftrack_ents_by_id[ftrack_id] = ftrack_ent
    #         name = ftrack_ent["name"]
    #         if name not in duplicated_names:
    #             duplicated_names.append(name)
    #
    #     joined_names = ", ".join(
    #         ["\"{}\"".format(name) for name in duplicated_names]
    #     )
    #     ft_ents = self.process_session.query(
    #         self.entities_name_query_by_name.format(
    #             ft_project["id"], joined_names
    #         )
    #     ).all()
    #
    #     ft_ents_by_name = collections.defaultdict(list)
    #     for ft_ent in ft_ents:
    #         name = ft_ent["name"]
    #         ft_ents_by_name[name].append(ft_ent)
    #
    #     if not ft_ents_by_name:
    #         return []
    #
    #     subtitle = "Duplicated entity names:"
    #     items = []
    #     items.append({
    #         "type": "label",
    #         "value": "# {}".format(subtitle)
    #     })
    #     items.append({
    #         "type": "label",
    #         "value": (
    #             "<p><i>NOTE: It is not allowed to use the same name"
    #             " for multiple entities in the same project</i></p>"
    #         )
    #     })
    #
    #     for name, ents in ft_ents_by_name.items():
    #         items.append({
    #             "type": "label",
    #             "value": "## {}".format(name)
    #         })
    #         paths = []
    #         for ent in ents:
    #             ftrack_id = ent["id"]
    #             ent_path = "/".join([_ent["name"] for _ent in ent["link"]])
    #             avalon_ent = self.avalon_ents_by_id.get(ftrack_id)
    #
    #             if avalon_ent:
    #                 additional = " (synchronized)"
    #                 if avalon_ent["name"] != name:
    #                     additional = " (synchronized as {})".format(
    #                         avalon_ent["name"]
    #                     )
    #                 ent_path += additional
    #             paths.append(ent_path)
    #
    #         items.append({
    #             "type": "label",
    #             "value": '<p>{}</p>'.format("<br>".join(paths))
    #         })
    #
    #     return items
    #
    # @property
    # def regex_report(self):
    #     if not self.regex_failed:
    #         return []
    #
    #     subtitle = "Entity names contain prohibited symbols:"
    #     items = []
    #     items.append({
    #         "type": "label",
    #         "value": "# {}".format(subtitle)
    #     })
    #     items.append({
    #         "type": "label",
    #         "value": (
    #             "<p><i>NOTE: You can use Letters( a-Z ),"
    #             " Numbers( 0-9 ) and Underscore( _ )</i></p>"
    #         )
    #     })
    #
    #     ft_project = self.cur_project
    #     for ftrack_id in self.regex_failed:
    #         ftrack_ent = self.ftrack_ents_by_id.get(ftrack_id)
    #         if not ftrack_ent:
    #             ftrack_ent = self.process_session.query(
    #                 self.entities_query_by_id.format(
    #                     ft_project["id"], ftrack_id
    #                 )
    #             ).one()
    #             self.ftrack_ents_by_id[ftrack_id] = ftrack_ent
    #
    #         name = ftrack_ent["name"]
    #         ent_path_items = [
    #             _ent["name"] for _ent in ftrack_ent["link"][:-1]
    #         ]
    #         ent_path_items.append("<strong>{}</strong>".format(name))
    #         ent_path = "/".join(ent_path_items)
    #         items.append({
    #             "type": "label",
    #             "value": "<p>{} - {}</p>".format(name, ent_path)
    #         })
    #
    #     return items
    #
    # def report(self):
    #     msg_len = len(self.duplicated) + len(self.regex_failed)
    #     for msgs in self.report_items.values():
    #         msg_len += len(msgs)
    #
    #     if msg_len == 0:
    #         return
    #
    #     items = []
    #     project_name = self.cur_project["full_name"]
    #     title = "Synchronization report ({}):".format(project_name)
    #
    #     keys = ["error", "warning", "info"]
    #     for key in keys:
    #         subitems = []
    #         if key == "warning":
    #             subitems.extend(self.duplicated_report)
    #             subitems.extend(self.regex_report)
    #
    #         for _msg, _items in self.report_items[key].items():
    #             if not _items:
    #                 continue
    #
    #             msg_items = _msg.split("||")
    #             msg = msg_items[0]
    #             subitems.append({
    #                 "type": "label",
    #                 "value": "# {}".format(msg)
    #             })
    #
    #             if len(msg_items) > 1:
    #                 for note in msg_items[1:]:
    #                     subitems.append({
    #                         "type": "label",
    #                         "value": "<p><i>NOTE: {}</i></p>".format(note)
    #                     })
    #
    #             if isinstance(_items, str):
    #                 _items = [_items]
    #             subitems.append({
    #                 "type": "label",
    #                 "value": '<p>{}</p>'.format("<br>".join(_items))
    #             })
    #
    #         if items and subitems:
    #             items.append(self.report_splitter)
    #
    #         items.extend(subitems)
    #
    #     self.show_interface(
    #         items=items,
    #         title=title,
    #         event=self._cur_event
    #     )
    #     return True
