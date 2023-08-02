import re
import collections
import json
import time
import atexit

import arrow
import ftrack_api

from ayon_api import (
    get_project,
    get_folders,
    get_tasks,
    slugify_string,
)

from ayon_api.entity_hub import EntityHub
from ftrack_common import (
    BaseEventHandler,

    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
    REMOVED_ID_VALUE,

    CUST_ATTR_KEY_SERVER_ID,
    CUST_ATTR_KEY_SERVER_PATH,
    CUST_ATTR_KEY_SYNC_FAIL,

    CUST_ATTR_AUTO_SYNC,
    FPS_KEYS,

    get_ayon_attr_configs,
    query_custom_attribute_values,

    convert_to_fps,

    create_chunks,
    join_filter_values,
)

UNKNOWN_VALUE = object()


class SyncProcess:
    interest_base_types = ["show", "task"]
    ignore_ent_types = ["Milestone"]
    ignore_change_keys = ["statusid", "thumbid"]

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

    def __init__(self, session, event, log):
        self.event = event
        self.session = session
        self.log = log

        self._ft_project_id = UNKNOWN_VALUE
        self._ft_project = UNKNOWN_VALUE
        self._project_name = UNKNOWN_VALUE

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

        # Caches from ftrack
        self._ft_cust_attr_types_by_id = None
        self._ft_cust_attrs = None
        self._ft_hier_cust_attrs = None
        self._ft_std_cust_attrs = None
        self._ft_object_type_name_by_id = None
        self._ft_task_type_name_by_id = None
        
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

    @property
    def project_name(self):
        """

        Returns:
            str: Name of project on which happened changes in processed event.
        """

        if self._project_name is UNKNOWN_VALUE:
            self._project_name = self.ft_project["full_name"]
        return self._project_name

    @property
    def ft_project_id(self):
        """

        Returns:
            Union[str, None]: Id of ftrack project based on information in
                processed event.
        """

        if self._ft_project_id is UNKNOWN_VALUE:
            found_id = None
            for ent_info in self.event["data"]["entities"]:
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
            ftrack_api.Entity: Ftrack project entity.
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
    def ft_cust_attrs(self):
        if self._ft_cust_attrs is None:
            self._ft_cust_attrs = get_ayon_attr_configs(
                self.session, query_keys=self.cust_attr_query_keys
            )
        return self._ft_cust_attrs

    @property
    def ft_hier_cust_attrs(self):
        if self._ft_hier_cust_attrs is None:
            hier_attrs = self.ft_cust_attrs[1]
            self._ft_hier_cust_attrs = {
                attr["key"]: attr
                for attr in hier_attrs
            }
        return self._ft_hier_cust_attrs

    @property
    def ft_std_cust_attrs(self):
        if self._ft_std_cust_attrs is None:
            ft_std_cust_attrs = collections.defaultdict(dict)
            attrs = self.ft_cust_attrs[0]
            for attr in attrs:
                object_type_id = attr["object_type_id"]
                key = attr["key"]
                ft_std_cust_attrs[object_type_id][key] = attr
            self._ft_std_cust_attrs = ft_std_cust_attrs

        return self._ft_std_cust_attrs

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

        self._split_event_entity_info()

        # If project was removed then skip rest of event processing
        if (
            self._ft_project_removed
            or not self._found_actions
        ):
            return

        self._chek_enabled_auto_sync()
        if self._project_changed_autosync:
            return

        self._filter_update_actions()

        if not self._found_actions:
            self.log.debug("Skipping. Nothing to update.")
            return

        # NOTE This if first part of code which should query entity from ftrack
        # Query project and check if can be actually queried and if has
        #   available custom attribute that is used to identify if project
        #   should be autosynced.
        ft_project = self.ft_project
        if ft_project is None:
            self.log.error("Failed to query ftrack project. Skipping event")
            return

        if CUST_ATTR_AUTO_SYNC not in ft_project["custom_attributes"]:
            # TODO should we sent message to someone?
            self.log.error((
                f"Custom attribute \"{CUST_ATTR_AUTO_SYNC}\" is not created"
                f" or user \"{self.session.api_user}\" used"
                " for Event server don't have permissions to access it!"
            ))
            return

        # Skip if auto-sync is not set
        auto_sync = ft_project["custom_attributes"][CUST_ATTR_AUTO_SYNC]
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
            "add": {}
        }
        found_actions = set()
        ft_project_removed = False
        for ent_info in self.event["data"]["entities"]:
            base_type = ent_info["entityType"]
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

    def _chek_enabled_auto_sync(self):
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
            entity = self.entity_hub.get_or_query_entity_by_id(
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
                parent = self.entity_hub.get_or_query_entity_by_id(
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

        hier_attrs_by_id = {
            attr["id"]: attr
            for attr in self.ft_hier_cust_attrs.values()
        }
        obj_type_id = ft_entity["object_type_id"]
        std_attrs_by_id = {
            attr["id"]: attr
            for attr in self.ft_std_cust_attrs[obj_type_id].values()
        }
        attr_ids = set(hier_attrs_by_id.keys()) | set(std_attrs_by_id.keys())
        value_items = query_custom_attribute_values(
            self.session, attr_ids, [ftrack_id]
        )
        attr_values_by_key = {}
        for item in value_items:
            value = item["value"]
            if value is None:
                continue
            attr_id = item["configuration_id"]
            attr = hier_attrs_by_id.get(attr_id)
            is_hier = True
            if attr is None:
                is_hier = False
                attr = std_attrs_by_id.get(attr_id)

            key = attr["key"]
            if key not in attr_values_by_key or is_hier:
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

        ft_is_task =  new_ft_match.entity_type == "Task"
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

            entity = self.entity_hub.get_or_query_entity_by_id(
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
                        parent = self.entity_hub.get_or_query_entity_by_id(
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
            entity = self.entity_hub.get_or_query_entity_by_id(
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
            self.log.debug(f"Removing entity {entity.path}")
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

    def _process_task_hierarchy_changes(self, task_hierarchy_changes):
        # TODO finish task name and parent changes
        for ftrack_id, info in task_hierarchy_changes.items():
            entity_ids = self.task_ids_by_ftrack_id[ftrack_id]
            if len(entity_ids) != 1:
                continue
            entity = self.entity_hub.get_or_query_entity_by_id(
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
                    parent = self.entity_hub.get_or_query_entity_by_id(
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
                    "short_name": re.sub(r"\W+", "", task_type_name.lower())
                })

        project_entity.task_types = new_task_types

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

            if not new_type_name in task_types_names:
                project_need_update = True

            to_change.append((entity, new_type_name))

        if project_need_update:
            self._update_project_task_types()

        for entity, new_type_name in to_change:
            prev_task_type = entity.task_type
            entity.task_type = new_type_name
            self.log.debug(
                f"Changed task type {prev_task_type} -> {new_type_name}")

    def _propagate_attrib_changes(self):
        std_cust_attr = self.ft_std_cust_attrs
        hier_cust_attr = self.ft_hier_cust_attrs

        # Prepare all created ftrack ids
        # - in that case it is not needed to update attributes as they have
        #   set all attributes from ftrack
        created_ftrack_ids = set(self._created_entity_by_ftrack_id.keys())
        task_type_changes = {}
        for ftrack_id, info in self.entities_by_action["update"].items():
            if ftrack_id in created_ftrack_ids:
                continue

            object_type_id = None
            if info["entityType"] == "show":
                entity = self.entity_hub.project_entity

            elif info["entityType"] == "task":
                object_type_id = info["objectTypeId"]
                if info["entity_type"] == "Task":
                    entity_ids = self.task_ids_by_ftrack_id[ftrack_id]
                    entity_types = ["task"]
                else:
                    entity_ids = self.folder_ids_by_ftrack_id[ftrack_id]
                    entity_types = ["folder"]

                if len(entity_ids) != 1:
                    continue

                entity_id = entity_ids[0]
                entity = self.entity_hub.get_or_query_entity_by_id(
                    entity_id, entity_types)

            else:
                continue

            for key, change_info in info["changes"].items():
                value = change_info["new"]
                if key == "typeid" and entity.entity_type == "task":
                    task_type_changes[ftrack_id] = (entity, info)

                if key not in entity.attribs:
                    continue

                if value is not None:
                    if key in FPS_KEYS:
                        value = convert_to_fps(value)
                    else:
                        attr = hier_cust_attr.get(key)
                        if attr is None:
                            attr = std_cust_attr[object_type_id].get(key)
                            if attr is None:
                                continue
                        value = self._convert_value_by_cust_attr_conf(
                            value, attr)
                entity.attribs[key] = value

        self._propagate_task_type_changes(task_type_changes)

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

        server_id_attr = self.ft_hier_cust_attrs[CUST_ATTR_KEY_SERVER_ID]
        path_attr = self.ft_hier_cust_attrs[CUST_ATTR_KEY_SERVER_PATH]
        fail_attr = self.ft_hier_cust_attrs[CUST_ATTR_KEY_SYNC_FAIL]
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

    def process_event_data(self):
        # Check if auto-sync custom attribute exists
        entities_by_action = self.entities_by_action
        debug_action_map = {
            "add": "Created",
            "remove": "Removed",
            "update": "Updated"
        }
        debug_msg = "\n".join([
            f"- {debug_action_map[action]}: {len(entities_info)}"
            for action, entities_info in entities_by_action.items()
        ])

        self.log.debug(
            f"Project \"{self.project_name}\" changes\n{debug_msg}")

        # Get ftrack entities - find all ftrack ids first
        ftrack_ids = set()
        for action, _ftrack_ids in entities_by_action.items():
            # skip removed (not exist in ftrack)
            if action != "remove":
                ftrack_ids |= set(_ftrack_ids)

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
            # 3. Commit changes to server
            self.entity_hub.commit_changes()
            # 4. Propagate entity changes to ftack
            time_4 = time.time()
            self._propagate_ftrack_attributes()
            # TODO propagate entities to ftrack
            #  - server id, server path, sync failed
            time_5 = time.time()

            total_time = f"{time_5 - time_1:.2f}"
            mid_times = ", ".join([
                f"{diff:.2f}"
                for diff in (
                    time_2 - time_1,
                    time_3 - time_2,
                    time_4 - time_3,
                    time_5 - time_4,
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
            All task changes are handled together by refresh from Ftrack.
        Args:
            session (object): session to Ftrack
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
            self.process_session, event, self.log
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
                # Trigger sync to avalon action if auto sync was turned on
                selection = [{
                    "entityId": sync_process.ft_project_id,
                    "entityType": "show"
                }]
                # TODO uncomment when out of testing stage
                self.trigger_action(
                    action_identifier="sync.from.ftrack.server",
                    event=sync_process.event,
                    selection=selection
                )

        if not sync_process.is_event_valid:
            self.log.debug(
                f"Project has disabled autosync {sync_process.project_name}."
                " Skipping."
            )
            return True

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

    @property
    def duplicated_report(self):
        if not self.duplicated:
            return []

        ft_project = self.cur_project
        duplicated_names = []
        for ftrack_id in self.duplicated:
            ftrack_ent = self.ftrack_ents_by_id.get(ftrack_id)
            if not ftrack_ent:
                ftrack_ent = self.process_session.query(
                    self.entities_query_by_id.format(
                        ft_project["id"], ftrack_id
                    )
                ).one()
                self.ftrack_ents_by_id[ftrack_id] = ftrack_ent
            name = ftrack_ent["name"]
            if name not in duplicated_names:
                duplicated_names.append(name)

        joined_names = ", ".join(
            ["\"{}\"".format(name) for name in duplicated_names]
        )
        ft_ents = self.process_session.query(
            self.entities_name_query_by_name.format(
                ft_project["id"], joined_names
            )
        ).all()

        ft_ents_by_name = collections.defaultdict(list)
        for ft_ent in ft_ents:
            name = ft_ent["name"]
            ft_ents_by_name[name].append(ft_ent)

        if not ft_ents_by_name:
            return []

        subtitle = "Duplicated entity names:"
        items = []
        items.append({
            "type": "label",
            "value": "# {}".format(subtitle)
        })
        items.append({
            "type": "label",
            "value": (
                "<p><i>NOTE: It is not allowed to use the same name"
                " for multiple entities in the same project</i></p>"
            )
        })

        for name, ents in ft_ents_by_name.items():
            items.append({
                "type": "label",
                "value": "## {}".format(name)
            })
            paths = []
            for ent in ents:
                ftrack_id = ent["id"]
                ent_path = "/".join([_ent["name"] for _ent in ent["link"]])
                avalon_ent = self.avalon_ents_by_id.get(ftrack_id)

                if avalon_ent:
                    additional = " (synchronized)"
                    if avalon_ent["name"] != name:
                        additional = " (synchronized as {})".format(
                            avalon_ent["name"]
                        )
                    ent_path += additional
                paths.append(ent_path)

            items.append({
                "type": "label",
                "value": '<p>{}</p>'.format("<br>".join(paths))
            })

        return items

    @property
    def regex_report(self):
        if not self.regex_failed:
            return []

        subtitle = "Entity names contain prohibited symbols:"
        items = []
        items.append({
            "type": "label",
            "value": "# {}".format(subtitle)
        })
        items.append({
            "type": "label",
            "value": (
                "<p><i>NOTE: You can use Letters( a-Z ),"
                " Numbers( 0-9 ) and Underscore( _ )</i></p>"
            )
        })

        ft_project = self.cur_project
        for ftrack_id in self.regex_failed:
            ftrack_ent = self.ftrack_ents_by_id.get(ftrack_id)
            if not ftrack_ent:
                ftrack_ent = self.process_session.query(
                    self.entities_query_by_id.format(
                        ft_project["id"], ftrack_id
                    )
                ).one()
                self.ftrack_ents_by_id[ftrack_id] = ftrack_ent

            name = ftrack_ent["name"]
            ent_path_items = [_ent["name"] for _ent in ftrack_ent["link"][:-1]]
            ent_path_items.append("<strong>{}</strong>".format(name))
            ent_path = "/".join(ent_path_items)
            items.append({
                "type": "label",
                "value": "<p>{} - {}</p>".format(name, ent_path)
            })

        return items

    def report(self):
        msg_len = len(self.duplicated) + len(self.regex_failed)
        for msgs in self.report_items.values():
            msg_len += len(msgs)

        if msg_len == 0:
            return

        items = []
        project_name = self.cur_project["full_name"]
        title = "Synchronization report ({}):".format(project_name)

        keys = ["error", "warning", "info"]
        for key in keys:
            subitems = []
            if key == "warning":
                subitems.extend(self.duplicated_report)
                subitems.extend(self.regex_report)

            for _msg, _items in self.report_items[key].items():
                if not _items:
                    continue

                msg_items = _msg.split("||")
                msg = msg_items[0]
                subitems.append({
                    "type": "label",
                    "value": "# {}".format(msg)
                })

                if len(msg_items) > 1:
                    for note in msg_items[1:]:
                        subitems.append({
                            "type": "label",
                            "value": "<p><i>NOTE: {}</i></p>".format(note)
                        })

                if isinstance(_items, str):
                    _items = [_items]
                subitems.append({
                    "type": "label",
                    "value": '<p>{}</p>'.format("<br>".join(_items))
                })

            if items and subitems:
                items.append(self.report_splitter)

            items.extend(subitems)

        self.show_interface(
            items=items,
            title=title,
            event=self._cur_event
        )
        return True


def register(session):
    '''Register plugin. Called when used as an plugin.'''
    AutoSyncFromFtrack(session).register()
