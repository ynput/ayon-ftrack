import re
import collections
import time
import logging
import typing
from typing import Any, Dict

import arrow
from ayon_api import (
    get_project,
    create_project,
    slugify_string,
    get_addons_settings,
)
from ayon_api.entity_hub import EntityHub, BaseEntity
import ftrack_api
from ftrack_common import (
    CUST_ATTR_KEY_SERVER_ID,
    CUST_ATTR_KEY_SERVER_PATH,
    CUST_ATTR_KEY_SYNC_FAIL,
    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
    REMOVED_ID_VALUE,
    create_chunks,
    MappedAYONAttribute,
    CustomAttributesMapping,
    get_custom_attributes_mapping,
    get_custom_attributes_by_entity_id,
    map_ftrack_users_to_ayon_users,
    join_filter_values,
)

if typing.TYPE_CHECKING:
    import ftrack_api.entity.base.Entity


def _get_ftrack_project(session, project_name):
    ft_project = session.query((
        "select id, name, full_name from Project where full_name is \"{}\""
    ).format(project_name)).first()
    if ft_project is None:
        raise ValueError(
            f"Project \"{project_name}\" was not found in ftrack"
        )
    return ft_project


class IdsMapping(object):
    def __init__(self):
        self._ftrack_to_server = {}
        self._server_to_ftrack = {}

    def set_ftrack_to_server(self, ftrack_id, server_id):
        self._ftrack_to_server[ftrack_id] = server_id
        self._server_to_ftrack[server_id] = ftrack_id

    def set_server_to_ftrack(self, server_id, ftrack_id):
        self.set_ftrack_to_server(ftrack_id, server_id)

    def get_server_mapping(self, ftrack_id):
        return self._ftrack_to_server.get(ftrack_id)

    def get_ftrack_mapping(self, server_id):
        return self._server_to_ftrack.get(server_id)


class SyncFromFtrack:
    """Helper for sync project from ftrack."""

    def __init__(self, session, project_name, log=None):
        self._log = log
        self._ft_session = session
        self._project_name = project_name
        self._ids_mapping = IdsMapping()

        ft_users = session.query("select id, username, email from User").all()
        users_mapping = map_ftrack_users_to_ayon_users(ft_users)
        for ftrack_id, ayon_id in users_mapping.items():
            if ayon_id:
                self._ids_mapping.set_ftrack_to_server(ftrack_id, ayon_id)

        # Create entity hub which handle entity changes
        self._entity_hub = EntityHub(project_name)
        self._project_settings = get_addons_settings(
            project_name=project_name
        )

        self._report_items = []

        # TODO add more and use them
        self._processed_server_ids = set()
        self._processed_ftrack_ids = set()
        self._skipped_ftrack_ids = set()
        self._duplicated_ftrack_ids = {}
        # Immutable
        self._im_invalid_entity_ids = set()
        self._im_renamed_entity_ids = set()
        self._im_moved_entity_ids = set()
        self._im_removed_entity_ids = set()

    @property
    def project_name(self):
        """Name of project which is synchronized.

        Returns:
            str: Project name which is synchronized.
        """

        return self._project_name

    @property
    def log(self):
        """Logger object.

        Returns:
            logging.Logger: Logger object.
        """

        if self._log is None:
            self._log = logging.getLogger(self.__class__.__name__)
        return self._log

    @property
    def report_items(self):
        """Report items shown once finished.

        Returns:
            list[dict[str, Any]]: List of interface items for ftrack UI.
        """

        return self._report_items

    def sync_project_types(self, ft_project, ft_session):
        """Sync project types from ftrack to AYON.

        Args:
            ft_project (ftrack_api.entity.Entity): ftrack project entity.
            ft_session (ftrack_api.Session): ftrack session.

        Returns:
            tuple[list, list]: Tuple of object types and task types.
        """

        self._entity_hub.fill_project_from_server()
        # Get Folder types and Task types from ftrack
        ft_object_types = ft_session.query(
            "select id, name, sort from ObjectType").all()
        ft_object_types_by_id = {
            ft_object_type["id"]: ft_object_type
            for ft_object_type in ft_object_types
        }

        ft_types = ft_session.query("select id, name, sort from Type").all()
        ft_types_by_id = {
            ft_type["id"]: ft_type
            for ft_type in ft_types
        }

        # Filter folder and task types for this project based on schema
        project_schema = ft_project["project_schema"]
        object_types = {
            ft_object_types_by_id[object_type["id"]]
            for object_type in project_schema["object_types"]
        }
        task_types = {
            ft_types_by_id[task_type["id"]]
            for task_type in project_schema["task_type_schema"]["types"]
        }

        # Update types on project entity from ftrack
        self.update_project_types(object_types, task_types)
        return object_types, task_types

    def sync_statuses(self, ft_project, ft_session):
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
        joined_schema_ids = join_filter_values(schema_ids)
        object_type_schemas = ft_session.query(
            "select id, object_type_id from Schema"
            f" where id in ({joined_schema_ids})"
        ).all()

        object_type_schema_ids = {
            schema["id"]
            for schema in object_type_schemas
        }
        joined_ot_schema_ids = join_filter_values(object_type_schema_ids)
        schema_statuses = ft_session.query(
            "select status_id from SchemaStatus"
            f" where schema_id in ({joined_ot_schema_ids})"
        ).all()
        folder_statuse_ids = {
            status["status_id"]
            for status in schema_statuses
        }

        # Task statues
        task_workflow_override_ids = {
            task_override["id"]
            for task_override in project_schema["task_workflow_schema_overrides"]
        }
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
            folder_statuse_ids
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
            if status_id in folder_statuse_ids:
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

    def _get_available_ft_statuses(
        self,
        ft_entity: "ftrack_api.entity.base.Entity",
        project_schema_id: str,
    ) :
        fields = {
            "asset_version_workflow_schema",
            "task_workflow_schema",
            "task_workflow_schema_overrides",
            "object_type_schemas",
        }

        joined_fields = ", ".join(fields)
        project_schema = self._session.query(
            f"select {joined_fields} from ProjectSchema"
            f" where id is '{project_schema_id}'"
        ).first()


        type_id = ft_entity["type_id"]
        task_workflow_override_ids = {
            task_override["id"]
            for task_override in
            project_schema["task_workflow_schema_overrides"]
        }
        joined_ids = join_filter_values(task_workflow_override_ids)
        overrides_schema = self._session.query(
            "select workflow_schema_id"
            f" from ProjectSchemaOverride"
            f" where id in ({joined_ids}) and type_id is '{type_id}'"
        ).first()
        workflow_id = project_schema["task_workflow_schema"]["id"]
        if overrides_schema is not None:
            workflow_id = overrides_schema["workflow_schema_id"]
        workflow_statuses = self._session.query(
            "select status_id"
            " from WorkflowSchemaStatus"
            f" where workflow_schema_id is '{workflow_id}'"
        ).all()
        return {
            item["status_id"]
            for item in workflow_statuses
        }

    def project_exists_in_ayon(self):
        """Does project exists on AYON server by name.

        Returns:
            bool: Project exists in AYON.
        """

        # Make sure project exists on server
        project = get_project(self.project_name)
        if not project:
            return False
        return True

    def create_project(self, preset_name, attributes):
        """Create project on AYON server.

        Args:
            preset_name (str): Name of anatomy preset that will be used.
            attributes (dict[str, Any]): Attributes for project creation.
        """

        project_name = self.project_name
        if self.project_exists_in_ayon():
            return

        ft_session = self._ft_session
        ft_project = _get_ftrack_project(ft_session, project_name)
        self.log.info(f"Creating project \"{project_name}\" on server")
        project_code = ft_project["name"]
        create_project(
            project_name,
            project_code,
            preset_name=preset_name
        )
        self.log.info(f"Project \"{project_name}\" created on server")

        self.sync_project_types(ft_project, ft_session)
        self.sync_statuses(ft_project, ft_session)
        project_entity = self._entity_hub.project_entity
        for key, value in attributes.items():
            project_entity.attribs[key] = value
        self._entity_hub.commit_changes()

    def sync_to_server(self):
        """Sync project with hierarchy from ftrack to AYON server."""

        t_start = time.perf_counter()
        project_name = self.project_name
        # Make sure project exists on server
        if not self.project_exists_in_ayon():
            self.log.info(
                f"Project \"{project_name}\" does not exist on server."
                " Skipping project synchronization."
            )
            self._report_items.extend([
                {
                    "type": "label",
                    "value": (
                        f"## Project '{project_name}' does not exist in AYON"
                    )
                },
                {
                    "type": "label",
                    "value": (
                        "Synchronization was skipped."
                        "<br/>Run Prepare Project action or create the"
                        " project manually on server and then run the"
                        " action again."
                    )
                }
            ])
            return

        ft_session = self._ft_session

        self.log.info(f"Synchronization of project \"{project_name}\" started")

        # Get ftrack custom attributes to sync
        attr_confs = ft_session.query(
            "select id, key, is_hierarchical, default"
            " from CustomAttributeConfiguration"
        ).all()

        # Check if there is custom attribute to store server id
        server_id_conf_id = None
        server_path_conf_id = None
        sync_failed_conf_id = None
        for attr_conf in attr_confs:
            if attr_conf["key"] == CUST_ATTR_KEY_SERVER_ID:
                server_id_conf_id = attr_conf["id"]
            elif attr_conf["key"] == CUST_ATTR_KEY_SERVER_PATH:
                server_path_conf_id = attr_conf["id"]
            elif attr_conf["key"] == CUST_ATTR_KEY_SYNC_FAIL:
                sync_failed_conf_id = attr_conf["id"]

        missing_attrs = []
        if not server_id_conf_id:
            missing_attrs.append(CUST_ATTR_KEY_SERVER_ID)

        if not server_path_conf_id:
            missing_attrs.append(CUST_ATTR_KEY_SERVER_PATH)

        if not sync_failed_conf_id:
            missing_attrs.append(CUST_ATTR_KEY_SYNC_FAIL)

        if missing_attrs:
            attr_end = ""
            was_were = "was"
            if len(missing_attrs) > 1:
                attr_end = "s"
                was_were = "were"
            joined_attrs = ", ".join([f'"{attr}"'for attr in missing_attrs])
            msg = (
                f"Hierarchical attribute{attr_end} {joined_attrs}"
                f" {was_were} not found in ftrack"
            )

            self.log.warning(msg)
            raise ValueError(msg)

        # Query ftrack project
        ft_project = _get_ftrack_project(ft_session, project_name)

        t_project_existence_1 = time.perf_counter()
        self.log.debug(
            f"Initial preparation took {t_project_existence_1 - t_start}"
        )
        self.log.debug("Loading entities from server")
        # Query entities from server (project, folders and tasks)
        self._entity_hub.query_entities_from_server()
        self._ids_mapping.set_ftrack_to_server(
            ft_project["id"], self._entity_hub.project_entity.id
        )
        t_server_query_2 = time.perf_counter()
        self.log.debug((
            "Loading of entities from server"
            f" took {t_server_query_2 - t_project_existence_1}"
        ))

        self.log.info("Querying necessary data from ftrack")

        object_types, task_types = self.sync_project_types(
            ft_project, ft_session
        )
        ft_object_type_name_by_id = {
            object_type["id"]: object_type["name"]
            for object_type in object_types
        }
        ft_type_names_by_id = {
            task_type["id"]: task_type["name"]
            for task_type in task_types
        }

        t_types_sync_3 = time.perf_counter()
        self.log.debug((
            "Update of types from ftrack"
            f" took {t_types_sync_3 - t_server_query_2}"
        ))

        self.log.info("Querying project hierarchy from ftrack")
        ft_entities = ft_session.query((
            "select id, name, parent_id, type_id, object_type_id, status_id"
            ", start_date, end_date, description, status_id"
            " from TypedContext where project_id is \"{}\""
        ).format(ft_project["id"])).all()
        t_ft_entities_4 = time.perf_counter()
        self.log.debug((
            f"Query of ftrack entities took {t_ft_entities_4 - t_types_sync_3}"
        ))

        ft_entities_by_id = {ft_project["id"]: ft_project}
        ft_entities_by_parent_id = collections.defaultdict(list)
        for entity in ft_entities:
            entity_id = entity["id"]
            parent_id = entity["parent_id"]
            ft_entities_by_id[entity_id] = entity
            ft_entities_by_parent_id[parent_id].append(entity)

        cust_attr_value_by_entity_id = self._prepare_attribute_values(
            ft_session,
            attr_confs,
            ft_entities_by_id,
        )

        self.log.info("Checking changes of immutable entities")
        self.match_immutable_entities(
            ft_project,
            ft_entities_by_id,
            ft_entities_by_parent_id,
        )

        self.log.info("Matching ftrack to server hierarchy")
        self.match_existing_entities(
            ft_project,
            ft_entities_by_parent_id,
            ft_object_type_name_by_id,
            ft_type_names_by_id,
            cust_attr_value_by_entity_id
        )

        self.log.info("Updating assignees")
        self.update_assignees_from_ftrack(
            ft_entities_by_id
        )

        self.log.info("Updating attributes of entities")
        self.update_attributes_from_ftrack(
            cust_attr_value_by_entity_id,
            ft_entities_by_id
        )
        self._entity_hub.commit_changes()

        self.log.info("Updating server ids on ftrack entities")
        self.update_ftrack_attributes(
            ft_entities_by_id,
            cust_attr_value_by_entity_id,
            server_id_conf_id,
            server_path_conf_id,
            sync_failed_conf_id
        )
        self.create_report(ft_entities_by_id)
        t_end = time.perf_counter()
        self.log.info((
            f"Synchronization of project \"{project_name}\" finished"
            f" in {t_end-t_start}"
        ))

    def update_project_types(self, object_types, task_types):
        project_entity = self._entity_hub.project_entity
        ignored_folder_types = {"task", "milestone"}
        src_folder_types = {
            folder_type["name"]: folder_type
            for folder_type in project_entity.folder_types
            if folder_type["name"].lower() not in ignored_folder_types
        }
        src_task_types = {
            task_type["name"]: task_type
            for task_type in project_entity.task_types
        }

        new_folder_types = []
        for object_type in sorted(object_types, key=lambda o: o["sort"]):
            name = object_type["name"]
            src_folder_type = src_folder_types.get(name)
            if src_folder_type is not None:
                new_folder_types.append(src_folder_type)
            else:
                new_folder_types.append({"name": name})

        new_task_types = []
        for task_type in task_types:
            name = task_type["name"]
            src_task_type = src_task_types.get(name)
            if src_task_type is not None:
                new_task_types.append(src_task_type)
            else:
                new_task_types.append({
                    "name": name,
                    "shortName": re.sub(r"\W+", "", name.lower())
                })

        project_entity.folder_types = new_folder_types
        project_entity.task_types = new_task_types

    def match_immutable_entities(
        self,
        ft_project,
        ft_entities_by_id,
        ft_entities_by_parent_id,
    ):
        self.log.debug("Validation of immutable entities started")

        # Collect all ftrack ids from immuable entities
        immutable_queue = collections.deque()
        for entity in self._entity_hub.project_entity.children:
            if entity.immutable_for_hierarchy:
                immutable_queue.append(entity)

        all_immutable_ftrack_ids = set()
        while immutable_queue:
            entity = immutable_queue.popleft()
            all_immutable_ftrack_ids.add(entity.attribs[FTRACK_ID_ATTRIB])
            for child in entity.children:
                immutable_queue.append(child)

        # Go through entities and find matching ftrack entity id
        hierarchy_queue = collections.deque()
        for entity in self._entity_hub.project_entity.children:
            if entity.immutable_for_hierarchy:
                hierarchy_queue.append((entity, ft_project["id"]))

        while hierarchy_queue:
            (entity, ft_parent_id) = hierarchy_queue.popleft()

            expected_ftrack_id = entity.attribs[FTRACK_ID_ATTRIB]
            ft_entity = ft_entities_by_id.get(expected_ftrack_id)
            if ft_entity is None:
                ft_children = []
                if ft_parent_id is not None:
                    ft_children = ft_entities_by_parent_id[ft_parent_id]

                is_folder = entity.entity_type == "folder"
                for ft_child in ft_children:
                    # Skip all entities that are already reserved for other
                    #   entities
                    if ft_child["id"] in all_immutable_ftrack_ids:
                        continue
                    name = slugify_string(ft_child["name"])
                    if name != entity.name:
                        continue
                    ft_is_folder = ft_child.entity_type != "Task"
                    if is_folder is ft_is_folder:
                        ft_entity = ft_child
                        break

                if ft_entity is None:
                    # Make sure 'expected_ftrack_id' is None
                    expected_ftrack_id = None
                    # Set ftrack id on entity to removed
                    entity.attribs[FTRACK_ID_ATTRIB] = REMOVED_ID_VALUE
                else:
                    # Change ftrack id of entity to matching ftrack entity
                    expected_ftrack_id = ft_entity["id"]
                    entity.attribs[FTRACK_ID_ATTRIB] = expected_ftrack_id
                    # Add the ftrack id to immutable ids
                    all_immutable_ftrack_ids.add(expected_ftrack_id)

            else:
                valid = True
                ft_name = slugify_string(ft_entity["name"])
                if ft_name != entity.name:
                    self._im_renamed_entity_ids.add(entity.id)
                    valid = False

                if ft_entity["parent_id"] != ft_parent_id:
                    self._im_moved_entity_ids.add(entity.id)
                    valid = False

                if not valid:
                    self._im_invalid_entity_ids.add(entity.id)

            if expected_ftrack_id:
                self._processed_ftrack_ids.add(expected_ftrack_id)
                self._ids_mapping.set_server_to_ftrack(
                    entity.id, expected_ftrack_id)

            self._processed_server_ids.add(entity.id)
            for child in entity.children:
                if child.immutable_for_hierarchy:
                    hierarchy_queue.append((child, expected_ftrack_id))

    def _create_new_entity(
        self,
        parent_entity,
        ft_entity,
        ft_object_type_name_by_id,
        ft_type_names_by_id: Dict[str, str],
        cust_attr_value_by_entity_id: Dict[str, Dict[str, Any]],
    ):
        ftrack_id = ft_entity["id"]
        custom_attributes = cust_attr_value_by_entity_id[ftrack_id]
        entity_id = custom_attributes.get(CUST_ATTR_KEY_SERVER_ID)

        label = ft_entity["name"]
        name = slugify_string(label)
        entity_type = ft_entity.entity_type
        if entity_type.lower() == "task":
            task_type_name = ft_type_names_by_id[ft_entity["type_id"]]
            new_entity = self._entity_hub.add_new_task(
                task_type=task_type_name,
                name=name,
                label=label,
                entity_id=entity_id,
                parent_id=parent_entity.id
            )

        else:
            object_type = ft_object_type_name_by_id[
                ft_entity["object_type_id"]]
            new_entity = self._entity_hub.add_new_folder(
                folder_type=object_type,
                name=name,
                label=label,
                entity_id=entity_id,
                parent_id=parent_entity.id
            )
        self._ids_mapping.set_ftrack_to_server(ftrack_id, new_entity.id)

        return new_entity

    def match_existing_entities(
        self,
        ft_project,
        ft_entities_by_parent_id,
        ft_object_type_name_by_id,
        ft_type_names_by_id: Dict[str, str],
        cust_attr_value_by_entity_id: Dict[str, Dict[str, Any]],
    ):
        """Match exiting entities on both sides.

        Create new entities that are on ftrack and are not on server and remove
        those which are not on ftrack.

        Todos:
            Handle duplicates more clearly. Don't compare children only by name
                but also by type (right now task == folder).

        Args:
            ft_project (ftrack_api.Entity): ftrack project entity.
            ft_entities_by_parent_id (dict[str, list[ftrack_api.Entity]]): Map
                of ftrack entities by their parent ids.
            ft_object_type_name_by_id (Dict[str, str]): Mapping of ftrack
                object type ids to their names.
            ft_type_names_by_id (Dict[str, str]): Mapping of ftrack task type
                ids to their names.
            cust_attr_value_by_entity_id (Dict[str, Dict[str, Any]): Custom
                attribute values by key stored by entity id.
        """

        fill_queue = collections.deque()
        for ft_child in ft_entities_by_parent_id[ft_project["id"]]:
            fill_queue.append((self._entity_hub.project_entity, ft_child))

        def _add_children_to_queue(ft_entity_id):
            children = ft_entities_by_parent_id[ft_entity_id]
            if not children:
                return

            entity_id = self._ids_mapping.get_server_mapping(ft_entity_id)
            entity = None
            if entity_id:
                entity = self._entity_hub.get_entity_by_id(entity_id)

            for ft_child in children:
                fill_queue.append((entity, ft_child))

        while fill_queue:
            (parent_entity, ft_entity) = fill_queue.popleft()
            ft_entity_path = "/".join([
                item["name"]
                for item in ft_entity["link"]
            ])
            ft_entity_id = ft_entity["id"]
            # Go to next children if is already processed
            if ft_entity_id in self._processed_ftrack_ids:
                _add_children_to_queue(ft_entity_id)
                self.log.debug(
                    f"{ft_entity_path} - ftrack id already processed")
                continue

            if parent_entity is None:
                self._skipped_ftrack_ids.add(ft_entity_id)
                _add_children_to_queue(ft_entity_id)
                self.log.debug(f"{ft_entity_path} - Skipped")
                continue

            label = ft_entity["name"]
            name = slugify_string(label)
            matching_name_entity = None
            for child in parent_entity.children:
                if child.name.lower() == name.lower():
                    matching_name_entity = child
                    break

            ft_is_folder = ft_entity.entity_type != "Task"
            if matching_name_entity is not None:
                # If entity was already processed we can skip ftrack entity
                # --- This is last condition that handle immutable entities ---
                #   After this condition can be server entities changed,
                #       removed or created.
                if matching_name_entity.id in self._processed_server_ids:
                    self._processed_ftrack_ids.add(ft_entity_id)
                    self._duplicated_ftrack_ids[ft_entity_id] = matching_name_entity
                    _add_children_to_queue(ft_entity_id)
                    self.log.debug(
                        f"{ft_entity_path} - Server id already processed")
                    continue

                is_folder = matching_name_entity.entity_type == "folder"
                # It is possible to remove previous server entity at this point
                #   as we're 100% sure it is not immutable at this point
                if ft_is_folder is not is_folder:
                    self.log.debug(
                        f"{ft_entity_path} - Deleted previous entity")
                    # Remove current entity if type does not match
                    matching_name_entity.parent_id = None
                    # Reset variable so new entity is created
                    matching_name_entity = None

            # No match was found, so we can create new server entity
            if matching_name_entity is None:
                self.log.debug(f"{ft_entity_path} - Creating new entity")
                entity = self._create_new_entity(
                    parent_entity,
                    ft_entity,
                    ft_object_type_name_by_id,
                    ft_type_names_by_id,
                    cust_attr_value_by_entity_id,
                )
                self._processed_server_ids.add(entity.id)
                self._processed_ftrack_ids.add(ft_entity_id)
                _add_children_to_queue(ft_entity_id)
                continue

            self.log.debug(f"{ft_entity_path} - Updating existing entity")
            matching_name_entity.name = name
            matching_name_entity.label = label
            matching_name_entity.active = True
            if matching_name_entity.entity_type == "task":
                task_type_id = ft_entity["type_id"]
                task_type_name = ft_type_names_by_id[task_type_id]
                if matching_name_entity.task_type != task_type_name:
                    matching_name_entity.task_type = task_type_name

            else:
                object_type_id = ft_entity["object_type_id"]
                object_type_name = ft_object_type_name_by_id[
                    object_type_id]
                if matching_name_entity.folder_type != object_type_name:
                    matching_name_entity.folder_type = object_type_name

            self._processed_server_ids.add(matching_name_entity.id)
            self._processed_ftrack_ids.add(ft_entity_id)
            self._ids_mapping.set_ftrack_to_server(
                ft_entity_id, matching_name_entity.id
            )
            _add_children_to_queue(ft_entity_id)

        deactivate_queue = collections.deque()
        for child in self._entity_hub.project_entity.children:
            deactivate_queue.append(child)

        while deactivate_queue:
            entity = deactivate_queue.popleft()
            if entity.id not in self._processed_server_ids:
                entity.active = False

            for child in entity.children:
                deactivate_queue.append(child)

    def _set_entity_status(
        self,
        ft_entity: "ftrack_api.entity.base.Entity",
        entity: BaseEntity,
        ftrack_statuses: Dict[str, str],
        ayon_statuses: Dict[str, Any],
    ):
        # QUESTION should we log all invalid/missing statuses?
        # QUESTION should we update AYON project statuses if status
        #   is not available?
        if entity.entity_type not in ("folder", "task"):
            return

        ft_status_name = ftrack_statuses.get(ft_entity.get("status_id"))
        if ft_status_name is None:
            return

        ayon_status = ayon_statuses.get(ft_status_name.lower())
        if ayon_status is None:
            return

        scope = ayon_status.scope
        if entity.entity_type in scope:
            entity.set_status(ayon_status["name"])

    def update_assignees_from_ftrack(self, ft_entities_by_id):
        task_entities_by_id = {}
        for entity in ft_entities_by_id.values():
            if entity.entity_type == "Task":
                task_id = entity["id"]
                ayon_id = self._ids_mapping.get_server_mapping(task_id)
                if ayon_id is not None:
                    task_entities_by_id[task_id] = entity

        if not task_entities_by_id:
            return

        assignment_by_task_id = {
            task_id: set()
            for task_id in task_entities_by_id
        }
        task_ids = list(task_entities_by_id.keys())
        for task_ids_chunk in create_chunks(task_ids, 50):
            joined_ids = ",".join([
                f'"{task_id}"'
                for task_id in task_ids_chunk
            ])
            appointments = self._ft_session.query(
                f"select resource_id, context_id from Appointment"
                f" where context_id in ({joined_ids})"
                f" and type is 'assignment'"
            ).all()
            for appointment in appointments:
                task_id = appointment["context_id"]
                user_id = appointment["resource_id"]
                assignment_by_task_id[task_id].add(user_id)

        for task_id, user_ids in assignment_by_task_id.items():
            ayon_task = self._entity_hub.get_entity_by_id(
                self._ids_mapping.get_server_mapping(task_id)
            )
            if ayon_task is None:
                continue

            new_assignees = set()
            # Keep users that don't have ftrack mapping on task
            for ayon_user in ayon_task.assignees:
                user_id = self._ids_mapping.get_ftrack_mapping(ayon_user)
                if user_id is None:
                    new_assignees.add(ayon_user)

            for user_id in user_ids:
                ayon_user = self._ids_mapping.get_server_mapping(user_id)
                if ayon_user:
                    new_assignees.add(ayon_user)

            ayon_task.assignees = list(new_assignees)

    def update_attributes_from_ftrack(
        self,
        cust_attr_value_by_entity_id: Dict[str, Dict[str, Any]],
        ft_entities_by_id: Dict[str, "ftrack_api.entity.base.Entity"]
    ):
        ftrack_statuses = {
            status["id"]: status["name"]
            for status in self._ft_session.query(
                "select id, name from Status"
            ).all()
        }
        ayon_statuses = {
            status["name"].lower(): status
            for status in self._entity_hub.project_entity["statuses"]
        }
        hierarchy_queue = collections.deque()
        hierarchy_queue.append(self._entity_hub.project_entity)
        while hierarchy_queue:
            entity = hierarchy_queue.popleft()
            # Add children to queue
            for child_entity in entity.children:
                hierarchy_queue.append(child_entity)

            ftrack_id = self._ids_mapping.get_ftrack_mapping(entity.id)
            if ftrack_id is None:
                continue

            ft_entity = ft_entities_by_id[ftrack_id]
            path = "/".join([
                item["name"]
                for item in ft_entity["link"]
                if item["type"] != "Project"
            ])
            entity.attribs[FTRACK_ID_ATTRIB] = ftrack_id
            entity.attribs[FTRACK_PATH_ATTRIB] = path

            self._set_entity_status(
                ft_entity, entity, ftrack_statuses, ayon_statuses
            )

            for attr_name, value in (
                ("startDate", ft_entity["start_date"]),
                ("endDate", ft_entity["end_date"]),
                ("description", ft_entity.get("description")),
            ):
                if value is None or attr_name not in entity.attribs:
                    continue

                if isinstance(value, arrow.Arrow):
                    # Shift date to 00:00:00 of the day
                    # - ftrack is returning e.g. '2024-10-29T22:00:00'
                    #  for '2024-10-30'
                    value = str(value.shift(hours=24 - value.hour))

                entity.attribs[attr_name] = str(value)

            # ftrack id can not be available if ftrack entity was recreated
            #   during immutable entity processing
            attribute_values = cust_attr_value_by_entity_id[ftrack_id]
            is_project = entity.entity_type == "project"

            # TODO handle "data" to sync custom attributes not available
            #   in 'attribs'
            for key, value in attribute_values.items():
                # QUESTION Should we skip "unsetting" of project attributes?
                #   - very dangerous for AYON and maybe for project should
                #       be taken default value of attribute (if there is any)
                if is_project and value is None:
                    continue

                if key in entity.attribs:
                    entity.attribs[key] = value

    def _prepare_attribute_values(
        self, ft_session, attr_confs, ft_entities_by_id
    ):
        ft_entity_ids = set(ft_entities_by_id.keys())
        attr_mapping: CustomAttributesMapping = (
            get_custom_attributes_mapping(
                ft_session,
                self._project_settings["ftrack"],
                attr_confs,
            )
        )
        default_attrs = {}
        for attr_conf in attr_confs:
            if attr_conf["key"] in (
                CUST_ATTR_KEY_SERVER_ID,
                CUST_ATTR_KEY_SERVER_PATH,
                CUST_ATTR_KEY_SYNC_FAIL,
            ):
                default_attrs[attr_conf["id"]] = attr_conf["key"]

        mapped_confs_by_id = {}
        for mapping_item in attr_mapping.values():
            for mapped_conf in mapping_item.attr_confs:
                mapped_confs_by_id[mapped_conf["id"]] = mapped_conf

        val_by_entity_id = get_custom_attributes_by_entity_id(
            ft_session,
            ft_entity_ids,
            list(mapped_confs_by_id.values()),
            store_by_key=False,
        )

        cust_attr_value_by_entity_id = collections.defaultdict(dict)
        for entity_id, entity in ft_entities_by_id.items():
            values_by_attr_id = val_by_entity_id[entity_id]
            values_by_key = {}
            for attr_id, default_key in default_attrs.items():
                value = values_by_attr_id.get(attr_id)
                if value is None:
                    values_by_key[default_key] = None

            for ayon_attr_name, mapping_item in attr_mapping.items():
                attr_conf = mapping_item.get_attr_conf_for_entity(entity)
                if attr_conf is None:
                    continue

                value = values_by_attr_id.get(attr_conf["id"])
                if value is not None:
                    values_by_key[ayon_attr_name] = value

            cust_attr_value_by_entity_id[entity_id] = values_by_key
        return cust_attr_value_by_entity_id

    def _create_ft_operation(
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
            old_value,
            new_value
        )

    def update_ftrack_attributes(
        self,
        ft_entities_by_id,
        cust_attr_value_by_entity_id,
        server_id_conf_id,
        server_path_conf_id,
        sync_failed_conf_id
    ):
        operations = []
        for ftrack_id, ft_entity in ft_entities_by_id.items():
            if ft_entity.entity_type == "Project":
                continue

            server_id = self._ids_mapping.get_server_mapping(ftrack_id)
            entity = None
            if server_id:
                entity = self._entity_hub.get_entity_by_id(server_id)

            entity_id = ""
            if entity is not None:
                entity_id = entity.id

            custom_attributes = cust_attr_value_by_entity_id[ftrack_id]

            oring_sync_failed = custom_attributes.get(CUST_ATTR_KEY_SYNC_FAIL)
            orig_id = custom_attributes.get(CUST_ATTR_KEY_SERVER_ID)
            orig_path = custom_attributes.get(CUST_ATTR_KEY_SERVER_PATH)
            sync_failed = False
            if entity is None or entity.id in self._im_invalid_entity_ids:
                sync_failed = True

            if sync_failed != oring_sync_failed:
                operations.append(
                    self._create_ft_operation(
                        sync_failed_conf_id,
                        ftrack_id,
                        CUST_ATTR_KEY_SYNC_FAIL not in custom_attributes,
                        sync_failed,
                        oring_sync_failed
                    )
                )

            if orig_id != entity_id:
                operations.append(
                    self._create_ft_operation(
                        server_id_conf_id,
                        ftrack_id,
                        CUST_ATTR_KEY_SERVER_ID not in custom_attributes,
                        entity_id,
                        orig_id
                    )
                )

            if ft_entity.entity_type == "Task" or sync_failed:
                continue

            path = entity.path
            if path != orig_path:
                operations.append(
                    self._create_ft_operation(
                        server_path_conf_id,
                        ftrack_id,
                        CUST_ATTR_KEY_SERVER_PATH not in custom_attributes,
                        path,
                        orig_path
                    )
                )

        if not operations:
            return

        for chunk in create_chunks(operations, 500):
            for operation in chunk:
                self._ft_session.recorded_operations.push(operation)
            self._ft_session.commit()

    def create_report(self, ft_entities_by_id):
        report_items = []

        # --- Immutable entities ---
        # Removed entities - they don't have ftrack euqivalent anymore
        deleted_paths = []
        for entity_id in self._im_removed_entity_ids:
            entity = self._entity_hub.get_entity_by_id(entity_id)
            path = entity.attribs[FTRACK_PATH_ATTRIB]
            if not path:
                path = entity.path
            deleted_paths.append(path)

        deleted_paths.sort()
        if deleted_paths:
            report_items.append({
                "type": "label",
                "value": "## Not found entities"
            })
            for path in deleted_paths:
                self.log.info((
                    f"Skipped sync of immutable entity {path} (was removed)"
                ))
                report_items.append({"type": "label", "value": f"- {path}"})

        # Changed position or name
        renamed_mapping = {}
        changed_hierarchy = (
            self._im_renamed_entity_ids | self._im_moved_entity_ids
        )
        for entity_id in changed_hierarchy:
            entity = self._entity_hub.get_entity_by_id(entity_id)
            ftrack_id = entity.attribs[FTRACK_ID_ATTRIB]
            ft_entity = ft_entities_by_id.get(ftrack_id)
            if ft_entity is None:
                continue
            path = "/".join([
                item["name"]
                for item in ft_entity["link"]
                if item["type"] != "Project"
            ])
            expected_path = entity.attribs[FTRACK_PATH_ATTRIB]
            if not expected_path:
                expected_path = entity.path
            renamed_mapping[path] = expected_path

        renamed_paths = []
        for path in sorted(renamed_mapping.keys()):
            renamed_paths.append((path, renamed_mapping[path]))

        if renamed_paths:
            if report_items:
                report_items.append({"type": "label", "value": "---"})
            report_items.append({
                "type": "label",
                "value": "## Renamed/Moved entities"
            })
            report_items.append({
                "type": "label",
                "value": (
                    "Entities were renamed or moved to different location"
                    " but it is not allowed to propagate the change."
                )
            })
            for (path, expected_path) in renamed_paths:
                self.log.info((
                    "Skipped sync of immutable"
                    f" entity {path} -> {expected_path}"
                ))
                report_items.append({
                    "type": "label",
                    "value": f"- {path} -> {expected_path}"
                })

        # --- Other possible issues ---
        synced_path_mapping = collections.defaultdict(list)
        for ftrack_id, entity in self._duplicated_ftrack_ids.items():
            synced_path = entity.attribs[FTRACK_PATH_ATTRIB]
            ft_entity = ft_entities_by_id.get(ftrack_id)
            if ft_entity is not None:
                path = "/".join([
                    item["name"]
                    for item in ft_entity["link"]
                    if item["type"] != "Project"
                ])
                synced_path_mapping[synced_path].append(path)

        if synced_path_mapping:
            if report_items:
                report_items.append({"type": "label", "value": "---"})

            report_items.append({
                "type": "label",
                "value": "## Duplicated names"
            })
            for synced_path in sorted(synced_path_mapping.keys()):
                paths = synced_path_mapping[synced_path]
                self.log.info((
                    "Skipped sync because duplicated names."
                    "\nSource entity: {}\n{}"
                ).format(
                    synced_path,
                    "\n".join([f"- {path}" for path in paths])
                ))
                report_items.append({
                    "type": "label",
                    "value": f"### {synced_path}"
                })
                for path in paths:
                    report_items.append({
                        "type": "label",
                        "value": f"- {path}"
                    })

        self._report_items = report_items
