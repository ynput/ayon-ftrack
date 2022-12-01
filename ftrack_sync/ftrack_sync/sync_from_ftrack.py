import re
import collections
import itertools
import time
import logging

from openpype_api import (
    get_project,
    create_project,
)
import ftrack_api

from .entity_hub import EntityHub

FTRACK_ID_ATTRIB = "ftrackId"
FTRACK_ATTR_KEY_SERVER_ID = "openpype_id"


def join_filter_values(values):
    return ",".join({
        '"{}"'.format(value)
        for value in values
    })


def create_chunks(iterable, chunk_size=None):
    """Separate iterable into multiple chunks by size.

    Args:
        iterable (Iterable[Any]): Object that will be separated into chunks.
        chunk_size (int): Size of one chunk. Default value is 200.

    Returns:
        List[List[Any]]: Chunked items.
    """

    chunks = []
    tupled_iterable = tuple(iterable)
    if not tupled_iterable:
        return chunks
    iterable_size = len(tupled_iterable)
    if chunk_size is None:
        chunk_size = 200

    if chunk_size < 1:
        chunk_size = 1

    for idx in range(0, iterable_size, chunk_size):
        chunks.append(tupled_iterable[idx:idx + chunk_size])
    return chunks


def query_custom_attribute_values(session, attr_ids, entity_ids):
    """Query custom attribute values from ftrack database.

    Using ftrack call method result may differ based on used table name and
    version of ftrack server.

    For hierarchical attributes you shou always use `only_set_values=True`
    otherwise result will be default value of custom attribute and it would not
    be possible to differentiate if value is set on entity or default value is
    used.

    Args:
        session (ftrack_api.Session): Connected ftrack session.
        attr_ids (Iterable[str]): Attribute configuration ids.
        entity_ids (Iterable[str]): Entity ids for which are values queried.

    Returns:
        List[Dict[str, Any]]: Results from server.
    """

    output = []
    # Just skip
    attr_ids = set(attr_ids)
    entity_ids = set(entity_ids)
    if not attr_ids or not entity_ids:
        return output

    # Prepare values to query
    attributes_joined = join_filter_values(attr_ids)

    # Query values in chunks
    chunk_size = 5000 // len(attr_ids)
    # Make sure entity_ids is `list` for chunk selection
    entity_ids = list(entity_ids)
    for idx in range(0, len(entity_ids), chunk_size):
        entity_ids_joined = join_filter_values(
            entity_ids[idx:idx + chunk_size]
        )
        output.extend(
            session.query(
                (
                    "select value, entity_id, configuration_id"
                    " from CustomAttributeValue"
                    " where entity_id in ({}) and configuration_id in ({})"
                ).format(entity_ids_joined, attributes_joined)
            ).all()
        )
    return output


def _get_ftrack_project(session, project_name):
    ft_project = session.query((
        "select id, name, full_name from Project where full_name is \"{}\""
    ).format(project_name)).first()
    if ft_project is None:
        raise ValueError(
            "Project \"{}\" was not found in ftrack".format(project_name)
        )
    return ft_project


def _get_custom_attr_configs(session, query_keys=None):
    custom_attributes = []
    hier_custom_attributes = []
    if not query_keys:
        query_keys = [
            "id",
            "key",
            "entity_type",
            "object_type_id",
            "is_hierarchical",
            "default"
        ]

    cust_attrs_query = (
        "select {}"
        " from CustomAttributeConfiguration"
        " where group.name in (\"openpype\")"
    ).format(", ".join(query_keys))
    all_avalon_attr = session.query(cust_attrs_query).all()
    for cust_attr in all_avalon_attr:
        if cust_attr["is_hierarchical"]:
            hier_custom_attributes.append(cust_attr)
        else:
            custom_attributes.append(cust_attr)

    return custom_attributes, hier_custom_attributes


def get_custom_attributes_by_entity_id(
    session, entity_ids, attr_confs, hier_attr_confs
):
    entity_ids = set(entity_ids)
    hier_attr_ids = {
        attr_conf["id"]
        for attr_conf in hier_attr_confs
    }
    attr_by_id = {
        attr_conf["id"]: attr_conf["key"]
        for attr_conf in itertools.chain(attr_confs, hier_attr_confs)
    }

    value_items = query_custom_attribute_values(
        session, attr_by_id.keys(), entity_ids
    )

    output = collections.defaultdict(dict)
    for value_item in value_items:
        value = value_item["value"]
        if value is None:
            continue

        entity_id = value_item["entity_id"]
        entity_values = output[entity_id]
        attr_id = value_item["configuration_id"]
        attr_key = attr_by_id[attr_id]
        # Hierarchical attributes are always preferred
        if attr_id in hier_attr_ids or attr_key not in entity_values:
            entity_values[attr_key] = value

    return output


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

    def __init__(self, session, project_name):
        self._log = None
        self._ft_session = session
        self._project_name = project_name
        self._ids_mapping = IdsMapping()
        # Create entity hub which handle entity changes
        self._entity_hub = EntityHub(project_name)

    @property
    def project_name(self):
        return self._project_name

    @property
    def log(self):
        if self._log is None:
            logger = logging.getLogger(self.__class__.__name__)
            logger.setLevel(logging.DEBUG)
            self._log = logger
        return self._log

    def sync_to_server(self, preset_name=None):
        t_start = time.perf_counter()
        project_name = self.project_name
        ft_session = self._ft_session

        self.log.info(f"Synchronization of project \"{project_name}\" started")

        # Query ftrack project
        ft_project = _get_ftrack_project(ft_session, project_name)
        # Make sure project exists on server
        self.make_sure_project_exists(ft_project, preset_name)
        t_project_existence_1 = time.perf_counter()
        self.log.debug(
            f"Project existence check took {t_project_existence_1 - t_start}"
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
        # Get ftrack custom attributes to sync
        attr_confs, hier_attr_confs = _get_custom_attr_configs(ft_session)
        # Check if there is custom attribute to store server id
        server_id_conf = next(
            (
                attr_conf
                for attr_conf in hier_attr_confs
                if attr_conf["key"] == FTRACK_ATTR_KEY_SERVER_ID
            ), None
        )
        if not server_id_conf:
            raise ValueError(
                "Hierarchical attribute \"{}\" was not found in Ftrack".format(
                    FTRACK_ATTR_KEY_SERVER_ID))

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

        self.log.info("Querying project hierarchy")
        ft_entities = ft_session.query((
            "select id, name, parent_id, type_id, object_type_id"
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

        ft_entity_ids = set(ft_entities_by_id.keys())
        cust_attr_value_by_entity_id = get_custom_attributes_by_entity_id(
            ft_session, ft_entity_ids, attr_confs, hier_attr_confs
        )
        self.log.info("Checking changes of immutable entities")
        self.match_immutable_entities(
            ft_project,
            ft_entities_by_id,
            ft_entities_by_parent_id,
            ft_object_type_name_by_id,
            cust_attr_value_by_entity_id
        )

        self.log.info("Matching ftrack to server hierarchy")
        self.match_existing_entities(
            ft_project,
            ft_entities_by_parent_id,
            ft_object_type_name_by_id,
            ft_type_names_by_id,
            cust_attr_value_by_entity_id
        )

        self.log.info("Updating attributes of entities")
        self.update_attributes_from_ftrack(cust_attr_value_by_entity_id)
        self._entity_hub.commit_changes()

        self.log.info("Updating server ids on ftrack entities")
        self.update_ftract_attributes(
            cust_attr_value_by_entity_id,
            server_id_conf
        )
        t_end = time.perf_counter()
        self.log.info((
            f"Synchronization of project \"{project_name}\" finished"
            f" in {t_end-t_start}"
        ))

    def make_sure_project_exists(self, ft_project, preset_name=None):
        project_name = ft_project["full_name"]
        # Make sure project exists on server
        project = get_project(project_name)
        if not project:
            self.log.info(f"Creating project \"{project_name}\" on server")
            project_code = ft_project["name"]
            create_project(
                project_name,
                project_code,
                preset_name=preset_name
            )

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
                    "short_name": re.sub(r"\W+", "", name.lower())
                })

        project_entity.folder_types = new_folder_types
        project_entity.task_types = new_task_types

    def _recreate_ft_entity(
        self,
        entity,
        ft_parent,
        ft_project,
        ft_object_type_name_by_id
    ):
        if entity.entity_type != "folder":
            raise ValueError(
                "Didn't expect recreation of entity type \"{}\"".format(
                    entity.entity_type))

        object_type_id = None
        first_type_id = None
        default_type_id = None
        for object_id, object_name in ft_object_type_name_by_id.items():
            if object_name == entity.folder_type:
                object_type_id = object_id
                break

            if first_type_id is None:
                first_type_id = object_id

            if default_type_id is None and object_name == "Folder":
                default_type_id = object_id

        if object_type_id is None:
            object_type_id = default_type_id
            if object_type_id is None:
                object_type_id = first_type_id

        new_entity = self._ft_session.create(
            "TypedContext",
            {
                "name": entity.name,
                "object_type_id": object_type_id,
                "parent_id": ft_parent["id"],
                "project_id": ft_project["id"]
            }
        )
        self._ft_session.commit()
        return new_entity

    def _handle_not_found_immutable(
        self,
        entity,
        ft_parent,
        ft_project,
        ft_entities_by_id,
        ft_entities_by_parent_id,
        ft_object_type_name_by_id,
        cust_attr_value_by_entity_id,
    ):
        ft_parent_id = ft_parent["id"]
        expected_ftrack_id = entity.attribs[FTRACK_ID_ATTRIB]
        if expected_ftrack_id:
            ft_entity = ft_entities_by_id.get(expected_ftrack_id)

        if ft_entity is not None:
            # Remove entity from previous parent
            prev_parent_id = ft_entity["parent_id"]
            ft_entities_by_parent_id[prev_parent_id].remove(ft_entity)
            # Change parent on ftrack entity
            ft_entity["parent_id"] = ft_parent_id

        else:
            ft_entity = self._recreate_ft_entity(
                entity,
                ft_parent,
                ft_project,
                ft_object_type_name_by_id
            )
            ftrack_id = ft_entity["id"]
            cust_attr_value_by_entity_id[ftrack_id] = {}
            ft_entities_by_id[ftrack_id] = ft_entity

        ft_entities_by_parent_id[ft_parent_id].append(ft_entity)
        return ft_entity

    def match_immutable_entities(
        self,
        ft_project,
        ft_entities_by_id,
        ft_entities_by_parent_id,
        ft_object_type_name_by_id,
        cust_attr_value_by_entity_id,
    ):
        immutable_queue = collections.deque()
        for entity in self._entity_hub.project_entity:
            if entity.immutable_for_hierarchy:
                immutable_queue.append((entity, ft_project))

        commit_object = object()
        while immutable_queue:
            item = immutable_queue.popleft()
            if item is commit_object:
                if self._ft_session.recorded_operations:
                    self._ft_session.commit()
                continue

            entity, ft_parent = item
            entity_low_name = entity.name.lower()

            ft_children = ft_entities_by_parent_id[ft_parent["id"]]
            matching_ft_entity = next(
                (
                    ft_child
                    for ft_child in ft_children
                    if ft_child["name"].lower() == entity_low_name
                ), None
            )

            if matching_ft_entity is None:
                matching_ft_entity = self._handle_not_found_immutable(
                    entity,
                    ft_parent,
                    ft_project,
                    ft_entities_by_id,
                    ft_entities_by_parent_id,
                    ft_object_type_name_by_id,
                    cust_attr_value_by_entity_id,
                )

            entity.attribs[FTRACK_ID_ATTRIB] = matching_ft_entity["id"]
            if matching_ft_entity["name"] != entity.name:
                matching_ft_entity["name"] = entity.name

            self._ids_mapping.set_server_to_ftrack(
                entity.id, matching_ft_entity["id"])

            for child in entity:
                if child.immutable_for_hierarchy:
                    immutable_queue.append((child, matching_ft_entity))
            immutable_queue.append(commit_object)

    def _create_new_entity(
        self,
        parent_entity,
        ft_entity,
        ft_object_type_name_by_id,
        ft_type_names_by_id,
        cust_attr_value_by_entity_id,
    ):
        ftrack_id = ft_entity["id"]
        custom_attributes = cust_attr_value_by_entity_id[ftrack_id]
        entity_id = custom_attributes.get(FTRACK_ATTR_KEY_SERVER_ID)
        if entity_id:
            # Check if entity id from custom attributes already have mapping to
            #   different entity
            # - this can happen when entity was moved to other place and entity
            #       with same name was created at the same hierarchy
            mapped_ftrack_id = self._ids_mapping.get_ftrack_mapping(entity_id)
            if mapped_ftrack_id and mapped_ftrack_id != ftrack_id:
                entity_id = None

        entity_type = ft_entity.entity_type
        if entity_type.lower() == "task":
            task_type_name = ft_type_names_by_id[ft_entity["type_id"]]
            new_entity = parent_entity.add_task(
                task_type_name,
                ft_entity["name"],
                entity_id=entity_id,
                is_new=True
            )

        else:
            object_type = ft_object_type_name_by_id[ft_entity["object_type_id"]]
            new_entity = parent_entity.add_folder(
                object_type,
                ft_entity["name"],
                entity_id=entity_id,
                is_new=True
            )

        return new_entity

    def match_existing_entities(
        self,
        ft_project,
        ft_entities_by_parent_id,
        ft_object_type_name_by_id,
        ft_type_names_by_id,
        cust_attr_value_by_entity_id,
    ):
        """Match exiting entities on both sides.

        Create new entities that are on ftrack and are not on server and remove
        those which are not on ftrack.

        Args:
            ft_project (ftrack_api.Entity): Ftrack project entity.
            ft_entities_by_parent_id (dict[str, list[ftrack_api.Entity]]): Ftrack
                entities by their parent ids.
            ft_object_type_name_by_id (Dict[str, str]): Mapping of ftrack object
                type ids to their names.
            ft_type_names_by_id (Dict[str,
        """

        found_entity_ids = {ft_project["id"]}

        server_entity_by_ftrack_id = {}
        for entity in self._entity_hub.entities:
            ftrack_id = entity.attribs[FTRACK_ID_ATTRIB]
            if ftrack_id:
                server_entity_by_ftrack_id[ftrack_id] = entity

        fill_queue = collections.deque()
        fill_queue.append((self._entity_hub.project_entity, ft_project))
        while fill_queue:
            item = fill_queue.popleft()
            entity, ft_entity = item
            ft_children = ft_entities_by_parent_id[ft_entity["id"]]
            ft_names = set()
            for ft_child in ft_children:
                ft_low_name = ft_child["name"].lower()
                ft_names.add(ft_low_name)
                # Match entity could be already matched in immutable entities
                #   handling
                ft_child_id = ft_child["id"]
                entity_match_id = self._ids_mapping.get_server_mapping(
                    ft_child_id)
                entity_match = None
                if entity_match_id is not None:
                    entity_match = self._entity_hub.get_entity_by_id(
                        entity_match_id)

                # Find entity based on same name on same hierarchy level
                is_task = ft_child.entity_type.lower() == "task"
                if entity_match is None and entity is not None:
                    entity_match = next(
                        (
                            child
                            for child in entity
                            if (
                                # Ftrack is case insensitive ("Bob" == "bob")
                                ft_low_name == child.name.lower()
                                and is_task is (child.entity_type == "task")
                            )
                        ), None
                    )

                # Entity match found so just store and process it's children
                if entity_match is None:
                    ftrack_id = ft_child["id"]
                    expected_entity = server_entity_by_ftrack_id.get(ftrack_id)
                    if expected_entity is not None:
                        expected_entity.set_parent(entity)
                        entity_match = expected_entity

                if entity_match is None:
                    print("Creating new entity")
                    entity_match = self._create_new_entity(
                        entity,
                        ft_child,
                        ft_object_type_name_by_id,
                        ft_type_names_by_id,
                        cust_attr_value_by_entity_id,
                    )

                if is_task:
                    task_type_id = ft_child["type_id"]
                    task_type_name = ft_type_names_by_id[task_type_id]
                    if entity_match.task_type != task_type_name:
                        entity_match.task_type = task_type_name
                else:
                    object_type_id = ft_child["object_type_id"]
                    object_type_name = ft_object_type_name_by_id[object_type_id]
                    if entity_match.folder_type != object_type_name:
                        entity_match.folder_type = object_type_name

                entity_match.attribs[FTRACK_ID_ATTRIB] = ft_child_id
                found_entity_ids.add(ft_child_id)
                self._ids_mapping.set_ftrack_to_server(
                    ft_child_id, entity_match.id
                )
                fill_queue.append((entity_match, ft_child))

            if entity is None:
                continue

            for child in tuple(entity):
                if child.name.lower() not in ft_names:
                    entity.remove_child(child)

    def update_attributes_from_ftrack(self, cust_attr_value_by_entity_id):
        hierarchy_queue = collections.deque()
        hierarchy_queue.append(self._entity_hub.project_entity)
        while hierarchy_queue:
            entity = hierarchy_queue.popleft()
            # Add children to queue
            for child_entity in entity:
                hierarchy_queue.append(child_entity)

            ftrack_id = self._ids_mapping.get_ftrack_mapping(entity.id)
            entity.attribs[FTRACK_ID_ATTRIB] = ftrack_id
            # Ftrack id can not be available if ftrack entity was recreated
            #   during immutable entity processing
            attribute_values = cust_attr_value_by_entity_id[ftrack_id]
            is_project = entity.entity_type == "project"

            # TODO handle "data" to sync custom attributes not available
            #   in 'attribs'
            for key, value in attribute_values.items():
                # QUESTION Should we skip "unsetting" of project attributes?
                #   - very dangerous for OpenPype and maybe for project should
                #       be taken default value of attribute (if there is any)
                if is_project and value is None:
                    continue

                if key in entity.attribs:
                    entity.attribs[key] = value

    def update_ftract_attributes(
        self,
        cust_attr_value_by_entity_id,
        server_id_conf,
    ):
        fill_queue = collections.deque()
        for child in self._entity_hub.project_entity:
            fill_queue.append(child)

        operations = []
        while fill_queue:
            entity = fill_queue.popleft()
            for child in entity:
                fill_queue.append(child)

            ftrack_id = self._ids_mapping.get_ftrack_mapping(entity.id)
            custom_attributes = cust_attr_value_by_entity_id[ftrack_id]
            server_id = custom_attributes.get(FTRACK_ATTR_KEY_SERVER_ID)
            if server_id == entity.id:
                continue

            entity_key = collections.OrderedDict((
                ("configuration_id", server_id_conf["id"]),
                ("entity_id", ftrack_id)
            ))
            if FTRACK_ATTR_KEY_SERVER_ID not in custom_attributes:
                operations.append(
                    ftrack_api.operation.CreateEntityOperation(
                        "CustomAttributeValue",
                        entity_key,
                        {"value": entity.id}
                    )
                )
            else:
                operations.append(
                    ftrack_api.operation.UpdateEntityOperation(
                        "CustomAttributeValue",
                        entity_key,
                        "value",
                        server_id,
                        entity.id
                    )
                )

        if not operations:
            return

        for chunk in create_chunks(operations, 500):
            for operation in chunk:
                self._ft_session.recorded_operations.push(operation)
            self._ft_session.commit()