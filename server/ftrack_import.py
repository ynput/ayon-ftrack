# TODO *MID* Find out if we need AYON's custom attributes in ftrack? Because
#    it might be possible that imported project won't be used afterwards.
# TODO *HIGH* Better handling of invalid characters in names
#    Folder and task name, maybe even type names?
# TODO *MID* Define default username for not mapped users for entity creation
#    and comments.
# TODO *LOW* Create entities as users who created them in ftrack.
# TODO *HIGH* sync all attributes from ftrack - create mapping in dialog
#    left side ftrack attribute right side comboboxes with AYON attributes
# TODO *MID* Make sure 'FTRACK_ID_ATTRIB' and 'FTRACK_PATH_ATTRIB'
#   do exist in AYON, or do not set them.
# TODO *MID* Make sure ftrack custom attributes contains mandatory ftrack
#   attributes, or do not set them.
# TODO *MID* Sync reviewables.
import re
import io
import uuid
import json
import collections
from typing import Any, Optional, Union

import httpx
from nxtools import slugify, logging

from ayon_server.access.access_groups import AccessGroups
from ayon_server.activities import create_activity
from ayon_server.lib.postgres import Postgres
from ayon_server.settings.anatomy import Anatomy
from ayon_server.entities import UserEntity, ProjectEntity
from ayon_server.entities.core import attribute_library
from ayon_server.exceptions import NotFoundException, BadRequestException
from ayon_server.helpers.thumbnails import store_thumbnail
from ayon_server.helpers.deploy_project import create_project_from_anatomy
from ayon_server.helpers.get_entity_class import get_entity_class
from ayon_server.operations import ProjectLevelOperations
from ayon_server.types import (
    PROJECT_NAME_REGEX,
    # PROJECT_CODE_REGEX,
    # USER_NAME_REGEX,
    # NAME_REGEX,
    # STATUS_REGEX,
)

from .constants import (
    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
    CUST_ATTR_GROUP,

    CUST_ATTR_KEY_SERVER_ID,
    CUST_ATTR_KEY_SERVER_PATH,
)
from .ftrack_session import (
    FtrackSession,
    ServerError,
    FtrackEntityType,
    join_filter_values,
    create_chunks,
    convert_ftrack_date_obj,
    convert_ftrack_date,
)

FTRACK_REVIEW_NAMES = [
    "ftrackreview-mp4",
    "ftrackreview-webm",
    "ftrackreview-image",
]
CREATE_ITEM = "__create__"
SKIP_ITEM = "__skip__"


async def _make_sure_ayon_custom_attribute_exists(
    session: FtrackSession,
    existing_custom_attributes: list[dict[str, Any]],
):
    security_roles = await session.query(
        "select id, name, type from SecurityRole"
    ).all()
    attr_security_roles = [
        {
            "__entity_type__": "SecurityRole",
            "id": security_role["id"],
        }
        for security_role in security_roles
    ]
    existing_keys = {
        attr_conf["key"]
        for attr_conf in existing_custom_attributes
    }
    batch = []
    for item in [
        {
            "key": CUST_ATTR_KEY_SERVER_ID,
            "type": "text",
            "label": "AYON ID",
            "default": "",
            "is_hierarchical": True,
            "config": json.dumps({"markdown": False}),
        },
        {
            "key": CUST_ATTR_KEY_SERVER_PATH,
            "type": "text",
            "label": "AYON path",
            "default": "",
            "is_hierarchical": True,
            "config": json.dumps({"markdown": False}),
        },
    ]:
        if item["key"] in existing_keys:
            continue
        item_id = str(uuid.uuid4())
        item.update({
            "__entity_type__": "CustomAttributeConfiguration",
            "core": False,
            "sort": 0,
            "id": item_id,
            "read_security_roles": attr_security_roles,
            "write_security_roles": attr_security_roles,
        })
        item.setdefault("entity_type", "show")
        batch.append({
            "action": "create",
            "entity_data": item,
            "entity_key": [item_id],
            "entity_type": "CustomAttributeConfiguration",
        })

    if not batch:
        return

    try:
        await session.call(batch)
    except ServerError:
        raise BadRequestException(
            "Failed to create custom attributes in ftrack."
            " Please make sure API key you filled has admin permissions."
        )


async def _get_supported_attribute_types(
    session: FtrackSession
) -> dict[str, str]:
    supported_types = {
        "text",
        "enumerator",
        "date",
        "number",
        "boolean",
        # "dynamic",
        # "notificationtype",
        # "expression",
        # "url",
    }
    return {
        conf_type["id"]: conf_type["name"]
        for conf_type in await session.query(
            "select id, name from CustomAttributeType"
        ).all()
        if conf_type["name"] in supported_types
    }


async def _find_possible_ayon_attributes(
    attr_conf: dict[str, Any],
    attr_type_name_by_id: dict[str, str],
    ayon_attributes: dict[str, Any],
):
    _ = {
        "text": {"string"},
        "date": {"datetime", "string"},
        "number": {"integer", "float", "string"},
        "boolean": {"boolean"},
        "enumerator": {"string", "list_of_strings", "list_of_integers"},
    }
    # NOTE Maybe also filter those that are built-in and don't have correct
    #   scope
    output = []
    attr_type_name = attr_type_name_by_id[attr_conf["type_id"]]
    for ayon_attribute in ayon_attributes.values():
        is_builtin = ayon_attribute["builtin"]
        ayon_attr_data = ayon_attribute["data"]
        if attr_type_name == "boolean":
            if ayon_attr_data["type"] == "boolean":
                output.append(ayon_attribute)

        elif attr_type_name == "text":
            if ayon_attr_data["type"] == "string":
                output.append(ayon_attribute)

        elif attr_type_name == "date":
            if ayon_attr_data["type"] in {"string", "datetime"}:
                output.append(ayon_attribute)

        elif attr_type_name == "enumerator":
            config = json.loads(attr_conf["config"])
            # Mapping to AYON builtin enumerator might be potential danger
            if is_builtin:
                pass
            elif config["multiSelect"]:
                if ayon_attr_data["type"] == "list_of_strings":
                    output.append(ayon_attribute)
            elif ayon_attr_data["type"] == "string":
                output.append(ayon_attribute)

        elif attr_type_name == "number":
            if ayon_attr_data["type"] in {"integer", "float", "string"}:
                output.append(ayon_attribute)

    return output


def _find_best_attr_conf(
    attr_confs: list[FtrackEntityType]
) -> list[FtrackEntityType]:
    if len(attr_confs) == 1:
        return attr_confs

    confs_by_type = collections.defaultdict(list)
    for _attr_conf in attr_confs:
        if _attr_conf["is_hierarchical"]:
            return [_attr_conf]
        confs_by_type[_attr_conf["type_id"]].append(_attr_conf)

    max_type_count = 0
    attr_confs = []
    for type_id, _attr_confs in confs_by_type.items():
        if len(_attr_confs) > max_type_count:
            max_type_count = len(_attr_confs)
            attr_confs = _attr_confs
    return attr_confs


async def prepare_attributes_mapping(session: FtrackSession):
    supported_type_ids = await _get_supported_attribute_types(session)
    attr_confs = await session.query(
        "select id, key, label, type_id, is_hierarchical,"
        " entity_type, config, object_type_id, default"
        " from CustomAttributeConfiguration"
    ).all()

    # NOTE make it optional?
    # Make sure ftrack has minimum required custom attributes we need for
    #   import
    await _make_sure_ayon_custom_attribute_exists(session, attr_confs)

    filtered_attr_confs = [
        attr_conf
        for attr_conf in attr_confs
        if (
            attr_conf["type_id"] in supported_type_ids
            and attr_conf["key"] not in {
                # TODO skip also keys that are not create with import script
                #   - can be found in constants.py
                CUST_ATTR_KEY_SERVER_PATH,
                CUST_ATTR_KEY_SERVER_ID,
            }
        )
    ]

    # TODO probably collect their scope and validate if ftrack attribute
    #   can be used for mapping in AYON?
    ayon_attributes = {
        row["name"]: row
        async for row in Postgres.iterate(
            "SELECT name, data, builtin FROM public.attributes"
        )
    }
    ayon_attribute_names = {
        name.lower()
        for name in ayon_attributes
    }
    # Remove ftrack attributes
    for attr_name in {FTRACK_ID_ATTRIB, FTRACK_PATH_ATTRIB}:
        ayon_attributes.pop(attr_name, None)

    # Group ftrack attributes by key
    attr_confs_by_key = collections.defaultdict(list)
    for attr_conf in filtered_attr_confs:
        attr_confs_by_key[attr_conf["key"]].append(attr_conf)

    items = []
    for key, attr_confs in attr_confs_by_key.items():
        # Choose which attribute will be used for mapping
        # - only one is used, hierarchical has always priority, then the most
        #   counted by attribute type (2 bools and 1 text -> bool is used)
        attr_confs = _find_best_attr_conf(attr_confs)
        if not attr_confs:
            continue

        enum_items = [{
            "value": SKIP_ITEM,
            "label": "< Skip >",
        }]
        # Do not allow to create attribute if already exists in AYON
        mapped_key = None
        possible_attributes = await _find_possible_ayon_attributes(
            attr_confs[0], supported_type_ids, ayon_attributes
        )
        possible_attributes.sort(key=lambda i: i["name"])
        for ayon_attribute in possible_attributes:
            enum_items.append({
                "value": ayon_attribute["name"],
                "label": ayon_attribute["name"],
            })
            if key.lower() == ayon_attribute["name"].lower():
                mapped_key = ayon_attribute["name"]
            elif key == "fstart":
                mapped_key = "frameStart"
            elif key == "fend":
                mapped_key = "frameEnd"

        if key.lower() not in ayon_attribute_names:
            enum_items.insert(
                0,
                {
                    "value": CREATE_ITEM,
                    "label": "< Create >"
                }
            )
            if not mapped_key:
                mapped_key = CREATE_ITEM

        # There is only 'SKIP_ITEM'
        if len(enum_items) == 1:
            continue

        if not mapped_key:
            mapped_key = SKIP_ITEM
        items.append({
            "key": key,
            "value": mapped_key,
            "enum_items": enum_items,
        })

    return {
        "items": items,
    }


class MappedAYONAttribute:
    def __init__(
        self,
        ayon_attribute_name: str,
        attr_confs: Optional[list[FtrackEntityType]] = None,
    ):
        self.ayon_attribute_name: str = ayon_attribute_name
        if attr_confs is None:
            attr_confs = []
        hierarchical_attr = None
        for attr_conf in attr_confs:
            if attr_conf["is_hierarchical"]:
                hierarchical_attr = attr_conf
                break
        self._hierarchical_attr: Optional[FtrackEntityType] = (
            hierarchical_attr
        )
        self._attr_confs: list[FtrackEntityType] = attr_confs

    def has_confs(self) -> bool:
        return bool(self.attr_confs)

    def add_attr_conf(self, attr_conf: FtrackEntityType):
        self._attr_confs.append(attr_conf)
        if attr_conf["is_hierarchical"]:
            self._hierarchical_attr = attr_conf

    def get_attr_confs(self) -> list[FtrackEntityType]:
        return list(self._attr_confs)

    attr_confs: list[FtrackEntityType] = property(get_attr_confs)

    def get_attr_conf_for_entity_type(
        self, entity_type: str, object_type_id: Optional[str]
    ) -> Optional[FtrackEntityType]:
        if not self.attr_confs:
            return None

        if self._hierarchical_attr is not None:
            return self._hierarchical_attr

        for attr_conf in self.attr_confs:
            if (
                attr_conf["entity_type"] == entity_type
                and attr_conf["object_type_id"] == object_type_id
            ):
                return attr_conf
        return None

    def get_attr_conf_for_entity(
        self, entity: FtrackEntityType
    ) -> Optional[FtrackEntityType]:
        if entity is None:
            return None

        entity_type = entity["__entity_type__"].lower()
        object_type_id = None
        if "context_type" in entity:
            entity_type = entity["context_type"]
            if entity_type == "task":
                object_type_id = entity["object_type_id"]
        return self.get_attr_conf_for_entity_type(
            entity_type, object_type_id
        )


class CustomAttributesMapping:
    def __init__(self, attr_confs):
        self._items: dict[str, MappedAYONAttribute] = {}
        self._attr_confs = attr_confs

    def __contains__(self, item):
        return item in self._items

    @property
    def attr_confs(self) -> list[FtrackEntityType]:
        return self._attr_confs

    def items(self):
        return self._items.items()

    def values(self):
        return self._items.values()

    def keys(self):
        return self._items.keys()

    def get(self, key, default=None):
        return self._items.get(key, default)

    def add_mapping_item(self, item: MappedAYONAttribute):
        self._items[item.ayon_attribute_name] = item

    def get_mapping_item_by_key(
        self, ft_entity: dict[str, Any], key: str
    ) -> Optional[MappedAYONAttribute]:
        for mapping_item in self.values():
            attr_conf = mapping_item.get_attr_conf_for_entity(ft_entity)
            if attr_conf is not None and attr_conf["key"] == key:
                return mapping_item


async def _get_custom_attributes_mapping(
    session: FtrackSession,
    attributes_mapping: dict[str, str],
) -> CustomAttributesMapping:
    """Query custom attribute configurations from ftrack server.

    Returns:
        CustomAttributesMapping: ftrack custom attributes mapping.

    """
    attr_confs: list[FtrackEntityType] = await session.query(
        "select id, key, entity_type, object_type_id, is_hierarchical,"
        " default, type_id from CustomAttributeConfiguration"
    ).all()

    ayon_attribute_names = set()
    builtin_attributes_names = set()
    for attr in attribute_library.info_data:
        ayon_attribute_names.add(attr["name"])
        if attr["builtin"]:
            builtin_attributes_names.add(attr["name"])

    type_names_by_id = await _get_supported_attribute_types(session)
    ftrack_attrs_by_name = collections.defaultdict(list)
    for attr_conf in attr_confs:
        type_name = type_names_by_id.get(attr_conf["type_id"])
        if type_name is not None:
            # NOTE Adding 'type_name' to attribute configuration
            attr_conf["type_name"] = type_name
            key = attr_conf["key"]
            ftrack_attrs_by_name[key].append(attr_conf)

    output = CustomAttributesMapping(attr_confs)
    for ftrack_key, ayon_attr_name in attributes_mapping.items():
        if ayon_attr_name == SKIP_ITEM:
            continue

        if ayon_attr_name == CREATE_ITEM:
            ayon_attr_name = ftrack_key

        if ayon_attr_name not in ayon_attribute_names:
            continue

        mapped_item = MappedAYONAttribute(ayon_attr_name)
        for attr_conf in ftrack_attrs_by_name[ftrack_key]:
            mapped_item.add_attr_conf(attr_conf)

        output.add_mapping_item(mapped_item)

    for attr_name in ayon_attribute_names:
        if attr_name not in output:
            output.add_mapping_item(MappedAYONAttribute(attr_name))

    return output


async def _query_custom_attribute_values(
    session: FtrackSession,
    entity_ids: set[str],
    attr_confs_ids: set[str],
):
    values_by_id = {
        entity_id: []
        for entity_id in entity_ids
    }
    if not attr_confs_ids:
        return values_by_id
    joined_attr_ids = join_filter_values(attr_confs_ids)
    for chunk_ids in create_chunks(entity_ids):
        joined_ids = join_filter_values(chunk_ids)
        for item in await session.query(
            "select value, entity_id, configuration_id"
            " from CustomAttributeValue"
            f" where entity_id in ({joined_ids})"
            f" and configuration_id in ({joined_attr_ids})"
        ).all():
            values_by_id[item["entity_id"]].append(item)
    return values_by_id


async def _convert_attrib_value(
    value: Any,
    ftrack_attr: dict[str, Any],
    ayon_attr: dict[str, Any]
):
    if value is None:
        return value

    ayon_type_name = ayon_attr["type"]
    ftrack_type_name = ftrack_attr["type_name"]

    if ayon_type_name == "boolean":
        return bool(value)

    if ftrack_type_name == "boolean":
        return None

    if ftrack_type_name == "date":
        if ayon_type_name in ("datetime", "string"):
            return convert_ftrack_date(value)
        return None

    if ftrack_type_name == "text":
        if isinstance(value, (str, int, float, bool)):
            return str(value)
        return None

    if ftrack_type_name == "number":
        if ayon_type_name == "string":
            return str(value)

        if ayon_type_name not in ("integer", "float"):
            return None

        if isinstance(value, str):
            try:
                value = float(value)
            except ValueError:
                pass

        if not isinstance(value, (int, float)):
            return None

        if ayon_type_name == "integer":
            return int(value)
        return float(value)

    if ftrack_type_name == "enumerator":
        if ayon_type_name == "string":
            if isinstance(value, str):
                return value
            if value and isinstance(value, list):
                return value[0]
            return None

        if ayon_type_name != "list_of_strings":
            return None

        enum = ayon_attr["data"].get("enum")
        if not enum:
            return None

        available_values = {
            item["value"]
            for item in enum
        }
        if isinstance(value, str):
            value = [value]

        if isinstance(value, list):
            return [
                subvalue
                for subvalue in value
                if subvalue in available_values
            ]
        return None
    return None


async def _get_anatomy_preset() -> dict:
    primary = {}
    async for row in Postgres.iterate(
        "SELECT * from anatomy_presets ORDER BY name, version"
    ):
        if row["is_primary"]:
            primary = row

    anatomy = Anatomy(**primary)
    return json.loads(anatomy.json())


async def _prepare_project_entity(
    session: FtrackSession,
    project: dict[str, Any],
    statuses: list[dict[str, str]],
    object_types: list[dict[str, str]],
    types: list[dict[str, str]],
    attrs_mapping: CustomAttributesMapping,
    ayon_attr_by_name: dict[str, Any],
    attr_values: list[dict[str, Any]],
    thumbnails_mapping: dict[str, str],
):
    project_name = project["full_name"]
    if project["thumbnail_id"]:
        thumbnails_mapping[project_name] = project["thumbnail_id"]

    ayon_project = await _get_anatomy_preset()

    attr_values_by_attr_id = {
        attr_value["configuration_id"]: attr_value["value"]
        for attr_value in attr_values
    }
    attribs = ayon_project["attributes"]
    attribs[FTRACK_ID_ATTRIB] = project["id"]
    for mapping_item in attrs_mapping.values():
        attr_conf = mapping_item.get_attr_conf_for_entity(project)
        if attr_conf is None:
            continue
        attr_conf_id = attr_conf["id"]
        dst_key = mapping_item.ayon_attribute_name
        value = await _convert_attrib_value(
            attr_values_by_attr_id.get(attr_conf_id),
            attr_conf,
            ayon_attr_by_name[dst_key]
        )
        if value is not None:
            attribs[dst_key] = value

    project_schema_id = project["project_schema_id"]
    fields = [
        "id",
        "task_type_schema",
        "object_types",
        "task_workflow_schema",
        "task_workflow_schema_overrides",
        "asset_version_workflow_schema",
        "object_type_schemas",
    ]
    schema_fields = ", ".join(fields)
    project_schema = await session.query(
        f"select {schema_fields} from ProjectSchema"
        f" where id is \"{project_schema_id}\""
    ).first()

    schema_object_ids = {
        item["id"]
        for item in project_schema["object_types"]
    }
    task_type_schema_id = project_schema["task_type_schema"]["id"]
    schema_task_type_ids = {
        item["type_id"]
        for item in await session.query(
            "select type_id"
            " from TaskTypeSchemaType"
            f" where task_type_schema_id  is \"{task_type_schema_id}\""
        ).all()
    }

    # Folder statuses
    schema_ids = {
        schema["id"]
        for schema in project_schema["object_type_schemas"]
    }
    object_type_schemas = []
    if schema_ids:
        joined_schema_ids = join_filter_values(schema_ids)
        object_type_schemas = await session.query(
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
        schema_statuses = await session.query(
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
        override_schemas = await session.query(
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
    workflow_statuses = await session.query(
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
        for item in await session.query(
            "select status_id"
            " from WorkflowSchemaStatus"
            f" where workflow_schema_id is '{av_workflow_schema_id}'"
        ).all()
    }

    statuses_by_id = {
        status["id"]: status
        for status in statuses
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
    state_name_by_id = {
        state["id"]: state["name"]
        for state in await session.query(
            "select id, name from State"
        ).all()
    }
    existing_statuses_by_low_name = {
        status["name"].lower(): status
        for status in ayon_project["statuses"]
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

        status_name = status["name"]
        short_name = ""
        existing_status = existing_statuses_by_low_name.get(status_name.lower())
        if existing_status:
            short_name = existing_status["shortName"]

        state_id = status["state"]["id"]
        ft_state = state_name_by_id[state_id]
        ayon_state = state_mapping[ft_state]
        statuses_data.append({
            "name": status_name,
            "color": status["color"],
            "state": ayon_state,
            "scope": scope,
            "sort": status["sort"],
            "shortName": short_name,
        })
    statuses_data.sort(key=lambda i: i["sort"])

    existing_folder_types = {
        folder_type["name"].lower(): folder_type
        for folder_type in ayon_project["folder_types"]
    }
    folder_types = []
    for object_type in sorted(object_types, key=lambda o: o["sort"]):
        name = object_type["name"]
        if (
            name in {"task", "milestone"}
            or object_type["id"] not in schema_object_ids
        ):
            continue

        folder_type = existing_folder_types.get(name.lower(), {})
        folder_type["name"] = name
        folder_types.append(folder_type)

    existing_task_types = {
        task_type["name"].lower(): task_type
        for task_type in ayon_project["task_types"]
    }
    task_types = []
    for ft_type in types:
        if ft_type["id"] not in schema_task_type_ids:
            continue
        name = ft_type["name"]
        task_type = existing_task_types.get(name.lower(), {})
        task_type["name"] = name
        task_type["shortName"] = re.sub(r"\W+", "", name.lower())
        task_types.append(task_type)

    ayon_project["statuses"] = statuses_data
    ayon_project["folder_types"] = folder_types
    ayon_project["task_types"] = task_types
    return ayon_project


async def _prepare_folder_entities(
    project_id: str,
    ftrack_entities: list[dict[str, Any]],
    status_names_by_id: dict[str, str],
    object_types: list[dict[str, Any]],
    attrs_mapping: CustomAttributesMapping,
    ayon_attr_by_name: dict[str, Any],
    attr_values_by_id: dict[str, list[dict[str, Any]]],
    thumbnails_mapping: dict[str, str],
) -> dict[str, dict[str, Any]]:
    folder_entities_by_ftrack_id: dict[str, dict[str, Any]] = {}
    ftrack_entities_by_parent_id = collections.defaultdict(list)
    for entity in ftrack_entities:
        ftrack_entities_by_parent_id[entity["parent_id"]].append(entity)

    object_type_names_by_id = {
        object_type["id"]: object_type["name"]
        for object_type in object_types
    }
    parents_queue = collections.deque()
    parents_queue.append((None, project_id, ""))
    while parents_queue:
        parent_id, ftrack_parent_id, ftrack_parent_path = (
            parents_queue.popleft()
        )
        for ft_entity in ftrack_entities_by_parent_id[ftrack_parent_id]:
            ftrack_id = ft_entity["id"]

            ayon_id = uuid.uuid4().hex

            name = ft_entity["name"]
            ftrack_path = f"{ftrack_parent_path}/{name}"
            folder_name = slugify(name, "_")
            label = None
            if name != folder_name:
                label = name

            obj_id = ft_entity["object_type_id"]
            status_id = ft_entity["status_id"]

            folder_type = object_type_names_by_id[obj_id]
            status = status_names_by_id.get(status_id)

            attribs = {
                FTRACK_ID_ATTRIB: ftrack_id,
                FTRACK_PATH_ATTRIB: ftrack_path,
            }
            for src_key, dst_key in (
                ("start_date", "startDate"),
                ("end_date", "endDate"),
                ("description", "description"),
            ):
                value = ft_entity.get(src_key)
                if value is not None:
                    attribs[dst_key] = value

            attr_values_by_attr_id = {
                attr_value["configuration_id"]: attr_value["value"]
                for attr_value in attr_values_by_id.get(ftrack_id) or []
            }
            for mapping_item in attrs_mapping.values():
                attr_conf = mapping_item.get_attr_conf_for_entity(ft_entity)
                if attr_conf is None:
                    continue
                dst_key = mapping_item.ayon_attribute_name
                value = await _convert_attrib_value(
                    attr_values_by_attr_id.get(attr_conf["id"]),
                    attr_conf,
                    ayon_attr_by_name[dst_key]
                )
                if value is not None:
                    attribs[dst_key] = value

            if ft_entity["thumbnail_id"]:
                thumbnails_mapping[ayon_id] = ft_entity["thumbnail_id"]

            folder_entities_by_ftrack_id[ftrack_id] = {
                "entity_id": ayon_id,
                "name": folder_name,
                "label": label,
                "parentId": parent_id,
                "folderType": folder_type,
                "status": status,
                "attrib": attribs,
            }
            parents_queue.append((ayon_id, ftrack_id, ftrack_path))
    return folder_entities_by_ftrack_id


async def _prepare_task_entities(
    folder_by_ftrack_id: dict[str, Any],
    ftrack_entities: list[dict[str, Any]],
    status_names_by_id: dict[str, str],
    types: list[dict[str, Any]],
    attrs_mapping: CustomAttributesMapping,
    ayon_attr_by_name: dict[str, Any],
    attr_values_by_id: dict[str, list[dict[str, Any]]],
    assignment_by_task_id: dict[str, set[str]],
    users_mapping: dict[str, Union[str, None]],
    thumbnails_mapping: dict[str, str],
) -> dict[str, dict[str, Any]]:
    task_entities_by_ftrack_id: dict[str, dict[str, Any]] = {}
    type_names_by_id = {
        ft_type["id"]: ft_type["name"]
        for ft_type in types
    }
    for ftrack_entity in ftrack_entities:
        ftrack_id = ftrack_entity["id"]
        ftrack_parent_id = ftrack_entity["parent_id"]
        ayon_parent = folder_by_ftrack_id.get(ftrack_parent_id)
        # When task is parented under another task
        if ayon_parent is None:
            continue

        name = ftrack_entity["name"]
        task_name = slugify(name, "_")
        task_label = None
        if name != task_name:
            task_label = name
        ftrack_path = ayon_parent["attrib"][FTRACK_PATH_ATTRIB] + "/" + name
        task_type = type_names_by_id[ftrack_entity["type_id"]]

        status = status_names_by_id[ftrack_entity["status_id"]]
        # TODO make sure 'FTRACK_ID_ATTRIB' and 'FTRACK_PATH_ATTRIB'
        #   do exist in AYON
        attribs = {
            FTRACK_ID_ATTRIB: ftrack_id,
            FTRACK_PATH_ATTRIB: ftrack_path,
        }
        for src_key, dst_key in (
            ("start_date", "startDate"),
            ("end_date", "endDate"),
            ("description", "description"),
        ):
            value = ftrack_entity.get(src_key)
            if value is not None:
                attribs[dst_key] = value

        attr_values_by_attr_id = {
            attr_value["configuration_id"]: attr_value["value"]
            for attr_value in attr_values_by_id.get(ftrack_entity["id"]) or []
        }
        for mapping_item in attrs_mapping.values():
            attr_conf = mapping_item.get_attr_conf_for_entity(ftrack_entity)
            if attr_conf is None:
                continue

            dst_key = mapping_item.ayon_attribute_name
            value = await _convert_attrib_value(
                attr_values_by_attr_id.get(attr_conf["id"]),
                attr_conf,
                ayon_attr_by_name[dst_key]
            )
            if value is not None:
                attribs[dst_key] = value

        assignees = []
        for user_id in assignment_by_task_id.get(ftrack_id, []):
            user_name = users_mapping.get(user_id)
            if user_name is not None:
                assignees.append(user_name)

        ayon_id = uuid.uuid4().hex
        if ftrack_entity["thumbnail_id"]:
            thumbnails_mapping[ayon_id] = ftrack_entity["thumbnail_id"]

        task_entity = {
            "entity_id": ayon_id,
            "name": task_name,
            "label": task_label,
            "folderId": ayon_parent["entity_id"],
            "taskType": task_type,
            "status": status,
            "attrib": attribs,
        }
        if assignees:
            task_entity["assignees"] = assignees
        task_entities_by_ftrack_id[ftrack_id] = task_entity

    return task_entities_by_ftrack_id


async def _prepare_product_entities(
    folder_entities_by_ftrack_id: dict[str, Any],
    asset_entities: list[dict[str, Any]],
    asset_types_by_id: dict[str, Any],
) -> dict[str, Any]:
    product_entities_by_ftrack_id = {}
    for asset_entity in asset_entities:
        ftrack_id = asset_entity["id"]
        parent_ftrack_id = asset_entity["context_id"]
        folder_entity = folder_entities_by_ftrack_id.get(parent_ftrack_id)
        if folder_entity is None:
            continue

        product_label = asset_entity["name"]
        product_name = slugify(product_label, "_")
        if product_name == product_label:
            product_label = None

        asset_type = asset_types_by_id[asset_entity["type_id"]]
        product_type = asset_type["short"]

        product_entities_by_ftrack_id[ftrack_id] = {
            "entity_id": uuid.uuid4().hex,
            "name": product_name,
            "label": product_label,
            "folderId": folder_entity["entity_id"],
            "productType": product_type,
        }

    return product_entities_by_ftrack_id


async def _prepare_version_entities(
    product_entities_by_ftrack_id: dict[str, Any],
    tasks_by_ftrack_id: dict[str, dict[str, Any]],
    ftrack_versions: list[dict[str, Any]],
    status_names_by_id: dict[str, str],
    attrs_mapping: CustomAttributesMapping,
    ayon_attr_by_name: dict[str, Any],
    attr_values_by_id: dict[str, list[dict[str, Any]]],
    thumbnails_mapping: dict[str, str],
) -> dict[str, dict[str, Any]]:
    version_entities = {}
    for asset_version in ftrack_versions:
        ftrack_id = asset_version["id"]
        asset_id = asset_version["asset_id"]
        task_id = asset_version["task_id"]
        product_entity = product_entities_by_ftrack_id.get(asset_id)
        if product_entity is None:
            continue
        task_entity = tasks_by_ftrack_id.get(task_id)
        task_id = None
        if task_entity:
            task_id = task_entity["entity_id"]

        attribs = {
            FTRACK_ID_ATTRIB: asset_version["id"],
            # TODO fill path?
            # FTRACK_PATH_ATTRIB: ftrack_path,
        }
        attr_values_by_attr_id = {
            attr_value["configuration_id"]: attr_value["value"]
            for attr_value in attr_values_by_id.get(ftrack_id) or []
        }
        for mapping_item in attrs_mapping.values():
            attr_conf = mapping_item.get_attr_conf_for_entity(asset_version)
            if attr_conf is None:
                continue
            dst_key = mapping_item.ayon_attribute_name
            value = await _convert_attrib_value(
                attr_values_by_attr_id.get(attr_conf["id"]),
                attr_conf,
                ayon_attr_by_name[dst_key]
            )
            if value is not None:
                attribs[dst_key] = value

        status_id = asset_version["status_id"]
        ayon_id = uuid.uuid4().hex
        if asset_version["thumbnail_id"]:
            thumbnails_mapping[ayon_id] = asset_version["thumbnail_id"]
        version_entities[ftrack_id] = {
            "entity_id": ayon_id,
            "version": asset_version["version"],
            "taskId": task_id,
            "productId": product_entity["entity_id"],
            "comment": asset_version["comment"],
            "status": status_names_by_id[status_id],
            "attrib": attribs,
        }

    return version_entities


class ActivitiesWrap:
    def __init__(self):
        self._activities = []
        self._activity_ids_mapping = {}
        self._metadata_by_id = {}

    def iter(self):
        for activity in self._activities:
            activity_id = activity["activity_id"]
            ftrack_id = self._activity_ids_mapping[activity_id]
            metadata = self._metadata_by_id.get(activity_id)
            yield activity, ftrack_id, metadata

    def add_activity(
        self,
        activity: dict[str, Any],
        ftrack_id: str,
        metadata: Optional[dict[str, Any]] = None,
    ):
        activity_id = activity["activity_id"]
        self._activities.append(activity)
        self._activity_ids_mapping[activity_id] = ftrack_id
        self._metadata_by_id[activity_id] = metadata


async def _prepare_activities(
    notes: list[dict[str, Any]],
    notes_metadata: dict[str, dict[str, Any]],
    task_entities_by_ftrack_id: dict[str, dict[str, Any]],
    version_entities_by_ftrack_id: dict[str, dict[str, Any]],
    default_username: str,
    users_mapping: dict[str, Union[str, None]],
) -> ActivitiesWrap:
    activities = ActivitiesWrap()
    for note in notes:
        user_id = note["user_id"]
        ayon_username = users_mapping.get(user_id)
        if ayon_username is None:
            ayon_username = default_username

        parent_ftrack_id = note["parent_id"]
        parent_entity = task_entities_by_ftrack_id.get(parent_ftrack_id)
        parent_entity_type = "task"
        if not parent_entity:
            parent_entity = version_entities_by_ftrack_id.get(
                parent_ftrack_id
            )
            parent_entity_type = "version"

        if not parent_entity:
            continue

        parent_ayon_id = parent_entity["entity_id"]

        activity_id = uuid.uuid4().hex
        note_id = note["id"]
        activities.add_activity(
            {
                "activity_id": activity_id,
                "activity_type": "comment",
                "parent_id": parent_ayon_id,
                "parent_type": parent_entity_type,
                "user_name": ayon_username,
                "body": note["content"],
                "timestamp": convert_ftrack_date_obj(note["date"]),
                "data": {"ftrack": {"id": note_id}},
            },
            note_id,
            notes_metadata.get(note_id)
        )
    return activities


class ComponentsInfo:
    def __init__(self):
        self._components_by_id = {}
        self._component_id_by_resource_id = collections.defaultdict(set)
        self._entity_ids_by_review_component = collections.defaultdict(set)
        self._entity_ids_by_thumbnail_component = collections.defaultdict(set)

    def get_component_ids(self) -> set[str]:
        return set(self._components_by_id)

    def get_resource_ids(self) -> set[str]:
        return set(self._component_id_by_resource_id)

    def get_component_by_id(
        self, component_id: str
    ) -> Optional[dict[str, Any]]:
        return self._components_by_id.get(component_id)

    def add_review_component(
        self,
        component: dict[str, Any],
        entity_ids: set[str],
    ):
        component_id = component["id"]
        self._components_by_id[component_id] = component
        self._entity_ids_by_review_component[component_id] |= entity_ids

    def add_thumbnail_component(
        self,
        component: dict[str, Any],
        entity_ids: set[str],
    ):
        component_id = component["id"]
        self._components_by_id[component_id] = component
        self._entity_ids_by_thumbnail_component[component_id] |= entity_ids

    def add_resource_id_mapping(self, component_id: str, resource_id: str):
        self._component_id_by_resource_id[resource_id].add(component_id)

    def to_data(self) -> dict[str, Any]:
        output = {}
        for resource_id in self.get_resource_ids():
            component_ids = self._component_id_by_resource_id[resource_id]
            component = next(
                (
                    self._components_by_id[component_id]
                    for component_id in component_ids
                ),
                None
            )
            if component is None:
                continue

            component_id = component["id"]
            output[resource_id] = {
                "ext": component["file_type"],
                "size": component["size"],
                "name": component["name"],
                "review_entities": (
                    self._entity_ids_by_review_component[component_id]
                ),
                "thumbnail_entities": (
                    self._entity_ids_by_thumbnail_component[component_id]
                ),
            }

        return output


async def _prepare_components(
    session: FtrackSession,
    version_ids: set[str],
    thumbnails_mapping: dict[str, str],
) -> dict[str, dict[str, Any]]:
    components_info = ComponentsInfo()
    review_names = join_filter_values(FTRACK_REVIEW_NAMES)
    for version_chunk_ids in create_chunks(version_ids):
        joined_version_chunk_ids = join_filter_values(version_chunk_ids)
        for component in await session.query(
            "select id, file_type, name, size, version_id from Component"
            f" where name in ({review_names})"
            f" and version_id in ({joined_version_chunk_ids})"
        ).all():
            components_info.add_review_component(
                component, {component["version_id"]}
            )

    thumbnail_ids = set(thumbnails_mapping.values())
    entity_ids_by_thumbnail_id = {
        thumbnail_id: set()
        for thumbnail_id in thumbnail_ids
    }
    for ayon_id, thumbnail_id in thumbnails_mapping.items():
        entity_ids_by_thumbnail_id[thumbnail_id].add(ayon_id)

    for thumbnail_chunk_ids in create_chunks(thumbnail_ids):
        joined_thumbnail_chunk_ids = join_filter_values(thumbnail_chunk_ids)
        thumbnail_components = await session.query(
            "select id, file_type, name, size from Component"
            f" where id in ({joined_thumbnail_chunk_ids})"
        ).all()
        for component in thumbnail_components:
            entity_ids = entity_ids_by_thumbnail_id[component["id"]]
            components_info.add_thumbnail_component(component, entity_ids)

    server_location = await session.query(
        "select id from Location where name is \"ftrack.server\""
    ).first()
    server_location_id = server_location["id"]

    for chunk_ids in create_chunks(components_info.get_component_ids()):
        joined_chunk_ids = join_filter_values(chunk_ids)
        component_locations = await session.query(
            "select component_id, resource_identifier"
            " from ComponentLocation"
            f" where location_id is {server_location_id}"
            f" and component_id in ({joined_chunk_ids})"
        ).all()
        for cl in component_locations:
            components_info.add_resource_id_mapping(
                cl["component_id"], cl["resource_identifier"]
            )

    components_data = components_info.to_data()
    for resource_id, resource_data in components_data.items():
        resource_data["url"] = session.get_url(resource_id)
    return components_data


async def _collect_project_data(
    session: FtrackSession,
    ftrack_project_name: str,
    default_username: str,
    users_mapping: dict[str, Union[str, None]],
    attrs_mapping: CustomAttributesMapping,
):
    """Collect data from ftrack and convert them to AYON data.

    Args:
        session (FtrackSession): ftrack session.
        ftrack_project_name (str): Name of the project.
        default_username (str): Default username used if user was not mapped.
        users_mapping (dict[str, Union[str, None]]): Mapping of user ids to
            AYON usernames.
        attrs_mapping (CustomAttributesMapping): Mapping of custom attributes.

    Returns:
        dict[str, Any]: Output contains project entity, folder entities
            and task entities. More might come in future (UPDATE).

    """
    ayon_attr_by_name = {
        attr["name"]: attr
        for attr in attribute_library.info_data
    }
    ftrack_project: FtrackEntityType = await session.query(
        "select id, full_name, name, thumbnail_id, project_schema_id"
        f" from Project where full_name is \"{ftrack_project_name}\""
    ).first()
    attr_confs_by_id: dict[str, FtrackEntityType] = {
        attr_conf["id"]: attr_conf
        for attr_conf in attrs_mapping.attr_confs
    }
    statuses: list[FtrackEntityType] = await session.query(
        "select id, name, color, sort, state from Status"
    ).all()
    status_names_by_id = {
        status["id"]: status["name"]
        for status in statuses
    }
    object_types: list[FtrackEntityType] = await session.query(
        "select id, name, sort from ObjectType"
    ).all()
    types: list[FtrackEntityType] = await session.query(
        "select id, name from Type"
    ).all()
    task_type_id: str = next(
        object_type["id"]
        for object_type in object_types
        if object_type["name"] == "Task"
    )
    project_id: str = ftrack_project["id"]
    typed_context_fields = ", ".join({
        "id",
        "name",
        "parent_id",
        "context_type",
        "object_type_id",
        "type_id",
        "status_id",
        "thumbnail_id",
        "description",
        "start_date",
        "end_date",
        "created_by_id",
    })
    # Store entity ids to query custom attribute values
    entity_ids = {ftrack_project["id"]}
    folder_src_entities = []
    task_src_entities = []
    for entity in await session.query(
        f"select {typed_context_fields}"
        f" from TypedContext where project_id is \"{project_id}\""
    ).all():
        entity["start_date"] = convert_ftrack_date(entity["start_date"])
        entity["end_date"] = convert_ftrack_date(entity["end_date"])
        entity_ids.add(entity["id"])
        if entity["object_type_id"] == task_type_id:
            task_src_entities.append(entity)
        else:
            folder_src_entities.append(entity)

    attr_values_by_id: dict[str, list[FtrackEntityType]] = (
        await _query_custom_attribute_values(
            session, entity_ids, set(attr_confs_by_id)
        )
    )

    asset_types_by_id = {
        asset_type["id"] : asset_type
        for asset_type in await session.query(
            "select id, name, short from AssetType"
        ).all()
    }
    assets = await session.query(
        "select id, context_id, name, type_id"
        f" from Asset where project_id is \"{project_id}\""
    ).all()
    versions = await session.query(
        "select id, version, asset_id, task_id, comment,"
        " status_id, thumbnail_id, user_id"
        f" from AssetVersion where project_id is \"{project_id}\""
    ).all()

    assignment_by_task_id = collections.defaultdict(set)
    task_ids = {
        task_entity["id"] for task_entity in task_src_entities
    }
    for task_ids_chunk in create_chunks(task_ids, 50):
        joined_task_ids = join_filter_values(task_ids_chunk)
        appointments = await session.query(
            "select resource_id, context_id from Appointment"
            f" where context_id in ({joined_task_ids})"
            " and type is 'assignment'"
        ).all()
        for appointment in appointments:
            task_id = appointment["context_id"]
            user_id = appointment["resource_id"]
            assignment_by_task_id[task_id].add(user_id)

    notes = []
    note_parent_ids = set(task_ids) | {version["id"] for version in versions}
    for entity_ids_chunk in create_chunks(note_parent_ids, 50):
        joined_parent_ids = join_filter_values(entity_ids_chunk)
        notes.extend(await session.query(
            "select id, content, date, parent_id, user_id, in_reply_to_id"
            " from Note"
            f" where parent_id in ({joined_parent_ids})"
        ).all())

    notes_metadata = {}
    note_ids = {note["id"] for note in notes}
    for note_ids_chunk in create_chunks(note_ids, 50):
        joined_note_ids = join_filter_values(note_ids_chunk)
        notes_metadata.update({
            metadata_item["parent_id"]: metadata_item
            for metadata_item in await session.query(
                "select key, parent_id, value from Metadata"
                " where key is 'ayon_activity_id'"
                f" and parent_id in ({joined_note_ids})"
            ).all()
        })

    # ftrack thumbnail id by AYON id
    thumbnails_mapping: dict[str, str] = {}

    project_entity: dict[str, Any] = await _prepare_project_entity(
        session,
        ftrack_project,
        statuses,
        object_types,
        types,
        attrs_mapping,
        ayon_attr_by_name,
        attr_values_by_id[ftrack_project["id"]],
        thumbnails_mapping,
    )

    folder_entities_by_ftrack_id: dict[str, Any] = (
        await _prepare_folder_entities(
            project_id,
            folder_src_entities,
            status_names_by_id,
            object_types,
            attrs_mapping,
            ayon_attr_by_name,
            attr_values_by_id,
            thumbnails_mapping,
        )
    )

    task_entities_by_ftrack_id: dict[str, dict[str, Any]] = (
        await _prepare_task_entities(
            folder_entities_by_ftrack_id,
            task_src_entities,
            status_names_by_id,
            types,
            attrs_mapping,
            ayon_attr_by_name,
            attr_values_by_id,
            assignment_by_task_id,
            users_mapping,
            thumbnails_mapping,
        )
    )

    product_entities_by_ftrack_id: dict[str, Any] = (
        await _prepare_product_entities(
            folder_entities_by_ftrack_id,
            assets,
            asset_types_by_id,
        )
    )

    version_entities_by_ftrack_id: dict[str, Any] =(
        await _prepare_version_entities(
            product_entities_by_ftrack_id,
            task_entities_by_ftrack_id,
            versions,
            status_names_by_id,
            attrs_mapping,
            ayon_attr_by_name,
            attr_values_by_id,
            thumbnails_mapping,
        )
    )
    activities = await _prepare_activities(
        notes,
        notes_metadata,
        task_entities_by_ftrack_id,
        version_entities_by_ftrack_id,
        default_username,
        users_mapping,
    )

    components: dict[str, dict[str, Any]] = await _prepare_components(
        session,
        set(version_entities_by_ftrack_id),
        thumbnails_mapping,
    )

    return {
        "project_code": ftrack_project["name"],
        "project": project_entity,
        "folders": list(folder_entities_by_ftrack_id.values()),
        "tasks": list(task_entities_by_ftrack_id.values()),
        "products": list(product_entities_by_ftrack_id.values()),
        "versions": list(version_entities_by_ftrack_id.values()),
        "components": components,
        "activities": activities,
    }


async def _import_comments(
    ayon_project_name: str,
    session: FtrackSession,
    activities: ActivitiesWrap,
):
    entity_by_id = {}
    async def _get_entity_obj(e_id, e_type):
        obj = entity_by_id.get(e_id)
        if obj is None:
            entity_class = get_entity_class(e_type)
            obj = await entity_class.load(ayon_project_name, e_id)
            entity_by_id[e_id] = obj
        return obj

    ftrack_batch_operations = []
    for activity, ftrack_id, metadata_item in activities.iter():
        entity_id = activity.pop("parent_id")
        entity_type = activity.pop("parent_type")
        activity_id = activity["activity_id"]
        entity = await _get_entity_obj(entity_id, entity_type)
        await create_activity(
            entity,
            **activity
        )
        if metadata_item:
            ftrack_batch_operations.append({
                "action": "update",
                "entity_data": {
                    "__entity_type__": "Metadata",
                    "value": activity_id,
                },
                "entity_key": [ftrack_id, "ayon_activity_id"],
                "entity_type": "Metadata",
            })
            continue

        ftrack_batch_operations.append({
            "action": "create",
            "entity_data": {
                "__entity_type__": "Metadata",
                "key": "ayon_activity_id",
                "parent_id": ftrack_id,
                "parent_type": "Note",
                "value": activity_id,
            },
            "entity_key": [ftrack_id, "ayon_activity_id"],
            "entity_type": "Metadata",
        })

    for operations_chunk in create_chunks(ftrack_batch_operations, 50):
        await session.call(operations_chunk)


async def _import_thumbnails(
    ayon_project_name: str,
    components: dict[str, dict[str, Any]],
):
    output = {}
    chunk_size = 512
    client = httpx.AsyncClient()
    for resource_id, component in components.items():
        thumbnail_entities = component["thumbnail_entities"]
        if not thumbnail_entities:
            continue
        resource_url = component["url"]
        response = await client.get(resource_url)
        download_url = response.headers["location"]
        stream = io.BytesIO()
        async with client.stream("GET", download_url) as response:
            async for chunk in response.aiter_bytes(chunk_size):
                stream.write(chunk)

        ext = component["ext"].lower()
        mime_type = None
        if ext in (".jpg", ".jpeg"):
            mime_type = "image/jpeg"
        elif ext == ".png":
            mime_type = "image/png"

        if not mime_type:
            logging.info(
                f"Skipping thumbnail component with extension: {ext}"
            )
            continue

        thumbnail_id = uuid.uuid4().hex
        # TODO get username somehow?
        # username = None
        await store_thumbnail(
            ayon_project_name,
            thumbnail_id,
            stream.getvalue(),
            mime=mime_type,
        )
        component["ayon_id"] = thumbnail_id
        for entity_id in thumbnail_entities:
            output[entity_id] = thumbnail_id
    return output


async def _import_project(
    session: FtrackSession,
    ftrack_project_name: str,
    default_username: str,
    users_mapping: dict[str, Union[str, None]],
    attrs_mapping: CustomAttributesMapping,
):
    """Sync ftrack project data to AYON.

    Args:
        ftrack_project_name (str): ftrack project name.
        session (FtrackSession): ftrack session.
        users_mapping (dict[str, Union[str, None]]): Mapping of ftrack user id
            to AYON username.
        attrs_mapping (CustomAttributesMapping): Mapping of ftrack attributes to
            AYON attributes.

    """
    ayon_project_name = ftrack_project_name
    if not re.match(PROJECT_NAME_REGEX, ayon_project_name):
        ayon_project_name = slugify(ayon_project_name, "_")

    try:
        # Project already exists -> skip
        _ = ProjectEntity.load(ayon_project_name)
        raise BadRequestException(
            f"Project '{ayon_project_name}' already exists"
        )

    except NotFoundException:
        pass
    data = await _collect_project_data(
        session,
        ftrack_project_name,
        default_username,
        users_mapping,
        attrs_mapping,
    )
    project_code = data["project_code"]

    await create_project_from_anatomy(
        ayon_project_name,
        project_code,
        Anatomy(**data["project"]),
    )

    thumbnail_ids_by_entity_id: dict[str, str] = (
        await _import_thumbnails(ayon_project_name, data["components"])
    )

    def _add_thumbnail(entity_data):
        entity_id = entity_data["entity_id"]
        thumbnail_id = thumbnail_ids_by_entity_id.get(entity_id)
        entity_data["thumbnailId"] = thumbnail_id

    operations = ProjectLevelOperations(ayon_project_name)
    for folder_entity in data["folders"]:
        _add_thumbnail(folder_entity)
        operations.create("folder", **folder_entity)

    for task_entity in data["tasks"]:
        _add_thumbnail(task_entity)
        operations.create("task", **task_entity)

    for product_entity in data["products"]:
        operations.create("product", **product_entity)

    for version_entity in data["versions"]:
        _add_thumbnail(version_entity)
        operations.create("version", **version_entity)

    await operations.process()

    await _import_comments(ayon_project_name, session, data["activities"])

    # components: dict[str, dict[str, Any]] = data["components"]
    # for resource_id, component in components.items():
    #     THIS IS EXAMPLE CONTENT
    #     component = {
    #         "ext": component["file_type"],
    #         "size": component["size"],
    #         "name": component["name"],
    #         "review_entities": set[str],  # ayon ids
    #         "thumbnail_entities": set[str],  # ayon ids
    #     }


def _map_ftrack_users_to_ayon_users(
    ftrack_users: list[dict[str, Any]],
    ayon_users: list[dict[str, Any]],
) -> dict[str, Union[str, None]]:
    """Map ftrack users to AYON users.

    Mapping is based on 2 possible keys, email and username where email has
    higher priority. Once AYON user is mapped it cannot be mapped again to
    different user.

    Fields used from ftrack users: 'id', 'username', 'email'.

    Args:
        ftrack_users (List[ftrack_api.entity.user.User]): List of ftrack users.
        ayon_users (List[Dict[str, Any]]): List of AYON users.

    Returns:
        Dict[str, Union[str, None]]: Mapping of ftrack user id
            to AYON username.

    """
    mapping: dict[str, Union[str, None]] = {
        user["id"]: None
        for user in ftrack_users
    }
    ayon_users_by_email: dict[str, str] = {}
    ayon_users_by_name: dict[str, str] = {}
    for ayon_user in ayon_users:
        ayon_name = ayon_user["name"]
        ayon_email = ayon_user["attrib"]["email"]
        ayon_users_by_name[ayon_name.lower()] = ayon_name
        if ayon_email:
            ayon_users_by_email[ayon_email.lower()] = ayon_name

    mapped_ayon_users: set[str] = set()
    for ftrack_user in ftrack_users:
        ftrack_id: str = ftrack_user["id"]
        # Make sure username does not contain '@' character
        ftrack_name: str = ftrack_user["username"].split("@", 1)[0]
        ftrack_email: str = ftrack_user["email"]

        if ftrack_email and ftrack_email.lower() in ayon_users_by_email:
            ayon_name: str = ayon_users_by_email[ftrack_email.lower()]
            if ayon_name not in mapped_ayon_users:
                mapping[ftrack_id] = ayon_name
                mapped_ayon_users.add(ayon_name)
            continue

        if ftrack_name in ayon_users_by_name:
            ayon_name: str = ayon_users_by_name[ftrack_name]
            if ayon_name not in mapped_ayon_users:
                mapped_ayon_users.add(ayon_name)
                mapping[ftrack_id] = ayon_name

    return mapping


def _calculate_default_access_groups(
    ftrack_projects: list[dict[str, Any]],
    project_names: set[str],
    user_security_roles: list[dict[str, Any]],
    project_roles_by_id: dict[str, list[dict[str, Any]]],
    access_groups: set[str],
):
    available_project_names = []

    allow_public_projects = any(
        user_security_role["is_all_projects"]
        for user_security_role in user_security_roles
    )
    project_ids = set()
    for user_security_role in user_security_roles:
        # QUESTION: Maybe we should check what role it is?
        role_id = user_security_role["id"]
        project_roles = project_roles_by_id.get(role_id, [])
        for project_role in project_roles:
            project_ids.add(project_role["project_id"])

    for ftrack_project in ftrack_projects:
        project_name = ftrack_project["full_name"]
        # Skip projects that are not in AYON
        if project_name not in project_names:
            continue

        # Public project
        if not ftrack_project["is_private"]:
            # Add access if user has access to all public projects
            if allow_public_projects:
                available_project_names.append(project_name)
            continue

        if ftrack_project["id"] in project_ids:
            available_project_names.append(project_name)

    return {
        project_name: list(access_groups)
        for project_name in available_project_names
    }


async def _prepare_users_mapping(
    session: FtrackSession
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, UserEntity],
    dict[str, Union[str, None]]
]:
    valid_ftrack_user_type_ids = {
        user_type["id"]
        for user_type in await session.query(
            "select id, name from UserType"
        ).all()
        # Ignore services and demo users
        if user_type["name"] not in ("service", "demo")
    }
    fields = {
        "id",
        "username",
        "is_active",
        "email",
        "first_name",
        "last_name",
        "user_type_id",
        # "resource_type",
        # "thumbnail_id",
        # "thumbnail_url",
    }
    joined_fields = ", ".join(fields)
    ftrack_users = [
        user
        for user in await session.query(
            f"select {joined_fields} from User"
        ).all()
        if user["user_type_id"] in valid_ftrack_user_type_ids
    ]
    ftrack_users_by_id = {
        ftrack_user["id"]: ftrack_user
        for ftrack_user in ftrack_users
    }

    ayon_users_by_name = {
        row["name"]: UserEntity.from_record(row)
        async for row in Postgres.iterate("SELECT * FROM users")
    }
    users_mapping: dict[str, Union[str, None]] = (
        _map_ftrack_users_to_ayon_users(
            ftrack_users,
            [user.dict() for user in ayon_users_by_name.values()]
        )
    )
    return ftrack_users_by_id, ayon_users_by_name, users_mapping


async def _attribute_needs_update(ftrack_attrs, ayon_attr) -> bool:
    for ftrack_attr in ftrack_attrs:
        if ftrack_attr["type"] != ayon_attr["type"]:
            return True

        if ftrack_attr["type"] == "string":
            if ftrack_attr["default"] != ayon_attr["default"]:
                return True
            continue

        if ftrack_attr["type"] == "number":
            if ftrack_attr["min"] != ayon_attr["min"]:
                return True
            if ftrack_attr["max"] != ayon_attr["max"]:
                return True
            continue

        if ftrack_attr["type"] == "boolean":
            if ftrack_attr["default"] != ayon_attr["default"]:
                return True
            continue

        if ftrack_attr["type"] == "enum":
            if ftrack_attr["values"] != ayon_attr["values"]:
                return True
            continue

        if ftrack_attr["type"] == "date":
            if ftrack_attr["default"] != ayon_attr["default"]:
                return True
            continue

        if ftrack_attr["type"] == "json":
            if ftrack_attr["default"] != ayon_attr["default"]:
                return True
            continue
    return False


_ATTR_TYPE_MAPPING = {
    "text": {"string"},
    "date": {"datetime"},
    "number": {"integer", "float"},
    "boolean": {"boolean"},
    "enumerator": {"string", "list_of_strings", "list_of_integers"},
}


async def _get_scope_n_inherit(
    attr_confs: list[dict[str, Any]],
    object_type_by_id: dict[str, str],
) -> tuple[set[str], bool]:
    # TODO implement
    inherit = False
    scope = set()
    for attr_conf in attr_confs:
        if attr_conf["is_hierarchical"]:
            inherit = True
            scope |= {"project", "folder", "task", "product", "version"}
            break

        entity_type = attr_conf["entity_type"]
        if entity_type == "show":
            scope.add("project")

        elif entity_type == "asset":
            scope.add("product")

        elif entity_type == "user":
            scope.add("user")

        elif entity_type == "task":
            object_type_id = attr_conf["object_type_id"]
            object_name = object_type_by_id[object_type_id]
            if object_name == "Task":
                scope.add("task")
            else:
                scope.add("folder")

        else:
            key = attr_conf["key"]
            logging.info(
                f"Unknown entity type '{entity_type}'"
                f" on custom attribute '{key}'"
            )

    return scope, inherit


async def _create_ftrack_addon_attributes(
    ayon_attr_by_name: dict[str, Any],
    position: int,
):
    ftrack_id_attribute_data = {
        "type": "string",
        "title": "ftrack id",
        "inherit": False,
    }
    ftrack_path_attribute_data = {
        "type": "string",
        "title": "ftrack path",
        "inherit": False,
    }
    ftrack_id_expected_scope = ["project", "folder", "task", "version"]
    ftrack_path_expected_scope = ["project", "folder", "task"]

    id_attr = ayon_attr_by_name.get(FTRACK_ID_ATTRIB)
    path_attr = ayon_attr_by_name.get(FTRACK_PATH_ATTRIB)

    if id_attr is None:
        id_update_needed = True
        id_attr_position = position
        position += 1
        id_attr_data = ftrack_id_attribute_data
    else:
        id_attr_position = id_attr["position"]
        id_update_needed = False
        if set(id_attr["scope"]) != set(ftrack_id_expected_scope):
            id_update_needed = True
        id_attr_data = id_attr["data"]
        for key, value in ftrack_id_attribute_data.items():
            if id_attr_data.get(key) != value:
                id_update_needed = True
                id_attr_data[key] = value

    if path_attr is None:
        path_update_needed = True
        path_attr_position = position
        position += 1
        path_attr_data = ftrack_path_attribute_data
    else:
        path_attr_position = path_attr["position"]
        path_update_needed = False
        if set(path_attr["scope"]) != set(ftrack_path_expected_scope):
            path_update_needed = True
        path_attr_data = path_attr["data"]
        for key, value in ftrack_path_attribute_data.items():
            if path_attr_data.get(key) != value:
                path_update_needed = True
                path_attr_data[key] = value

    postgre_query = "\n".join((
        "INSERT INTO public.attributes",
        "    (name, position, scope, data)",
        "VALUES",
        "    ($1, $2, $3, $4)",
        "ON CONFLICT (name)",
        "DO UPDATE SET",
        "    scope = $3,",
        "    data = $4",
    ))
    if id_update_needed:
        await Postgres.execute(
            postgre_query,
            FTRACK_ID_ATTRIB,
            id_attr_position,
            ftrack_id_expected_scope,
            id_attr_data,
        )

    if path_update_needed:
        await Postgres.execute(
            postgre_query,
            FTRACK_PATH_ATTRIB,
            path_attr_position,
            ftrack_path_expected_scope,
            path_attr_data,
        )

    return id_update_needed or path_update_needed


async def _create_attribute(
    ftrack_key: str,
    ftrack_attr_confs: list[dict[str, Any]],
    type_names_by_id: dict[str, str],
    ftrack_object_types: list[FtrackEntityType],
    position: int,
):
    object_type_by_id = {
        object_type["id"]: object_type["name"]
        for object_type in ftrack_object_types
    }
    ftrack_confs = _find_best_attr_conf(ftrack_attr_confs)
    scope, inherit = _get_scope_n_inherit(
        ftrack_confs, object_type_by_id
    )
    first_attr = ftrack_confs[0]
    attr_type_name = type_names_by_id[first_attr["type_id"]]

    ayon_attr_data = {
        "title": ftrack_key,
        "description": None,
        "example": None,
        "default": None,
        "gt": None,
        "lt": None,
        "ge": None,
        "le": None,
        "min_length": None,
        "max_length": None,
        "min_items": None,
        "max_items": None,
        "regex": None,
        "enum": None,
        "inherit": inherit,
    }
    ayon_attr_type = None
    if attr_type_name == "boolean":
        ayon_attr_type = "boolean"
    elif attr_type_name == "text":
        ayon_attr_type = "string"
    elif attr_type_name == "date":
        ayon_attr_type = "datetime"
    elif attr_type_name == "enumerator":
        config = json.loads(first_attr["config"])
        if config["multiSelect"]:
            ayon_attr_type = "list_of_strings"
        else:
            ayon_attr_type = "string"
        ayon_attr_data["enum"] = [
            {
                "value": item["value"],
                "label": item["menu"],
            }
            for item in json.loads(config["data"])
        ]
    elif attr_type_name == "number":
        config = json.loads(first_attr["config"])
        ayon_attr_type = "integer"
        if config.get("isdecimal", False):
            ayon_attr_type = "float"

    if ayon_attr_type is None:
        return False

    ayon_attr_data["type"] = ayon_attr_type

    ayon_attr = {
        "name": ftrack_key,
        "position": position,
        "scope": list(scope),
        "data": ayon_attr_data,
    }
    await Postgres.execute(
        """
        INSERT INTO public.attributes
            (name, position, scope, data)
        VALUES
            ($1, $2, $3, $4)
        ON CONFLICT (name) DO UPDATE
        SET
            position = EXCLUDED.position,
            scope = EXCLUDED.scope,
            builtin = EXCLUDED.builtin,
            data = EXCLUDED.data
        """,
        ftrack_key,
        position,
        list(scope),
        ayon_attr,
    )
    return True


async def _udpdate_attribute(
    ftrack_attrs: list[FtrackEntityType],
    ftrack_type_names_by_id: dict[str, str],
    ftrack_object_types: list[FtrackEntityType],
    ayon_attr: dict[str, Any]
):
    if ayon_attr["builtin"]:
        return False

    object_type_by_id = {
        object_type["id"]: object_type["name"]
        for object_type in ftrack_object_types
    }
    ftrack_confs = _find_best_attr_conf(ftrack_attrs)
    extracted_scope, inherit = _get_scope_n_inherit(
        ftrack_confs, object_type_by_id
    )

    attr_name = ayon_attr["name"]
    ayon_attr_data = ayon_attr["data"]
    ayon_type = ayon_attr_data["type"]
    ayon_title = ayon_attr_data["title"]

    # NOTE Missing updates
    # - default
    first_attr = ftrack_confs[0]

    scope = ayon_attr["scope"]
    attr_scope = set(scope)
    new_scope = attr_scope | extracted_scope
    changed = False
    if len(attr_scope) != len(new_scope):
        scope = list(new_scope)
        changed = True

    if ayon_title != first_attr["label"]:
        changed = True
        ayon_attr_data["title"] = first_attr["label"]

    type_name = ftrack_type_names_by_id[first_attr["type_id"]]
    if (
        type_name == "enumerator"
        and ayon_type in {"list_of_strings", "string"}
    ):
        enum_items = json.loads(first_attr["confis"]["data"])
        # Mapping to AYON builtin enumerator might be potential danger
        new_enum = [
            {
                "value": item["value"],
                "label": item["menu"],
            }
            for item in enum_items
        ]
        if new_enum != ayon_attr_data.get("enum"):
            ayon_attr_data["enum"] = new_enum
            changed = True

    if not changed:
        return False

    await Postgres.execute(
        """
        INSERT INTO public.attributes
            (name, position, scope, data)
        VALUES
            ($1, $2, $3, $4)
        ON CONFLICT (name) DO UPDATE
        SET
            position = EXCLUDED.position,
            scope = EXCLUDED.scope,
            builtin = EXCLUDED.builtin,
            data = EXCLUDED.data
        """,
        attr_name,
        ayon_attr["position"],
        scope,
        ayon_attr_data,
    )
    return True


    # --- AYON attr def ---
    # {
    #     "name": "default",
    #     "scope": ["project", "folder", "task", "product", "version", "representation", "workfile", "user"],
    #     "position": 1,
    #     "builtin": True,
    #     "data": {...},
    # }
    #
    # --- AYON attr def.data ---
    # "type"
    # "title"
    # "default",
    # "example",
    # "regex",
    # "description",
    # "gt",
    # "lt",
    # "inherit",
    #
    # --- AYON attr def.data.type ---
    # "string",
    # "integer",
    # "float",
    # "boolean",
    # "datetime",
    # "list_of_strings",
    # "list_of_integers",

    # --- ftrack conf types ---
    # "text"
    # "enumerator"
    # "date"
    # "number"
    # "boolean"
    # for ftrac_attr in ftrack_attrs:


async def create_update_attributes(
    session: FtrackSession,
    attributes_mapping: dict[str, str],
) -> dict[str, Any]:
    attr_confs: list[FtrackEntityType] = await session.query(
        "select id, key, entity_type, object_type_id, is_hierarchical,"
        " default, type_id from CustomAttributeConfiguration"
    ).all()
    ftrack_object_types: list[FtrackEntityType] = await session.query(
        "select id, name, sort from ObjectType"
    ).all()
    type_names_by_id = await _get_supported_attribute_types(session)
    ayon_attr_by_name = {
        attr["name"]: attr
        for attr in attribute_library.info_data
    }
    ftrack_attrs_by_name = collections.defaultdict(list)
    for attr_conf in attr_confs:
        if attr_conf["type_id"] in type_names_by_id:
            key = attr_conf["key"]
            ftrack_attrs_by_name[key].append(attr_conf)

    position = max(
        attr["position"]
        for attr in attribute_library.info_data
    ) + 1
    restart_required = False
    for ftrack_key, ayon_key in attributes_mapping.items():
        if ayon_key == SKIP_ITEM:
            continue

        ftrack_attr_confs = ftrack_attrs_by_name[ftrack_key]
        if not ftrack_attr_confs:
            logging.warning(
                f"Failed to find ftrack custom attribute '{ftrack_key}'."
            )
            continue

        if ayon_key == CREATE_ITEM:
            if await _create_attribute(
                ftrack_key,
                ftrack_attr_confs,
                type_names_by_id,
                ftrack_object_types,
                position,
            ):
                restart_required = True
            position += 1
            continue

        ayon_attr = ayon_attr_by_name.get(ftrack_key)
        if ayon_attr is not None:
            if await _udpdate_attribute(
                ftrack_attr_confs,
                type_names_by_id,
                ftrack_object_types,
                ayon_attr
            ):
                restart_required = True

    if await _create_ftrack_addon_attributes(ayon_attr_by_name, position):
        restart_required = True

    return {
        "restartRequired": restart_required,
    }


async def _import_users(session) -> dict[str, Union[str, None]]:
    (
        ftrack_users_by_id,
        ayon_users_by_name,
        users_mapping
    ) = await _prepare_users_mapping(session)
    security_roles_by_id: dict[str, dict[str, Any]] = {
        role["id"]: role
        for role in await session.query(
            "select id, name, type from SecurityRole"
        ).all()
    }
    ayon_role_by_user_id: dict[str, str] = {
        ftrack_id: "artist"
        for ftrack_id in ftrack_users_by_id
    }
    user_roles_by_user_id: dict[str, list[dict[str, Any]]] = {
        ftrack_id: []
        for ftrack_id in ftrack_users_by_id
    }
    project_role_ids: set[str] = set()
    for user_security_role in await session.query(
        "select is_all_projects, is_all_open_projects"
        ", security_role_id, user_id"
        " from UserSecurityRole"
    ).all():
        role: dict[str, Any] = (
            security_roles_by_id[user_security_role["security_role_id"]]
        )
        user_id: str = user_security_role["user_id"]
        # Ignore users that are not 'ftrack' users
        if user_id not in ftrack_users_by_id:
            continue

        user_roles_by_user_id.setdefault(user_id, []).append(
            user_security_role
        )
        if not user_security_role["is_all_projects"]:
            project_role_ids.add(user_security_role["id"])
            continue

        # Mark user as admin
        if role["name"] == "Administrator":
            ayon_role_by_user_id[user_id] = "admin"
            continue

        # Make sure that user which was already marked with previous role
        #   as admin is not downgraded
        current_role = ayon_role_by_user_id[user_id]
        if role["name"] == "Project Manager" and current_role != "admin":
            ayon_role_by_user_id[user_id] = "manager"

    project_roles_by_id = {}
    for chunk in create_chunks(project_role_ids):
        project_role_ids = join_filter_values(chunk)
        for project_role in await session.query(
            "select id, project_id, user_security_role_id"
            " from UserSecurityRoleProject"
            f" where user_security_role_id in ({project_role_ids})"
        ).all():
            role_id = project_role["user_security_role_id"]
            project_roles_by_id.setdefault(role_id, []).append(
                project_role
            )

    access_groups = set()
    for ag_key, _ in AccessGroups.access_groups.items():
        access_group_name, pname = ag_key
        if pname == "_":
            access_groups.add(access_group_name)

    project_names = {
        row["name"]
        async for row in Postgres.iterate("SELECT name FROM projects")
    }

    ftrack_projects: list[dict[str, Any]] = await session.query(
        "select id, full_name, is_private from Project"
    ).all()
    for ftrack_id, ayon_user_name in users_mapping.items():
        ftrack_user = ftrack_users_by_id[ftrack_id]
        ftrack_username = ftrack_user["username"].split("@", 1)[0]
        logging.debug(f"Processing ftrack user {ftrack_username}")

        ayon_user_data = {
            "ftrack": {
                "id": ftrack_id,
                "username": ftrack_user["username"],
            }
        }

        ayon_role = ayon_role_by_user_id[ftrack_id]

        attrib = {}
        if ftrack_user["email"]:
            attrib["email"] = ftrack_user["email"]

        full_name_items = []
        for key in "first_name", "last_name":
            value = ftrack_user[key]
            if value:
                full_name_items.append(value)

        if full_name_items:
            attrib["fullName"] = " ".join(full_name_items)

        is_admin = ayon_role == "admin"
        is_manager = ayon_role == "manager"
        if not ayon_user_name:
            logging.debug("User does not exist in AYON yet, creating...")
            ayon_user_data["isAdmin"] = is_admin
            ayon_user_data["isManger"] = is_manager
            # Create new user
            if not is_admin and not is_manager:
                # TODO use predefined endpoints (are not available
                #   at the moment of this PR)
                ayon_user_data["defaultAccessGroups"] = list(
                    access_groups
                )
                ayon_user_data["accessGroups"] = (
                    _calculate_default_access_groups(
                        ftrack_projects,
                        project_names,
                        user_roles_by_user_id[ftrack_id],
                        project_roles_by_id,
                        access_groups,
                    )
                )

            new_ayon_user = {
                "name": ftrack_username,
                "active": ftrack_user["is_active"],
                "data": ayon_user_data,
            }
            if attrib:
                new_ayon_user["attrib"] = attrib
            ayon_user = UserEntity(new_ayon_user)
            await ayon_user.save()
            ayon_users_by_name[ayon_user.name] = ayon_user
            continue

        logging.debug(
            f"Mapped ftrack user {ftrack_username}"
            f" to AYON user {ayon_user_name}, updating..."
        )
        # Fetch user with REST to get 'data'
        ayon_user = ayon_users_by_name[ayon_user_name]
        if ftrack_user["is_active"] != ayon_user.active:
            ayon_user.active = ftrack_user["is_active"]

        # Comapre 'data' field
        current_user_data = ayon_user.data
        if "ftrack" in current_user_data:
            ayon_user_ftrack_data = current_user_data["ftrack"]
            for key, value in ayon_user_data["ftrack"].items():
                if (
                    key not in ayon_user_ftrack_data
                    or ayon_user_ftrack_data[key] != value
                ):
                    ayon_user_ftrack_data.update(ayon_user_data["ftrack"])
                    current_user_data["ftrack"] = ayon_user_ftrack_data
                    break

        if ayon_role == "admin":
            if not current_user_data.get("isAdmin"):
                current_user_data["isAdmin"] = True
                if current_user_data.get("isManger"):
                    current_user_data["isManger"] = False

        elif ayon_role == "manager":
            if not current_user_data.get("isManger"):
                current_user_data["isManger"] = True
                if current_user_data.get("isAdmin"):
                    current_user_data["isAdmin"] = False

        elif ayon_role == "artist":
            became_artist = False
            if current_user_data.get("isAdmin"):
                became_artist = True
                current_user_data["isAdmin"] = False

            if current_user_data.get("isManger"):
                became_artist = True
                current_user_data["isManger"] = False

            # User will become artist and we need to update access groups
            if became_artist:
                current_user_data["defaultAccessGroups"] = list(
                    access_groups
                )
                current_user_data["accessGroups"] = (
                    _calculate_default_access_groups(
                        ftrack_projects,
                        project_names,
                        user_roles_by_user_id[ftrack_id],
                        project_roles_by_id,
                        access_groups,
                    )
                )

        # Compare 'attrib' fields
        for key, value in attrib.items():
            if not hasattr(ayon_user.attrib, key):
                continue

            if getattr(ayon_user.attrib, key) != value:
                setattr(ayon_user.attrib, key, value)

        await ayon_user.save()

    return users_mapping


async def import_projects(
    session: FtrackSession,
    default_username: str,
    attributes_mapping: dict[str, str],
    project_names: list[str],
):
    """Import ftrack projects to AYON.

    Args:
        session (FtrackSession): ftrack session.
        default_username (str): Default username used for unmapped users.
        attributes_mapping (dict[str, str]): Mapping of ftrack attributes to
            AYON attributes.
        project_names (List[str]): List of ftrack project names.

    """
    users_mapping = await _import_users(session)
    attrs_mapping: CustomAttributesMapping = (
        await _get_custom_attributes_mapping(
            session, attributes_mapping
        )
    )
    for project_name in project_names:
        await _import_project(
            session,
            project_name,
            default_username,
            users_mapping,
            attrs_mapping,
        )
