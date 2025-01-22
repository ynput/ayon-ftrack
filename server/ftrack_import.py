# TODO what to do if project already exists in AYON?
# - Probably delete it? But there is missing api function for that.
# TODO Better handling of invalid characters in names
#    Folder and task name, maybe even type names?
# TODO Define default username for not mapped users for entity creation
#    and comments.
# TODO Create entities as users who created them in ftrack.
# TODO Figure out how to do custom attributes mapping
#    Right now a sync mapping was copied here, but that is code duplication
#    and is based on addon settings -> we might need to ask in dialog?
# TODO Make sure 'FTRACK_ID_ATTRIB' and 'FTRACK_PATH_ATTRIB'
#   do exist in AYON, or do not set them.
# TODO Make sure ftrack custom attributes contains mandatory ftrack
#   attributes, or do not set them.
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
from ayon_server.entities import UserEntity
from ayon_server.entities.core import attribute_library
from ayon_server.helpers.thumbnails import store_thumbnail
from ayon_server.helpers.deploy_project import create_project_from_anatomy
from ayon_server.helpers.get_entity_class import get_entity_class
from ayon_server.operations import ProjectLevelOperations
from ayon_server.types import PROJECT_NAME_REGEX

from .constants import (
    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
    CUST_ATTR_GROUP,
)
from .ftrack_session import (
    FtrackSession,
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


class MappedAYONAttribute:
    def __init__(
        self,
        ayon_attribute_name: str,
        is_hierarchical: bool = True,
        attr_confs: Optional[list[FtrackEntityType]] = None,
    ):
        self.ayon_attribute_name: str = ayon_attribute_name
        self.is_hierarchical: bool = is_hierarchical
        if attr_confs is None:
            attr_confs = []
        self._attr_confs: list[FtrackEntityType] = attr_confs

    def has_confs(self) -> bool:
        return bool(self.attr_confs)

    def add_attr_conf(self, attr_conf: FtrackEntityType):
        self._attr_confs.append(attr_conf)

    def get_attr_confs(self) -> list[FtrackEntityType]:
        return list(self._attr_confs)

    attr_confs: list[FtrackEntityType] = property(get_attr_confs)

    def get_attr_conf_for_entity_type(
        self, entity_type: str, object_type_id: Optional[str]
    ) -> Optional[FtrackEntityType]:
        if not self.attr_confs:
            return None
        if self.is_hierarchical:
            return self.attr_confs[0]

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
    def __init__(self):
        self._items: dict[str, MappedAYONAttribute] = {}

    def __contains__(self, item):
        return item in self._items

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


def get_custom_attributes_mapping(
    attr_confs: list[FtrackEntityType],
    addon_settings: dict[str, Any],
) -> CustomAttributesMapping:
    """Query custom attribute configurations from ftrack server.

    Returns:
        Dict[str, List[object]]: ftrack custom attributes.

    """
    # TODO this is not available yet in develop
    attributes_mapping = addon_settings.get("custom_attributes", {}).get("attributes_mapping", {})
    ayon_attribute_names = set()
    builtin_attributes_names = set()
    for attr in attribute_library.info_data:
        ayon_attribute_names.add(attr["name"])
        if attr["builtin"]:
            builtin_attributes_names.add(attr["name"])

    hier_attrs = []
    nonhier_attrs = []
    for attr_conf in attr_confs:
        if attr_conf["is_hierarchical"]:
            hier_attrs.append(attr_conf)
        else:
            nonhier_attrs.append(attr_conf)

    output = CustomAttributesMapping()
    if not attributes_mapping.get("enabled"):
        for attr_conf in hier_attrs:
            attr_name = attr_conf["key"]
            # Use only AYON attribute hierarchical equivalent
            if (
                attr_name in output
                or attr_name not in ayon_attribute_names
            ):
                continue

            # Attribute must be in builtin attributes or openpype/ayon group
            # NOTE get rid of group name check when only mapping is used
            if (
                attr_name in builtin_attributes_names
                or attr_conf["group"]["name"] in ("openpype", CUST_ATTR_GROUP)
            ):
                output.add_mapping_item(MappedAYONAttribute(
                    attr_name,
                    True,
                    [attr_conf],
                ))

    else:
        for item in attributes_mapping["mapping"]:
            ayon_attr_name = item["name"]
            if ayon_attr_name not in ayon_attribute_names:
                continue

            is_hierarchical = item["attr_type"] == "hierarchical"

            mapped_item = MappedAYONAttribute(
                ayon_attr_name, is_hierarchical, []
            )

            if is_hierarchical:
                attr_name = item["hierarchical"]
                for attr_conf in hier_attrs:
                    if attr_conf["key"] == attr_name:
                        mapped_item.add_attr_conf(attr_conf)
                        break
            else:
                attr_names = item["standard"]
                for attr_conf in nonhier_attrs:
                    if attr_conf["key"] in attr_names:
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
        value = attr_values_by_attr_id.get(attr_conf_id)
        dst_key = mapping_item.ayon_attribute_name
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
                value = attr_values_by_attr_id.get(attr_conf["id"])
                if value is not None:
                    dst_key = mapping_item.ayon_attribute_name
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
            value = attr_values_by_attr_id.get(attr_conf["id"])
            if value is not None:
                dst_key = mapping_item.ayon_attribute_name
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
            value = attr_values_by_attr_id.get(attr_conf["id"])
            if value is not None:
                dst_key = mapping_item.ayon_attribute_name
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
    users_mapping
) -> ActivitiesWrap:
    activities = ActivitiesWrap()
    default_username = None
    for note in notes:
        user_id = note["user_id"]
        ayon_username = users_mapping.get(user_id)
        if ayon_username is None:
            ayon_username = default_username

        if ayon_username is None:
            continue

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
    studio_settings: dict[str, Any],
):
    """Collect data from ftrack and convert them to AYON data.

    Args:
        session (FtrackSession): ftrack session.
        ftrack_project_name (str): Name of the project.
        studio_settings (dict[str, Any]): Studio settings.

    Returns:
        dict[str, Any]: Output contains project entity, folder entities
            and task entities. More might come in future (UPDATE).

    """
    ftrack_project: FtrackEntityType = await session.query(
        "select id, full_name, name, thumbnail_id, project_schema_id"
        f" from Project where full_name is \"{ftrack_project_name}\""
    ).first()
    attr_confs: list[FtrackEntityType] = await session.query(
        "select id, key, entity_type, object_type_id, is_hierarchical,"
        " default, type_id from CustomAttributeConfiguration"
    ).all()
    attr_confs_by_id: dict[str, FtrackEntityType] = {
        attr_conf["id"]: attr_conf
        for attr_conf in attr_confs
    }
    attrs_mapping: CustomAttributesMapping = get_custom_attributes_mapping(
        attr_confs, studio_settings
    )
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
        attr_values_by_id[ftrack_project["id"]],
        thumbnails_mapping,
    )

    (
        ftrack_users_by_id,
        ayon_users_by_name,
        users_mapping
    ) = await _prepare_users_mapping(session)

    folder_entities_by_ftrack_id: dict[str, Any] = (
        await _prepare_folder_entities(
            project_id,
            folder_src_entities,
            status_names_by_id,
            object_types,
            attrs_mapping,
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
            attr_values_by_id,
            thumbnails_mapping,
        )
    )
    activities = await _prepare_activities(
        notes,
        notes_metadata,
        task_entities_by_ftrack_id,
        version_entities_by_ftrack_id,
        users_mapping
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


async def import_project(
    ftrack_project_name: str,
    session: FtrackSession,
    studio_settings: dict[str, Any],
):
    """Sync ftrack project data to AYON.

    Args:
        ftrack_project_name (str): ftrack project name.
        session (FtrackSession): ftrack session.
        studio_settings (dict[str, Any]): Studio settings.

    """
    ayon_project_name = ftrack_project_name
    if not re.match(PROJECT_NAME_REGEX, ayon_project_name):
        ayon_project_name = slugify(ayon_project_name, "_")

    # Missing delete project api function
    # - this implementation does not handle storage files, user permissions
    #   etc.
    # try:
    #     project_entity = ProjectEntity.load(ayon_project_name)
    #     logging.warning(
    #         f"Project '{ayon_project_name}' already exists, replacing it."
    #     )
    #     await project_entity.delete()
    # except NotFoundException:
    #     pass
    data = await _collect_project_data(
        session, ftrack_project_name, studio_settings,
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


async def import_users(session):
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
        user_diffs = {}
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

        ayon_user.save()
