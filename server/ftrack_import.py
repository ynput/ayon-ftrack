# TODO what to do if project already exists in AYON?
# TODO sync users first to keep assignments
# TODO sync assignments
# TODO better handling of invalid characters in names
#    Project, folder and task name, maybe even type names?
# TODO figure out how to do custom attributes mapping
#    Right now a sync mapping was copied here, but that is code duplication
#    and is based on addon settings.
# TODO custom attributes mapping could be shared from common?
# TODO make sure 'FTRACK_ID_ATTRIB' and 'FTRACK_PATH_ATTRIB'
#   do exist in AYON
# TODO make sure ftrack custom attributes contains mandatory ftrack
#   attributes
# TODO actually sync data to AYON
# TODO sync products and versions
import re
import uuid
import json
import collections
from typing import Any, Optional

from nxtools import slugify

from ayon_server.lib.postgres import Postgres
from ayon_server.settings.anatomy import Anatomy
from ayon_server.entities.core import attribute_library
from ayon_server.helpers.deploy_project import create_project_from_anatomy
from ayon_server.operations import ProjectLevelOperations

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
        # TODO fill attributes
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

        ayon_id = uuid.uuid4().hex
        if ftrack_entity["thumbnail_id"]:
            thumbnails_mapping[ayon_id] = ftrack_entity["thumbnail_id"]

        task_entities_by_ftrack_id[ftrack_id] = {
            "entity_id": ayon_id,
            "name": task_name,
            "label": task_label,
            "folderId": ayon_parent["entity_id"],
            "taskType": task_type,
            "status": status,
            "attrib": attribs,
        }

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

        product_type = asset_types_by_id[asset_entity["type_id"]]["name"]

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
        version_entities[ftrack_id] = {
            "id": uuid.uuid4().hex,
            "version": asset_version["version"],
            "taskId": task_id,
            "productId": product_entity["entity_id"],
            "comment": asset_version["comment"],
            "status": status_names_by_id[status_id],
            "attrib": attribs,
        }

    return version_entities


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
    project_name: str,
    studio_settings: dict[str, Any],
):
    """Collect data from ftrack and convert them to AYON data.

    Args:
        session (FtrackSession): Ftrack session.
        project_name (str): Name of the project.
        studio_settings (dict[str, Any]): Studio settings.

    Returns:
        dict[str, Any]: Output contains project entity, folder entities
            and task entities. More might come in future (UPDATE).

    """
    ftrack_project: FtrackEntityType = await session.query(
        "select id, full_name, name, thumbnail_id, project_schema_id"
        f" from Project where full_name is \"{project_name}\""
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

    # TODO: Implement assets and versions (AYON products and versions)
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
    }


async def import_project(
    project_name: str,
    session: FtrackSession,
    studio_settings: dict[str, Any],
):
    """Sync ftrack project data to AYON.

    Args:
        project_name (str): Name of the project.
        session (FtrackSession): Ftrack session.
        studio_settings (dict[str, Any]): Studio settings.

    """
    data = await _collect_project_data(
        session, project_name, studio_settings,
    )
    project_code = data["project_code"]

    await create_project_from_anatomy(
        project_name,
        project_code,
        Anatomy(**data["project"]),
    )
    operations = ProjectLevelOperations(project_name)
    for folder_entity in data["folders"]:
        operations.create("folder", **folder_entity)

    for folder_entity in data["tasks"]:
        operations.create("task", **folder_entity)

    await operations.process()
