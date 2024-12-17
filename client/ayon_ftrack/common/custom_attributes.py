import os
import json
import copy
import collections
import typing
from typing import Optional, Dict, List, Any, Iterable

import ayon_api
import ftrack_api

from .lib import join_filter_values, create_chunks
from .constants import (
    CUST_ATTR_GROUP,
    CUST_ATTR_KEY_SERVER_ID,
    CUST_ATTR_KEY_SERVER_PATH,
    CUST_ATTR_KEY_SYNC_FAIL,
    CUST_ATTR_AUTO_SYNC,
)

if typing.TYPE_CHECKING:
    from ftrack_api.entity.base import Entity as FtrackEntity


class MappedAYONAttribute:
    def __init__(
        self,
        ayon_attribute_name: str,
        is_hierarchical: bool = True,
        attr_confs: Optional[List["FtrackEntity"]] = None,
    ):
        self.ayon_attribute_name: str = ayon_attribute_name
        self.is_hierarchical: bool = is_hierarchical
        if attr_confs is None:
            attr_confs = []
        self._attr_confs: List["FtrackEntity"] = attr_confs

    def has_confs(self) -> bool:
        return bool(self.attr_confs)

    def add_attr_conf(self, attr_conf: "FtrackEntity"):
        self._attr_confs.append(attr_conf)

    def get_attr_confs(self) -> List["FtrackEntity"]:
        return list(self._attr_confs)

    attr_confs: List["FtrackEntity"] = property(get_attr_confs)

    def get_attr_conf_for_entity_type(
        self, entity_type: str, object_type_id: Optional[str]
    ) -> Optional["FtrackEntity"]:
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
        self, entity: "FtrackEntity"
    ) -> Optional["FtrackEntity"]:
        if entity is None:
            return None

        entity_type = entity.entity_type.lower()
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
        self._items: Dict[str, MappedAYONAttribute] = {}

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
        self, ft_entity: "FtrackEntity", key: str
    ) -> Optional[MappedAYONAttribute]:
        for mapping_item in self.values():
            attr_conf = mapping_item.get_attr_conf_for_entity(ft_entity)
            if attr_conf is not None and attr_conf["key"] == key:
                return mapping_item


def get_all_attr_configs(
    session: ftrack_api.Session,
    fields: Optional[Iterable[str]] = None,
) -> List["FtrackEntity"]:
    """Query custom attribute configurations from ftrack server.

    Args:
        session (ftrack_api.Session): Connected ftrack session.
        fields (Optional[Iterable[str]]): Field to query for
            attribute configurations.

    Returns:
        List[FtrackEntity]: ftrack custom attributes.

    """
    if not fields:
        fields = {
            "id",
            "key",
            "entity_type",
            "object_type_id",
            "is_hierarchical",
            "default",
            "group_id",
            "type_id",
            # "config",
            # "label",
            # "sort",
            # "project_id",
        }

    joined_fields = ", ".join(set(fields))

    return session.query(
        f"select {joined_fields} from CustomAttributeConfiguration"
    ).all()


def get_custom_attributes_mapping(
    session: ftrack_api.Session,
    addon_settings: Dict[str, Any],
    attr_confs: Optional[List[object]] = None,
    ayon_attributes: Optional[List[object]] = None,
) -> CustomAttributesMapping:
    """Query custom attribute configurations from ftrack server.

    Returns:
        Dict[str, List[object]]: ftrack custom attributes.

    """
    cust_attr = addon_settings["custom_attributes"]
    # "custom_attributes/attributes_mapping/mapping"
    attributes_mapping = cust_attr["attributes_mapping"]

    if attr_confs is None:
        attr_confs = get_all_attr_configs(session)

    if ayon_attributes is None:
        ayon_attributes = ayon_api.get_attributes_schema()["attributes"]

    ayon_attribute_names = {
        attr["name"]
        for attr in ayon_attributes
    }

    hier_attrs = []
    nonhier_attrs = []
    for attr_conf in attr_confs:
        if attr_conf["is_hierarchical"]:
            hier_attrs.append(attr_conf)
        else:
            nonhier_attrs.append(attr_conf)

    output = CustomAttributesMapping()
    if not attributes_mapping["enabled"]:
        builtin_attrs = {
            attr["name"]
            for attr in ayon_attributes
            if attr["builtin"]
        }
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
                attr_name in builtin_attrs
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
    for chunk in create_chunks(entity_ids, chunk_size):
        entity_ids_joined = join_filter_values(chunk)
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


def get_custom_attributes_by_entity_id(
    session,
    entity_ids,
    attr_configs,
    skip_none_values=True,
    store_by_key=True
):
    """Query custom attribute values and store their value by entity and attr.

    There is option to return values by attribute key or attribute id. In case
    the output should be stored by key and there is hierarchical attribute
    with same key as non-hierarchical it's then hierarchical value
    has priority of usage.

    Args:
        session (ftrack_api.Session): Connected ftrack session.
        entity_ids (Iterable[str]): Entity ids for which custom attribute
            values should be returned.
        attr_configs: Custom attribute configurations.
        skip_none_values (bool): Custom attribute with value set to 'None'
            won't be in output.
        store_by_key (bool): Output will be stored by attribute keys if true
            otherwise is value stored by attribute id.

    Returns:
        Dict[str, Dict[str, Any]]: Custom attribute values by entity id.

    """
    entity_ids = set(entity_ids)
    hier_attr_ids = {
        attr_conf["id"]
        for attr_conf in attr_configs
        if attr_conf["is_hierarchical"]
    }
    attr_by_id = {
        attr_conf["id"]: attr_conf["key"]
        for attr_conf in attr_configs
    }

    value_items = query_custom_attribute_values(
        session, attr_by_id.keys(), entity_ids
    )

    output = collections.defaultdict(dict)
    for value_item in value_items:
        value = value_item["value"]
        if skip_none_values and value is None:
            continue

        entity_id = value_item["entity_id"]
        entity_values = output[entity_id]
        attr_id = value_item["configuration_id"]
        if not store_by_key:
            entity_values[attr_id] = value
            continue

        attr_key = attr_by_id[attr_id]
        # Hierarchical attributes are always preferred
        if attr_key not in entity_values or attr_id in hier_attr_ids:
            entity_values[attr_key] = value

    return output


def default_custom_attributes_definition():
    """Default custom attribute definitions created in ftracl.

    Todos:
        Convert to list of dictionaries to be able determine order. Check if
            ftrack api support to define order first!

    Returns:
        dict[str, Any]: Custom attribute configurations per entity type that
            can be used to create/update custom attributes.

    """
    # TODO use AYON built-in attributes as source of truth
    json_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "custom_attributes.json"
    )
    with open(json_file_path, "r") as json_stream:
        data = json.load(json_stream)
    return data


def ensure_custom_attribute_group_exists(
    session: ftrack_api.Session,
    group: str,
    groups: Optional[List["FtrackEntity"]] = None,
) -> "FtrackEntity":
    """Ensure custom attribute group in ftrack.

    Args:
        session (ftrack_api.Session): Connected ftrack session.
        group (str): Name of group.
        groups (Optional[List[FtrackEntity]]): Pre-fetched
            custom attribute groups.

    Returns:
        FtrackEntity: Created custom attribute group.

    """
    if groups is None:
        groups = session.query(
            "select id, name from CustomAttributeGroup"
        ).all()
    low_name = group.lower()
    for group in groups:
        if group["name"].lower() == low_name:
            return group

    group = session.create(
        "CustomAttributeGroup",
        {"name": group}
    )
    session.commit()
    return group


def ensure_mandatory_custom_attributes_exists(
    session: ftrack_api.Session,
    addon_settings: Dict[str, Any],
    attr_confs: Optional[List["FtrackEntity"]] = None,
    custom_attribute_types: Optional[List["FtrackEntity"]] = None,
    groups: Optional[List["FtrackEntity"]] = None,
    security_roles: Optional[List["FtrackEntity"]] = None,
):
    """Make sure that mandatory custom attributes exists in ftrack.

    Args:
        session (ftrack_api.Session): Connected ftrack session.
        addon_settings (Dict[str, Any]): Addon settings.
        attr_confs (Optional[List[FtrackEntity]]): Pre-fetched all existing
            custom attribute configurations in ftrack.
        custom_attribute_types (Optional[List[FtrackEntity]]): Pre-fetched
            custom attribute types.
        groups (Optional[List[FtrackEntity]]): Pre-fetched custom attribute
            groups.
        security_roles (Optional[List[FtrackEntity]]): Pre-fetched security
            roles.

    """
    if attr_confs is None:
        attr_confs = get_all_attr_configs(session)

    # Split existing custom attributes
    attr_confs_by_entity_type = collections.defaultdict(list)
    hier_confs = []
    for attr_conf in attr_confs:
        if attr_conf["is_hierarchical"]:
            hier_confs.append(attr_conf)
        else:
            entity_type = attr_conf["entity_type"]
            attr_confs_by_entity_type[entity_type].append(attr_conf)

    # Prepare possible attribute types
    if custom_attribute_types is None:
        custom_attribute_types = session.query(
            "select id, name from CustomAttributeType"
        ).all()

    attr_type_id_by_low_name = {
        attr_type["name"].lower(): attr_type["id"]
        for attr_type in custom_attribute_types
    }

    if security_roles is None:
        security_roles = session.query(
            "select id, name, type from SecurityRole"
        ).all()

    security_roles = {
        role["name"].lower(): role
        for role in security_roles
    }
    mandatory_attributes_settings = (
        addon_settings
        ["custom_attributes"]
        ["mandatory_attributes"]
    )

    # Prepare group
    group_entity = ensure_custom_attribute_group_exists(
        session, CUST_ATTR_GROUP, groups
    )
    group_id = group_entity["id"]

    for item in [
        {
            "key": CUST_ATTR_KEY_SERVER_ID,
            "type": "text",
            "label": "AYON ID",
            "default": "",
            "is_hierarchical": True,
            "config": {"markdown": False},
            "group_id": group_id,
        },
        {
            "key": CUST_ATTR_KEY_SERVER_PATH,
            "type": "text",
            "label": "AYON path",
            "default": "",
            "is_hierarchical": True,
            "config": {"markdown": False},
            "group_id": group_id,
        },
        {
            "key": CUST_ATTR_KEY_SYNC_FAIL,
            "type": "boolean",
            "label": "AYON sync failed",
            "is_hierarchical": True,
            "default": False,
            "group_id": group_id,
        },
        {
            "key": CUST_ATTR_AUTO_SYNC,
            "type": "boolean",
            "label": "AYON auto-sync",
            "default": False,
            "is_hierarchical": False,
            "entity_type": "show",
            "group_id": group_id,
        }
    ]:
        key = item["key"]
        attr_settings = mandatory_attributes_settings[key]
        read_roles = []
        write_roles = []
        for role_names, roles in (
            (attr_settings["read_security_roles"], read_roles),
            (attr_settings["write_security_roles"], write_roles),
        ):
            if not role_names:
                roles.extend(security_roles.values())
                continue

            for name in role_names:
                role = security_roles.get(name.lower())
                if role is not None:
                    roles.append(role)

        is_hierarchical = item["is_hierarchical"]
        entity_type_confs = hier_confs
        if not is_hierarchical:
            entity_type = item["entity_type"]
            entity_type_confs = attr_confs_by_entity_type.get(entity_type, [])
        matching_attr_conf = next(
            (
                attr_conf
                for attr_conf in entity_type_confs
                if attr_conf["key"] == key
            ),
            None
        )

        entity_data = copy.deepcopy(item)
        attr_type = entity_data.pop("type")
        entity_data["type_id"] = attr_type_id_by_low_name[attr_type.lower()]
        # Convert 'config' to json string
        config = entity_data.get("config")
        if isinstance(config, dict):
            entity_data["config"] = json.dumps(config)

        if matching_attr_conf is None:
            # Make sure 'entity_type' is filled for hierarchical attribute
            # - it is required to be able to create custom attribute
            if is_hierarchical:
                entity_data.setdefault("entity_type", "show")
            # Make sure config is set to empty dictionary for creation
            entity_data.setdefault("config", "{}")
            entity_data["read_security_roles"] = read_roles
            entity_data["write_security_roles"] = write_roles
            session.create(
                "CustomAttributeConfiguration",
                entity_data
            )
            session.commit()
            continue

        changed = False
        for key, value in entity_data.items():
            if matching_attr_conf[key] != value:
                matching_attr_conf[key] = value
                changed = True

        match_read_role_ids = {
            role["id"] for role in matching_attr_conf["read_security_roles"]
        }
        match_write_role_ids = {
            role["id"] for role in matching_attr_conf["write_security_roles"]
        }
        if match_read_role_ids != {role["id"] for role in read_roles}:
            matching_attr_conf["read_security_roles"] = read_roles
            changed = True
        if match_write_role_ids != {role["id"] for role in write_roles}:
            matching_attr_conf["write_security_roles"] = write_roles
            changed = True

        if changed:
            session.commit()

