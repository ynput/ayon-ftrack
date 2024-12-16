import os
import json
import collections
import typing
from typing import Optional, Dict, List, Any

import ayon_api
import ftrack_api

from .lib import join_filter_values, create_chunks
from .constants import CUST_ATTR_GROUP

if typing.TYPE_CHECKING:
    import ftrack_api.entity.base.Entity


class MappedAYONAttribute:
    def __init__(
        self,
        ayon_attribute_name: str,
        is_hierarchical: bool = True,
        attr_confs: Optional[List["ftrack_api.entity.base.Entity"]] = None,
    ):
        self.ayon_attribute_name: str = ayon_attribute_name
        self.is_hierarchical: bool = is_hierarchical
        if attr_confs is None:
            attr_confs = []
        self._attr_confs: List["ftrack_api.entity.base.Entity"] = attr_confs

    def has_confs(self) -> bool:
        return bool(self.attr_confs)

    def add_attr_conf(self, attr_conf: "ftrack_api.entity.base.Entity"):
        self._attr_confs.append(attr_conf)

    def get_attr_confs(self) -> List["ftrack_api.entity.base.Entity"]:
        return list(self._attr_confs)

    attr_confs: List["ftrack_api.entity.base.Entity"] = property(
        get_attr_confs
    )

    def get_attr_conf_for_entity_type(
        self, entity_type: str, object_type_id: Optional[str]
    ) -> Optional["ftrack_api.entity.base.Entity"]:
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
        self, entity: "ftrack_api.entity.base.Entity"
    ) -> Optional["ftrack_api.entity.base.Entity"]:
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
        self, ft_entity: "ftrack_api.entity.base.Entity", key: str
    ) -> Optional[MappedAYONAttribute]:
        for mapping_item in self.values():
            attr_conf = mapping_item.get_attr_conf_for_entity(ft_entity)
            if attr_conf and attr_conf["key"] == key:
                return mapping_item


def get_ayon_attr_configs(session, query_keys=None, split_hierarchical=True):
    """Query custom attribute configurations from ftrack server.

    Args:
        session (ftrack_api.Session): Connected ftrack session.
        query_keys (Union[Iterable[str], None]): Key to query for
            attribute configurations.
        split_hierarchical (bool): Change output type. Attributes are split
            into 2 lists if enabled.

    Returns:
        Union[List[Any], Tuple[List[Any], List[Any]]: ftrack custom attributes.
    """

    custom_attributes = []
    hier_custom_attributes = []
    if not query_keys:
        query_keys = {
            "id",
            "key",
            "entity_type",
            "object_type_id",
            "is_hierarchical",
            "default"
        }

    query_keys = set(query_keys)
    if split_hierarchical:
        query_keys.add("is_hierarchical")

    cust_attrs_query = (
        "select {}"
        " from CustomAttributeConfiguration"
        " where group.name in ({})"
    ).format(
        ", ".join(query_keys),
        join_filter_values({"openpype", CUST_ATTR_GROUP})
    )
    all_attrs = session.query(cust_attrs_query).all()
    for cust_attr in all_attrs:
        if split_hierarchical and cust_attr["is_hierarchical"]:
            hier_custom_attributes.append(cust_attr)
        else:
            custom_attributes.append(cust_attr)

    if not split_hierarchical:
        return custom_attributes
    return custom_attributes, hier_custom_attributes


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
        query_keys = ", ".join({
            "id",
            "key",
            "entity_type",
            "object_type_id",
            "is_hierarchical",
            "default",
        })
        attr_confs = session.query(
            f"select {query_keys} from CustomAttributeConfiguration"
        ).all()

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
                attr_names = item["nonhierarchical"]
                for attr_conf in nonhier_attrs:
                    if attr_conf["key"] in attr_names:
                        mapped_item.add_attr_conf(attr_conf)

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
        if attr_id in hier_attr_ids or attr_key not in entity_values:
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

    json_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "custom_attributes.json"
    )
    with open(json_file_path, "r") as json_stream:
        data = json.load(json_stream)
    return data
