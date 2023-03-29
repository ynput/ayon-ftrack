import itertools
import collections

from .lib import join_filter_values, create_chunks
from .constants import CUST_ATTR_GROUP


def get_ayon_attr_configs(session, query_keys=None, split_hierarchical=True):
    """Query custom attribute configurations from ftrack server.

    Args:
        session (ftrack_api.Session): Connected ftrack session.
        query_keys (Union[Iterable[str], None]): Key to query for
            attribute configurations.
        split_hierarchical (bool): Change output type. Attributes are split
            into 2 lists if enabled.

    Returns:
        Union[List[Any], Tuple[List[Any], List[Any]]: Ftrack custom attributes.
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
    all_avalon_attr = session.query(cust_attrs_query).all()
    for cust_attr in all_avalon_attr:
        if split_hierarchical and cust_attr["is_hierarchical"]:
            hier_custom_attributes.append(cust_attr)
        else:
            custom_attributes.append(cust_attr)

    if not split_hierarchical:
        return custom_attributes
    return custom_attributes, hier_custom_attributes


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
    hier_attr_configs,
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
        attr_configs: Non-hierarchical attribute configurations.
        hier_attr_configs: Hierarchical attribute configurations.
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
        for attr_conf in hier_attr_configs
    }
    attr_by_id = {
        attr_conf["id"]: attr_conf["key"]
        for attr_conf in itertools.chain(attr_configs, hier_attr_configs)
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
        if store_by_key:
            attr_key = attr_by_id[attr_id]
            # Hierarchical attributes are always preferred
            if attr_id in hier_attr_ids or attr_key not in entity_values:
                entity_values[attr_key] = value
        else:
            entity_values[attr_id] = value

    return output