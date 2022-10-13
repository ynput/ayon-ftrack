from .constants import CUST_ATTR_GROUPS


def join_query_values(values):
    """Helper to join fitler values for query."""

    return ",".join(["\"{}\"".format(value) for value in values])


def query_custom_attributes(
    session, conf_ids, entity_ids, only_set_values=False
):
    """Query custom attribute values from ftrack database.

    Using ftrack call method result may differ based on used table name and
    version of ftrack server.

    For hierarchical attributes you shou always use `only_set_values=True`
    otherwise result will be default value of custom attribute and it would not
    be possible to differentiate if value is set on entity or default value is
    used.

    Args:
        session(ftrack_api.Session): Connected ftrack session.
        conf_id(list, set, tuple): Configuration(attribute) ids which are
            queried.
        entity_ids(list, set, tuple): Entity ids for which are values queried.
        only_set_values(bool): Entities that don't have explicitly set
            value won't return a value. If is set to False then default custom
            attribute value is returned if value is not set.
    """
    output = []
    # Just skip
    if not conf_ids or not entity_ids:
        return output

    if only_set_values:
        table_name = "CustomAttributeValue"
    else:
        table_name = "ContextCustomAttributeValue"

    # Prepare values to query
    attributes_joined = join_query_values(conf_ids)
    attributes_len = len(conf_ids)

    # Query values in chunks
    chunk_size = int(5000 / attributes_len)
    # Make sure entity_ids is `list` for chunk selection
    entity_ids = list(entity_ids)
    for idx in range(0, len(entity_ids), chunk_size):
        entity_ids_joined = join_query_values(
            entity_ids[idx:idx + chunk_size]
        )
        output.extend(
            session.query(
                (
                    "select value, entity_id, configuration_id from {}"
                    " where entity_id in ({}) and configuration_id in ({})"
                ).format(
                    table_name,
                    entity_ids_joined,
                    attributes_joined
                )
            ).all()
        )
    return output


def get_openpype_attr(session, split_hierarchical=True, query_keys=None):
    custom_attributes = []
    hier_custom_attributes = []
    if not query_keys:
        query_keys = [
            "id",
            "entity_type",
            "object_type_id",
            "is_hierarchical",
            "default"
        ]
    # TODO remove deprecated "pype" group from query
    cust_attrs_query = (
        "select {}"
        " from CustomAttributeConfiguration"
        # Kept `pype` for Backwards Compatiblity
        " where group.name in ({})"
    ).format(", ".join(query_keys), join_query_values(CUST_ATTR_GROUPS))
    all_avalon_attr = session.query(cust_attrs_query).all()
    for cust_attr in all_avalon_attr:
        if split_hierarchical and cust_attr["is_hierarchical"]:
            hier_custom_attributes.append(cust_attr)
            continue

        custom_attributes.append(cust_attr)

    if split_hierarchical:
        # return tuple
        return custom_attributes, hier_custom_attributes

    return custom_attributes
