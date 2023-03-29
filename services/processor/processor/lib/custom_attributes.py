from .constants import CUST_ATTR_GROUPS


def join_query_values(values):
    """Helper to join fitler values for query."""

    return ",".join({"\"{}\"".format(value) for value in values})


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
