"""Ftrack common functions that can be used in ftrack services or on client.

Most of the functionality is usable at multiple places and would require to
be duplicated.

It is expected content of this folder is copied to place from which will be
process able to import them.
"""

from .constants import (
    FTRACK_ID_ATTRIB,
    CUST_ATTR_KEY_SERVER_ID,
    CUST_ATTR_GROUP,
    CUST_ATTR_AUTO_SYNC,
    FPS_KEYS,
)

from .custom_attributes import (
    get_custom_attr_configs,
    query_custom_attribute_values,
    get_custom_attributes_by_entity_id,
)

from .lib import (
    join_filter_values,
    create_chunks,
)


__all__ = (
    "FTRACK_ID_ATTRIB",
    "CUST_ATTR_KEY_SERVER_ID",
    "CUST_ATTR_GROUP",
    "CUST_ATTR_AUTO_SYNC",
    "FPS_KEYS",

    "get_custom_attr_configs",
    "query_custom_attribute_values",
    "get_custom_attributes_by_entity_id",

    "join_filter_values",
    "create_chunks",
)