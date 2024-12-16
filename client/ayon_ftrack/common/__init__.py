"""ftrack common functions that can be used in ftrack services or on client.

Most of the functionality is usable at multiple places and would require to
be duplicated.

It is expected content of this folder is copied to place from which will be
process able to import them.
"""

__all__ = (
    "FTRACK_ID_ATTRIB",
    "FTRACK_PATH_ATTRIB",
    "REMOVED_ID_VALUE",

    "CUST_ATTR_KEY_SERVER_ID",
    "CUST_ATTR_KEY_SERVER_PATH",
    "CUST_ATTR_KEY_SYNC_FAIL",
    "CUST_ATTR_GROUP",
    "CUST_ATTR_AUTO_SYNC",

    "FPS_KEYS",
    "CUST_ATTR_INTENT",

    "InvalidFpsValue",

    "import_filepath",
    "modules_from_path",

    "is_ftrack_enabled_in_settings",
    "join_filter_values",
    "create_chunks",
    "convert_to_fps",
    "get_host_ip",
    "get_ftrack_public_url",
    "get_ftrack_icon_url",
    "get_service_ftrack_icon_url",

    "map_ftrack_users_to_ayon_users",

    "MappedAYONAttribute",
    "CustomAttributesMapping",
    "get_ayon_attr_configs",
    "get_custom_attributes_mapping",
    "query_custom_attribute_values",
    "get_custom_attributes_by_entity_id",
    "default_custom_attributes_definition",

    "get_folder_path_for_entities",
    "get_datetime_data",

    "BaseHandler",
    "BaseEventHandler",
    "BaseAction",
    "LocalAction",
    "ServerAction",

    "FtrackServer",
)


from .constants import (
    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
    REMOVED_ID_VALUE,

    CUST_ATTR_KEY_SERVER_ID,
    CUST_ATTR_KEY_SERVER_PATH,
    CUST_ATTR_KEY_SYNC_FAIL,

    CUST_ATTR_GROUP,
    CUST_ATTR_AUTO_SYNC,

    FPS_KEYS,
    CUST_ATTR_INTENT,
)

from .exceptions import (
    InvalidFpsValue,
)

from .python_module_tools import (
    import_filepath,
    modules_from_path,
)

from .lib import (
    is_ftrack_enabled_in_settings,
    join_filter_values,
    create_chunks,
    convert_to_fps,
    get_host_ip,
    get_ftrack_public_url,
    get_ftrack_icon_url,
    get_service_ftrack_icon_url,
)

from .users import map_ftrack_users_to_ayon_users

from .custom_attributes import (
    MappedAYONAttribute,
    CustomAttributesMapping,
    get_ayon_attr_configs,
    get_custom_attributes_mapping,
    query_custom_attribute_values,
    get_custom_attributes_by_entity_id,
    default_custom_attributes_definition,
)
from .utils import (
    get_folder_path_for_entities,
    get_datetime_data,
)

from .event_handlers import (
    BaseHandler,
    BaseEventHandler,
    BaseAction,
    LocalAction,
    ServerAction,
)

from .ftrack_server import FtrackServer
