"""Utils for ftrack.

Whereas lib.py contains utils for general use, this module contains utils
    for certain situations, or functions that would cause circular import
    when implemented in lib.py.

It is possible that some functions will be moved from lib.py to this module
    to keep consistency.
"""

from .constants import CUST_ATTR_KEY_SERVER_PATH
from .custom_attributes import query_custom_attribute_values


def get_folder_path_for_entities(
    session, entities, path_cust_attr_id=None, allow_use_link=True
):
    """Get folder path for ftrack entities.

    Folder path is received from custom attribute, or from entity link
        which contains name of parent entities.

    Args:
        session (ftrack_api.Session): Connected ftrack session.
        entities (List[dict]): List of ftrack entities.
        path_cust_attr_id (Union[str, None]): Custom attribute
            configuration id which stores entity path.
        allow_use_link (bool): Use 'link' value if path is not found in
            custom attributes.

    Returns:
        dict[str, Union[str, None]]: Entity path by ftrack entity id.
            Output will always contain all entity ids from input.
    """

    entities_by_id = {
        entity["id"]: entity
        for entity in entities
    }
    entity_ids = set(entities_by_id.keys())
    folder_paths_by_id = {
        entity_id: None
        for entity_id in entity_ids
    }
    if not folder_paths_by_id:
        return folder_paths_by_id

    if path_cust_attr_id is None:
        cust_attr_conf = session.query(
            "select id, key from CustomAttributeConfiguration"
            f" where key is '{CUST_ATTR_KEY_SERVER_PATH}'"
        ).first()
        if cust_attr_conf:
            path_cust_attr_id = cust_attr_conf["id"]

    value_items = []
    if path_cust_attr_id is not None:
        value_items = query_custom_attribute_values(
            session, {path_cust_attr_id}, entity_ids
        )

    for value_item in value_items:
        path = value_item["value"]
        entity_id = value_item["entity_id"]
        if path:
            entity_ids.discard(entity_id)
            folder_paths_by_id[entity_id] = path

    if allow_use_link:
        for missing_id in entity_ids:
            entity = entities_by_id[missing_id]
            # Use stupidly simple solution
            link_names = [item["name"] for item in entity["link"]]
            # Change project name to empty string
            link_names[0] = ""
            folder_paths_by_id[missing_id] = "/".join(link_names)

    return folder_paths_by_id
