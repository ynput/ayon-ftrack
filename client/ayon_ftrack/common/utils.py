"""Utils for ftrack.

Whereas lib.py contains utils for general use, this module contains utils
    for certain situations, or functions that would cause circular import
    when implemented in lib.py.

It is possible that some functions will be moved from lib.py to this module
    to keep consistency.
"""

import datetime

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


def get_datetime_data(datetime_obj=None):
    """Returns current datetime data as dictionary.

    Note:
        This function is copied from 'openpype.lib'.

    Args:
        datetime_obj (datetime): Specific datetime object

    Returns:
        dict: prepared date & time data

    Available keys:
        "d" - <Day of month number> in shortest possible way.
        "dd" - <Day of month number> with 2 digits.
        "ddd" - <Week day name> shortened week day. e.g.: `Mon`, ...
        "dddd" - <Week day name> full name of week day. e.g.: `Monday`, ...
        "m" - <Month number> in shortest possible way. e.g.: `1` if January
        "mm" - <Month number> with 2 digits.
        "mmm" - <Month name> shortened month name. e.g.: `Jan`, ...
        "mmmm" - <Month name> full month name. e.g.: `January`, ...
        "yy" - <Year number> shortened year. e.g.: `19`, `20`, ...
        "yyyy" - <Year number> full year. e.g.: `2019`, `2020`, ...
        "H" - <Hours number 24-hour> shortened hours.
        "HH" - <Hours number 24-hour> with 2 digits.
        "h" - <Hours number 12-hour> shortened hours.
        "hh" - <Hours number 12-hour> with 2 digits.
        "ht" - <Midday type> AM or PM.
        "M" - <Minutes number> shortened minutes.
        "MM" - <Minutes number> with 2 digits.
        "S" - <Seconds number> shortened seconds.
        "SS" - <Seconds number> with 2 digits.
    """

    if not datetime_obj:
        datetime_obj = datetime.datetime.now()

    year = datetime_obj.strftime("%Y")

    month = datetime_obj.strftime("%m")
    month_name_full = datetime_obj.strftime("%B")
    month_name_short = datetime_obj.strftime("%b")
    day = datetime_obj.strftime("%d")

    weekday_full = datetime_obj.strftime("%A")
    weekday_short = datetime_obj.strftime("%a")

    hours = datetime_obj.strftime("%H")
    hours_midday = datetime_obj.strftime("%I")
    hour_midday_type = datetime_obj.strftime("%p")
    minutes = datetime_obj.strftime("%M")
    seconds = datetime_obj.strftime("%S")

    return {
        "d": str(int(day)),
        "dd": str(day),
        "ddd": weekday_short,
        "dddd": weekday_full,
        "m": str(int(month)),
        "mm": str(month),
        "mmm": month_name_short,
        "mmmm": month_name_full,
        "yy": str(year[2:]),
        "yyyy": str(year),
        "H": str(int(hours)),
        "HH": str(hours),
        "h": str(int(hours_midday)),
        "hh": str(hours_midday),
        "ht": hour_midday_type,
        "M": str(int(minutes)),
        "MM": str(minutes),
        "S": str(int(seconds)),
        "SS": str(seconds),
    }
