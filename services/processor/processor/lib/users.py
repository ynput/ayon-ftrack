from typing import Dict, List, Set, Union, Any

import ftrack_api.entity.user


def map_ftrack_users_to_ayon_users(
    ftrack_users: List[ftrack_api.entity.user.User],
    ayon_users: List[Dict[str, Any]],
) -> Dict[str, Union[str, None]]:
    """Map ftrack users to AYON users.

    Mapping is based on 2 possible keys, email and username where email has
    higher priority. Once AYON user is mapped it cannot be mapped again to
    different user.

    Args:
        ftrack_users (List[ftrack_api.entity.user.User]): List of ftrack users.
        ayon_users (List[Dict[str, Any]]): List of AYON users.

    Returns:
        Dict[str, Union[str, None]]: Mapping of ftrack user id
            to AYON username.

    """
    mapping: Dict[str, Union[str, None]] = {
        user["id"]: None
        for user in ftrack_users
    }
    ayon_users_by_email: Dict[str, str] = {}
    ayon_users_by_name: Dict[str, str] = {}
    for ayon_user in ayon_users:
        ayon_name = ayon_user["name"]
        ayon_email = ayon_user["attrib"]["email"]
        ayon_users_by_name[ayon_name.lower()] = ayon_name
        if ayon_email:
            ayon_users_by_email[ayon_email.lower()] = ayon_name

    mapped_ayon_users: Set[str] = set()
    for ftrack_user in ftrack_users:
        ftrack_id: str = ftrack_user["id"]
        # Make sure username does not contain '@' character
        ftrack_name: str = ftrack_user["username"].split("@", 1)[0]
        ftrack_email: str = ftrack_user["email"]

        if ftrack_email and ftrack_email.lower() in ayon_users_by_email:
            ayon_name: str = ayon_users_by_email[ftrack_email.lower()]
            if ayon_name not in mapped_ayon_users:
                mapping[ftrack_id] = ayon_name
                mapped_ayon_users.add(ayon_name)
            continue

        if ftrack_name in ayon_users_by_name:
            ayon_name: str = ayon_users_by_name[ftrack_name]
            if ayon_name not in mapped_ayon_users:
                mapped_ayon_users.add(ayon_name)
                mapping[ftrack_id] = ayon_name

    return mapping
