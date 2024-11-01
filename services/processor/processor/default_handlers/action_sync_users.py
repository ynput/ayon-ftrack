import typing
from typing import Union, List, Dict

import ayon_api

from ftrack_common import (
    ServerAction,
    get_service_ftrack_icon_url,
)
from processor.lib import map_ftrack_users_to_ayon_users

if typing.TYPE_CHECKING:
    from ftrack_api.entity.base import Entity as FtrackEntity


class SyncUsersFromFtrackAction(ServerAction):
    """Sync user entities to AYON.

    This action is used to synchronize users from ftrack to AYON.

    When a user is created it also synchronizes the user's roles.
    Roles are NOT synchronized on user update, because user roles might be
    already modified in AYON.

    Note: We might add interface to the action to be able to sync only specific
    users, or to sync roles even on user update.

    This action is available on project entities, but syncs all users.

    Does not sync user thumbnails, might be added in the future if needed,
    but probably only for new users or .
    """

    identifier = "sync.users.from.ftrack.to.ayon"
    label = "AYON Admin"
    variant = "- Sync users to AYON"
    description = "Synchronize users based on ftrack"
    icon = get_service_ftrack_icon_url("AYONAdmin.svg")

    role_list = ["Administrator", "Project Manager"]
    settings_key = "sync_users_from_ftrack"

    def discover(self, session, entities, event):
        """Show only on project."""
        if (
            len(entities) != 1
            or entities[0].entity_type.lower() != "project"
        ):
            return False
        return self.valid_roles(session, entities, event)

    def interface(self, session, entities, event):
        if event["data"].get("values"):
            return

        response = ayon_api.get("accessGroups/_")
        access_groups = [
            item["name"]
            for item in response.data
        ]

        title = "Default artist access groups"

        items = [
            {
                "type": "label",
                "value": "Select default access groups for artists",
            },
            {
                "type": "enumerator",
                "name": "access_groups",
                "data": [
                    {
                        "value": access_group,
                        "label": access_group,
                    }
                    for access_group in access_groups
                ],
                "value": access_groups,
                "multi_select": True,
            },
        ]

        return {
            "items": items,
            "title": title
        }

    def launch(self, session, entities, event):
        values = event["data"].get("values")
        access_groups = None
        if values:
            access_groups = values.get("access_groups")

        if access_groups is None:
            response = ayon_api.get("accessGroups/_")
            access_groups = [
                item["name"]
                for item in response.json()
            ]

        self.log.info("Synchronization begins")
        fields = {
            "id",
            "username",
            "is_active",
            "email",
            "first_name",
            "last_name",
            # "resource_type",
            # "thumbnail_id",
            # "thumbnail_url",
        }
        joined_fields = ", ".join(fields)
        ftrack_users = session.query(
            f"select {joined_fields} from User"
        ).all()
        ayon_users = list(ayon_api.get_users())
        ftrack_users_by_id = {
            ftrack_user["id"]: ftrack_user
            for ftrack_user in ftrack_users
        }
        security_roles_by_id: Dict[str, "FtrackEntity"] = {
            role["id"]: role
            for role in session.query(
                "select id, name, type from SecurityRole"
            ).all()
        }
        ayon_role_by_user_id: Dict[str, str] = {
            ftrack_id: "artist"
            for ftrack_id in ftrack_users_by_id
        }
        user_roles_by_user_id: Dict[str, List["FtrackEntity"]] = {
            ftrack_id: []
            for ftrack_id in ayon_role_by_user_id
        }
        for security_role in session.query(
            "select is_all_projects, is_all_open_projects"
            ", security_role_id, user_id"
            " from UserSecurityRole"
        ).all():
            role: FtrackEntity = (
                security_roles_by_id[security_role["security_role_id"]]
            )
            user_id: str = security_role["user_id"]
            user_roles_by_user_id.setdefault(user_id, []).append(role)
            if (
                not security_role["is_all_projects"]
                and not security_role["is_all_open_projects"]
            ):
                continue

            if role["name"] == "Administrator":
                ayon_role_by_user_id[user_id] = "admin"
                continue
            current_role = ayon_role_by_user_id[user_id]
            if role["name"] == "Project Manager" and current_role != "admin":
                ayon_role_by_user_id[user_id] = "manager"

        ftrack_projects: List[FtrackEntity] = session.query(
            "select id, full_name, is_private from Project"
        )
        users_mapping: Dict[str, Union[str, None]] = (
            map_ftrack_users_to_ayon_users(ftrack_users, ayon_users)
        )
        for ftrack_id, ayon_user_name in users_mapping.items():
            ftrack_user = ftrack_users_by_id[ftrack_id]
            ayon_user_data = {
                "ftrack": {
                    "id": ftrack_id,
                    "username": ftrack_user["username"],
                }
            }
            ayon_role = ayon_role_by_user_id[ftrack_id]

            attrib = {}
            if ftrack_user["email"]:
                attrib["email"] = ftrack_user["email"]

            full_name_items = []
            for key in "first_name", "last_name":
                value = ftrack_user[key]
                if value:
                    full_name_items.append(value)

            if full_name_items:
                attrib["fullName"] = " ".join(full_name_items)

            is_admin = ayon_role == "admin"
            is_manager = ayon_role == "manager"
            if not ayon_user_name:
                ayon_user_data["isAdmin"] = is_admin
                ayon_user_data["isManger"] = is_manager
                # Create new user
                if not is_admin and not is_manager:
                    # TODO use predefined endpoints (are not available
                    #   at the moment of this PR)
                    ayon_user_data["defaultAccessGroups"] = list(
                        access_groups
                    )
                    ayon_user_data["accessGroups"] = (
                        self._calculate_default_access_groups(
                            ftrack_projects,
                            user_roles_by_user_id[ftrack_id],
                            access_groups
                        )
                    )

                new_ayon_user = {
                    "active": ftrack_user["is_active"],
                    "data": ayon_user_data,
                }
                if attrib:
                    new_ayon_user["attrib"] = attrib
                username = ftrack_user["username"].split("@", 1)[0]
                ayon_api.put(
                    f"users/{username}",
                    **new_ayon_user,
                )
                continue

            # Fetch user with REST to get 'data'
            ayon_user = ayon_api.get_user(ayon_user_name)
            user_diffs = {}
            if ftrack_user["is_active"] != ayon_user["active"]:
                user_diffs["active"] = ftrack_user["is_active"]

            # Comapre 'data' field
            current_user_data = ayon_user["data"]
            data_diffs = {}
            if "ftrack" in current_user_data:
                ayon_user_ftrack_data = current_user_data["ftrack"]
                for key, value in ayon_user_data["ftrack"].items():
                    if (
                        key not in ayon_user_ftrack_data
                        or ayon_user_ftrack_data[key] != value
                    ):
                        ayon_user_ftrack_data.update(ayon_user_data["ftrack"])
                        data_diffs["ftrack"] = ayon_user_ftrack_data
                        break

            if ayon_role == "admin":
                if not current_user_data.get("isAdmin"):
                    data_diffs["isAdmin"] = True
                    if current_user_data.get("isManger"):
                        data_diffs["isManger"] = False

            elif ayon_role == "manager":
                if not current_user_data.get("isManger"):
                    data_diffs["isManger"] = True
                    if current_user_data.get("isAdmin"):
                        data_diffs["isAdmin"] = False

            elif ayon_role == "artist":
                if current_user_data.get("isAdmin"):
                    data_diffs["isAdmin"] = False

                if current_user_data.get("isManger"):
                    data_diffs["isManger"] = False

            if data_diffs:
                user_diffs["data"] = data_diffs

            # Compare 'attrib' fields
            for key, value in attrib.items():
                if ayon_user["attrib"].get(key) != value:
                    attrib_diffs = user_diffs.setdefault("attrib", {})
                    attrib_diffs[key] = value

            if user_diffs:
                ayon_api.patch(
                    f"users/{ayon_user_name}",
                    **user_diffs,
                )

        self.log.info("Synchronization finished")
        return True

    def _calculate_default_access_groups(
        self, ftrack_projects, user_roles, access_groups
    ):
        available_project_names = []
        project_names = set(ayon_api.get_project_names(active=None))
        for ftrack_project in ftrack_projects:
            project_name = ftrack_project["full_name"]
            if project_name not in project_names:
                continue

            if not ftrack_project["is_private"]:
                available_project_names.append(project_name)
                continue

            project_id = ftrack_project["id"]
            for role in user_roles:
                if role["project_id"] == project_id:
                    available_project_names.append(project_name)
                    break

        return {
            project_name: list(access_groups)
            for project_name in available_project_names
        }
