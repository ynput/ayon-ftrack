from typing import Dict, Union

import ayon_api

from ftrack_common import BaseEventHandler
from processor.lib import map_ftrack_users_to_ayon_users


class SyncUsersFromFtrackEvent(BaseEventHandler):
    """Sync access groups of private projects.

    Listen to changes of user roles in private projects. When user is added
    to the project, user is added to the project access group in AYON.
    When user is removed from the project, user is removed from the project
    access group in AYON.

    That works only for artist users. Admin and manager users can access all
    projects in AYON anyway. Roles from 'defaultAccessGroups'
    are used as roles on user in AYON.
    """
    def launch(self, session, event):
        project_ids_by_user_id = {}
        project_ids = set()

        for ent_info in event["data"].get("entities", []):
            if ent_info.get("entityType") != "userroleproject":
                continue

            change_user_ids = [
                p["entityId"]
                for p in ent_info["parents"]
                if p["entityType"] == "user"
            ]
            if not change_user_ids:
                continue

            project_id_change = ent_info["changes"].get("project_id")
            if not project_id_change:
                continue

            project_id = project_id_change["new"] or project_id_change["old"]
            if not project_id:
                continue

            user_id = change_user_ids[0]
            project_ids.add(project_id)
            filtered_changes = project_ids_by_user_id.setdefault(
                user_id, {}
            )
            filtered_changes[project_id] = project_id_change["new"] is None

        if not project_ids_by_user_id:
            return

        joined_project_ids = self.join_query_keys(project_ids)
        projects = session.query(
            "select id, full_name, is_private from Project"
            f" where id in ({joined_project_ids})"
        ).all()
        # Filter only private projects
        # - maybe it could be done in query?
        project_ids_by_name = {
            project["full_name"]: project["id"]
            for project in projects
            if project["is_private"]
        }
        if not project_ids_by_name:
            return

        # Filter only projects that are available in AYON
        ayon_projects_names = {
            ayon_project["name"]
            for ayon_project in ayon_api.get_projects(
                set(project_ids_by_name), fields={"name"}
            )
        }
        filtered_project_ids = {
            project_ids_by_name[project_name]
            for project_name in ayon_projects_names
            if project_name in project_ids_by_name
        }
        if not filtered_project_ids:
            return

        # Filter only artist users
        # - admin and manager users can access all projects in AYON
        ayon_users = [
            user
            for user in ayon_api.get_users()
            if not user["isAdmin"] and not user["isManager"]
        ]
        if not ayon_users:
            return

        joined_user_ids = self.join_query_keys(project_ids_by_user_id)
        ftrack_users = session.query(
            "select id, username, email from User"
            f" where id in ({joined_user_ids})"
        ).all()
        users_mapping: Dict[str, Union[str, None]] = (
            map_ftrack_users_to_ayon_users(ftrack_users, ayon_users)
        )
        project_name_by_id = {
            project_id: project_name
            for project_name, project_id in project_ids_by_name.items()
        }
        for ftrack_id, ayon_username in users_mapping.items():
            # Mapping was not found - ignore
            if not ayon_username:
                continue

            removed_by_project_id = project_ids_by_user_id[ftrack_id]
            filtered_project_ids = {
                project_id
                for project_id in removed_by_project_id
                if project_id in filtered_project_ids
            }
            if not filtered_project_ids:
                continue

            ayon_user = ayon_api.get_user(ayon_username)
            user_data = ayon_user["data"]
            default_user_access_groups = user_data.get(
                "defaultAccessGroups", []
            )
            user_access_groups = user_data.setdefault("accessGroups", {})

            changed = False
            for project_id in filtered_project_ids:
                if project_id not in removed_by_project_id:
                    continue

                removed: bool = removed_by_project_id[project_id]
                project_name = project_name_by_id[project_id]
                if removed and project_name not in user_access_groups:
                    continue

                access_groups = user_access_groups.setdefault(
                    project_name, []
                )
                if removed:
                    if access_groups:
                        access_groups.clear()
                        changed = True

                elif not access_groups:
                    for group in default_user_access_groups:
                        if group not in access_groups:
                            access_groups.append(group)
                            changed = True

            if changed:
                ayon_api.patch(
                    f"users/{ayon_username}",
                    data=user_data
                )
                self.log.info(f"Updated access groups of '{ayon_username}'.")
