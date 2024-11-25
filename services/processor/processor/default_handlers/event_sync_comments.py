import collections
from typing import Optional

import ayon_api

from ftrack_common import (
    BaseEventHandler,
    CUST_ATTR_KEY_SERVER_ID,
    is_ftrack_enabled_in_settings,
    map_ftrack_users_to_ayon_users,
)

# Not used, just for reference
NOTE_KEYS = [
    "project_id",
    "parent_type",
    "parent_id",
    "userid",
    "noteid",
    "noteparentid",
    "is_todo",
    "date",
    "triggerdate",
    "text",
]


class SyncCommentsHandler(BaseEventHandler):
    """Sync comments from ftrack to AYON."""

    subscription_topic: str = "ftrack.update"

    def launch(self, session, event):
        filtered_info = collections.defaultdict(list)
        for ent_info in event["data"]["entities"]:
            if ent_info.get("entityType") != "note":
                continue

            if (
                ent_info["action"] == "update"
                and "text" not in ent_info["changes"]
            ):
                continue

            parents = ent_info["parents"]
            if parents[1]["entityType"] not in ("assetversion", "task"):
                continue

            project_id = ent_info["parents"][-1]["entityId"]
            filtered_info[project_id].append(ent_info)

        for project_id, ents_info in filtered_info.items():
            self._process_notes(session, event, project_id, ents_info)

    def _process_notes(self, session, event, project_id, ents_info):
        project_name = self.get_project_name_from_event(
            session, event, project_id
        )
        project_settings = self.get_project_settings_from_event(
            event, project_name
        )
        ftrack_settings = project_settings["ftrack"]
        if not is_ftrack_enabled_in_settings(ftrack_settings):
            self.log.debug(
                f"ftrack is disabled for project \"{project_name}\""
            )
            return

        ayon_id_attr = session.query(
            "select id, key from CustomAttributeConfiguration"
            f" where key is '{CUST_ATTR_KEY_SERVER_ID}'"
        ).first()
        ayon_id_attr_id = None
        if ayon_id_attr:
            ayon_id_attr_id = ayon_id_attr["id"]

        user_id = event["data"]["user"]["userid"]
        ftrack_user = session.query(
            f"select username, email from User where id is '{user_id}'"
        ).first()
        ayon_username = None
        if ftrack_user:
            mapping = map_ftrack_users_to_ayon_users([ftrack_user])
            ayon_username = mapping[user_id]

        con = ayon_api.get_server_api_connection()
        with con.as_username(ayon_username):
            for ent_info in ents_info:
                parent_info = ent_info["parents"][1]
                parent_entity = self._get_parent_ftrack_entity(
                    session, parent_info
                )
                if parent_entity is None:
                    continue

                ayon_entity, ayon_entity_type = self._get_ayon_entity(
                    session,
                    project_name,
                    parent_entity,
                    ayon_id_attr_id
                )
                if not ayon_entity:
                    continue

                activities = list(ayon_api.get_activities(
                    project_name,
                    activity_types={"comment"},
                    entity_ids={ayon_entity["id"]},
                ))

                if ent_info["action"] == "add":
                    self._handle_added_note(
                        project_name,
                        session,
                        ent_info,
                        ayon_entity,
                        ayon_entity_type,
                        activities,
                    )
                elif ent_info["action"] == "remove":
                    self._handle_removed_note(
                        project_name,
                        ent_info,
                        activities,
                    )
                elif ent_info["action"] == "update":
                    self._handle_updated_note(
                        project_name,
                        ent_info,
                        activities,
                    )

    def _get_parent_ftrack_entity(self, session, parent_info):
        parent_type = parent_info["entityType"]
        parent_id = parent_info["entityId"]
        if parent_type == "assetversion":
            return session.query(
                "select id, asset_id, version from AssetVersion"
                f" where id is '{parent_id}'"
            ).first()
        return session.query(
            "select id, name from TypedContext"
            f" where id is '{parent_id}'"
        ).first()

    def _get_ayon_entity(
        self, session, project_name, ft_entity, ayon_id_attr_id
    ) -> tuple[Optional[dict], str]:
        parent_id = ft_entity["id"]
        value_items = session.query((
            "select value, entity_id, configuration_id"
            " from CustomAttributeValue"
            f" where entity_id is '{parent_id}'"
            f" and configuration_id is '{ayon_id_attr_id}'"
        )).all()
        entity_id = None
        for item in value_items:
            if item["value"]:
                entity_id = item["value"]

        entity_type = ft_entity.entity_type.lower()

        if entity_type == "assetversion":
            ayon_entity_type = "version"
        elif entity_type == "task":
            ayon_entity_type = "task"
        else:
            ayon_entity_type = "folder"

        if entity_id:
            if entity_type == "assetversion":
                return ayon_api.get_version_by_id(
                    project_name, entity_id
                ), ayon_entity_type
            elif entity_type == "task":
                return ayon_api.get_task_by_id(
                    project_name, entity_id
                ), ayon_entity_type
            return ayon_api.get_folder_by_id(
                project_name, entity_id
            ), ayon_entity_type

        if entity_type != "assetversion":
            return None, ayon_entity_type

        asset_id = ft_entity["asset_id"]
        asset = session.query(
            f"select name, context_id from Asset where id is '{asset_id}'"
        ).first()
        if not asset:
            return None, ayon_entity_type
        parent_id = asset["context_id"]
        parent_entity = session.query(
            f"select id from TypedContext where id is '{parent_id}'"
        ).first()
        if not parent_entity:
            return None, ayon_entity_type
        parent_ayon_entity, _ = self._get_ayon_entity(
            session, project_name, parent_entity, ayon_id_attr_id
        )
        if not parent_ayon_entity:
            return None, ayon_entity_type

        expected_product_name = asset["name"].lower()
        matching_product = None
        for product in ayon_api.get_products(
            project_name,
            folder_ids={parent_ayon_entity["id"]},
        ):
            if product["name"].lower() == expected_product_name:
                matching_product = product
                break

        if not matching_product:
            return None, ayon_entity_type

        for version in ayon_api.get_version_by_name(
            project_name,
            version=ft_entity["version"],
            product_id=matching_product["id"],
        ):
            return version, ayon_entity_type
        return None, ayon_entity_type

    def _handle_added_note(
        self,
        project_name,
        session,
        ent_info,
        ayon_entity,
        ayon_entity_type,
        activities,
    ):
        note_id = ent_info["entityId"]
        note = session.query(
            f"select id, metadata from Note where id is '{note_id}'"
        ).first()
        if not note:
            return

        activity_id = note["metadata"].get("ayon_activity_id")
        changes = ent_info["changes"]
        text = changes["text"]["new"]
        ftrack_id_filled = False
        matching_activity = None
        if activity_id:
            matching_activity = next(
                (
                    activity
                    for activity in activities
                    if activity["id"] == activity_id
                ),
                None
            )
            if matching_activity:
                data = matching_activity["activityData"]
                data_id = data.get("ftrack", {}).get("id")
                ftrack_id_filled = data_id == note_id

        if matching_activity is None:
            for activity in activities:
                data = activity["activityData"]
                data_id = data.get("ftrack", {}).get("id")
                if data_id == note_id or activity["body"] == text:
                    matching_activity = activity
                    ftrack_id_filled = data_id == note_id
                    break

        session.commit()
        if matching_activity is None:
            activity_id = ayon_api.create_activity(
                project_name,
                ayon_entity["id"],
                ayon_entity_type,
                "comment",
                body=text,
                data={"ftrack": {"id": note_id}}
            )
            note["metadata"]["ayon_activity_id"] = activity_id
            session.commit()
            return

        if not ftrack_id_filled:
            ft_data = matching_activity["activityData"].setdefault(
                "ftrack", {}
            )
            ft_data["id"] = note_id

            ayon_api.update_activity(
                project_name,
                matching_activity["activityId"],
                data=matching_activity["activityData"]
            )

    def _handle_updated_note(
        self,
        project_name,
        ent_info,
        activities,
    ):
        note_id = ent_info["entityId"]
        changes = ent_info["changes"]
        old_text = changes["text"]["old"]
        text = changes["text"]["new"]
        ftrack_id_filled = False
        matching_activity = None
        for activity in activities:
            data = activity["activityData"]
            data_id = data.get("ftrack", {}).get("id")
            if data_id == note_id or activity["body"] == old_text:
                matching_activity = activity
                ftrack_id_filled = data_id == note_id
                break

        if matching_activity is None:
            return

        kwargs = {}
        if not ftrack_id_filled:
            ft_data = matching_activity["activityData"].setdefault(
                "ftrack", {}
            )
            ft_data["id"] = note_id
            kwargs["data"] = matching_activity["activityData"]

        ayon_api.update_activity(
            project_name,
            matching_activity["activityId"],
            body=text,
            **kwargs
        )

    def _handle_removed_note(
        self,
        project_name,
        ent_info,
        activities,
    ):
        changes = ent_info["changes"]
        note_id = ent_info["entityId"]
        text = changes["text"]["old"]
        matching_activity = None
        for activity in activities:
            data = activity["activityData"]
            data_id = data.get("ftrack", {}).get("id")
            if data_id == note_id or activity["body"] == text:
                matching_activity = activity
                break

        if matching_activity is not None:
            ayon_api.delete_activity(
                project_name, matching_activity["activityId"]
            )
