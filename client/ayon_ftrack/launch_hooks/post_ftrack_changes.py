import os

import ayon_api
import ftrack_api

from ayon_ftrack.common import (
    FTRACK_ID_ATTRIB,
    is_ftrack_enabled_in_settings,
)

from openpype.settings import get_project_settings
from openpype.lib.applications import PostLaunchHook
try:
    # Backwards compatibility
    # TODO remove in next minor version bump (after 0.3.x)
    from openpype.lib.applications import LaunchTypes
    local_launch_type = LaunchTypes.local
except Exception:
    local_launch_type = "local"


class PostFtrackHook(PostLaunchHook):
    order = None
    launch_types = {local_launch_type}

    def execute(self):
        project_name = self.data.get("project_name")
        project_settings = self.data.get("project_settings")
        folder_path = self.data.get("asset_name")
        task_name = self.data.get("task_name")

        missing_context_keys = [
            key
            for value, key in (
                (project_name, "project_name"),
                (project_settings, "project_settings"),
                (folder_path, "asset_name"),
                (task_name, "task_name"),
            )
            if not value
        ]
        if missing_context_keys:
            missing_keys_str = ", ".join([
                f"'{key}'" for key in missing_context_keys
            ])
            self.log.debug(
                f"Hook {self.__class__.__name__} skipped."
                f" Missing required data: {missing_keys_str}"
            )
            return

        if "ftrack" not in project_settings:
            self.log.debug(
                "Missing ftrack settings. Skipping post launch logic."
            )
            return

        if not is_ftrack_enabled_in_settings(project_settings["ftrack"]):
            self.log.debug(
                f"Ftrack is disabled for project '{project_name}'. Skipping."
            )
            return

        required_keys = ("FTRACK_SERVER", "FTRACK_API_USER", "FTRACK_API_KEY")
        for key in required_keys:
            if not os.environ.get(key):
                self.log.debug(
                    f"Missing required environment '{key}'"
                    " for ftrack post launch procedure."
                )
                return

        try:
            session = ftrack_api.Session(auto_connect_event_hub=False)
            self.log.debug("ftrack session created")
        except Exception:
            self.log.warning("Couldn't create ftrack session")
            return

        try:
            entity = self._find_ftrack_task_entity(
                session, project_name, folder_path, task_name
            )
            if entity:
                self.ftrack_status_change(session, entity, project_name)

        except Exception:
            self.log.warning(
                "Couldn't finish ftrack post launch logic.",
                exc_info=True
            )
            return

        finally:
            session.close()

    def ftrack_status_change(self, session, entity, project_name):
        project_settings = get_project_settings(project_name)
        status_update = (
            project_settings
            ["ftrack"]
            ["post_launch_hook"]
        )
        if not status_update["enabled"]:
            self.log.debug(
                f"Status changes are disabled for project '{project_name}'"
            )
            return

        status_mapping = status_update["mapping"]
        if not status_mapping:
            self.log.warning(
                f"Project '{project_name}' does not have set status changes."
            )
            return

        actual_status = entity["status"]["name"].lower()
        already_tested = set()
        ent_path = "/".join(
            [ent["name"] for ent in entity["link"]]
        )
        # TODO refactor
        while True:
            next_status_name = None
            for item in status_mapping:
                new_status = item["name"]
                if new_status in already_tested:
                    continue

                from_statuses = item["value"]
                if (
                    actual_status in from_statuses
                    or "__any__" in from_statuses
                ):
                    if new_status != "__ignore__":
                        next_status_name = new_status
                        already_tested.add(new_status)
                    break
                already_tested.add(new_status)

            if next_status_name is None:
                break

            status = session.query(
                f"Status where name is \"{next_status_name}\""
            ).first()
            if status is None:
                self.log.warning(
                    f"Status '{next_status_name}' not found in ftrack."
                )
                continue

            try:
                entity["status_id"] = status["id"]
                session.commit()
                self.log.debug(
                    f"Status changed to \"{next_status_name}\" <{ent_path}>"
                )
                break

            except Exception:
                session.rollback()
                self.log.warning(
                    f"Status '{next_status_name}' is not available"
                    f" for ftrack entity type '{entity.entity_type}'"
                )

    def _find_ftrack_folder_entity(self, session, folder):
        """
        Args:
            session (ftrack_api.Session): Ftrack session.
            folder (dict): AYON folder data.

        Returns:
            Union[ftrack_api.entity.base.Entity, None]: Ftrack folder entity.
        """

        # Find ftrack entity by id stored on folder
        # - Maybe more options could be used? Find ftrack entity by folder
        #   path in ftrack custom attributes.
        if folder:
            ftrack_id = folder["attrib"].get(FTRACK_ID_ATTRIB)
            if ftrack_id:
                entity = session.query(
                    f"TypedContext where id is \"{ftrack_id}\""
                ).first()
                if entity:
                    return entity
        return None

    def _find_ftrack_task_entity(
        self, session, project_name, folder_path, task_name
    ):
        """

        Args:
            session (ftrack_api.Session): Ftrack session.
            project_name (str): Project name.
            folder_path (str): Folder path.
            task_name (str): Task name.

        Returns:
            Union[ftrack_api.entity.base.Entity, None]: Ftrack task entity.
        """

        project_entity = session.query(
            f"Project where full_name is \"{project_name}\""
        ).first()
        if not project_entity:
            self.log.warning(
                f"Couldn't find project '{project_name}' in ftrack."
            )
            return None

        # TODO use 'folder' entity data from launch context when is available.
        #   At this moment there is only "asset doc".
        folder = ayon_api.get_folder_by_path(project_name, folder_path)
        parent_entity = self._find_ftrack_folder_entity(session, folder)
        if parent_entity is None:
            self.log.warning(
                f"Couldn't find folder '{folder_path}' in ftrack"
                f" project '{project_name}'."
            )
            return None

        parent_id = parent_entity["id"]
        task_entity = session.query(
            f"Task where parent_id is '{parent_id}' and name is '{task_name}'"
        ).first()
        if task_entity is None:
            self.log.warning(
                f"Couldn't find task '{folder_path}/{task_name}' in ftrack."
            )

        return task_entity
