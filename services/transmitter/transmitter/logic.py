import logging
from typing import Optional, Dict, Any

import ftrack_api
import ayon_api

from ftrack_common import (
    FTRACK_ID_ATTRIB,
    map_ftrack_users_to_ayon_users,
)

from .structures import JobEventType


class EventProcessor:
    def __init__(self, session: ftrack_api.Session):
        self._session = session
        self._log = logging.getLogger(self.__class__.__name__)

    def process_event(
        self,
        source_event: Dict[str, Any],
        job_event: JobEventType,
    ):
        job_status = "finished"
        try:
            topic: str = source_event["topic"]
            if topic == "reviewable.created":
                self._process_reviewable_created(source_event)
            elif topic.startswith("entity"):
                self._process_entity_event(source_event)
            else:
                self._log.error(f"Unknown topic: '{topic}'")

        except Exception:
            job_status = "failed"

        finally:
            ayon_api.update_event(
                job_event["id"],
                status=job_status
            )

    def _get_entity_by_id(self, project_name, entity_type, entity_id):
        if entity_type == "project":
            return ayon_api.get_project(project_name)
        if entity_type == "folder":
            return ayon_api.get_folder_by_id(project_name, entity_id)
        if entity_type == "task":
            return ayon_api.get_task_by_id(project_name, entity_id)
        if entity_type == "product":
            return ayon_api.get_product_by_id(project_name, entity_id)
        if entity_type == "version":
            return ayon_api.get_version_by_id(project_name, entity_id)
        self._log.warning(f"Unsupported entity type: {entity_type}")
        return None

    def _process_reviewable_created(self, source_event: Dict[str, Any]):
        # TODO implement
        pass

    def _process_entity_event(self, source_event: Dict[str, Any]):
        entity_data = self._convert_entity_event(source_event)
        if entity_data is None:
            return

        if entity_data["action"] == "update":
            self._handle_update_event(entity_data)

    def _convert_entity_event(
        self, source_event: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        # TODO find out if this conversion makes sense?
        topic: str = source_event["topic"]
        topic_parts = topic.split(".")
        if len(topic_parts) != 3:
            self._log.warning(
                f"Unexpected topic strucure: '{topic}'."
                " Expected 'entity.<entity_type>.<change_type>'",
            )
        head, entity_type, change_type = topic_parts
        if head != "entity":
            self._log.warning(f"Unexpected topic: {topic}")
            return None

        output: Dict[str, Any] = {
            "project_name": source_event["project"],
            "entity_type": entity_type,
        }
        if change_type == "created":
            output["action"] = "create"
            output["entity_id"] = source_event["summary"]["entityId"]
            return output

        if change_type == "deleted":
            output["action"] = "deleted"
            entity_data = source_event["payload"]["entityData"]
            output["entity_data"] = entity_data
            output["entity_id"] = entity_data["id"]
            return output

        if change_type in (
            "tags", "data", "thumbnail", "active",
        ):
            return None

        output["action"] = "update"
        output["entity_id"] = source_event["summary"]["entityId"]

        payload = source_event["payload"]

        if change_type == "renamed":
            change_type = "name"
        elif change_type.endswith("_changed"):
            change_type = change_type[:-8]

        if change_type == "type":
            if entity_type == "folder":
                change_type = "folderType"
            elif entity_type == "task":
                change_type = "taskType"
            elif entity_type == "product":
                change_type = "productType"

        output["update_key"] = change_type

        if change_type == "attrib":
            output["changes"] = {
                "old": {"attrib": payload["oldValue"]},
                "new": {"attrib": payload["newValue"]},
            }
            return output

        if change_type in (
            "name",
            "label",
            "assignees",
        ):
            output["changes"] = {
                "old": {change_type: payload["oldValue"]},
                "new": {change_type: payload["newValue"]},
            }
            return output

        return None

    def _handle_update_event(self, changes_data):
        entity_type = changes_data["entity_type"]
        # TODO implement all entities
        if entity_type in ("project", "folder", "product", "version"):
            return

        project_name = changes_data["project_name"]
        entity_id = changes_data["entity_id"]
        entity = self._get_entity_by_id(project_name, entity_type, entity_id)
        if entity is None:
            self._log.warning(
                f"Entity with id '{entity_id}'"
                f" not found in Project '{project_name}'"
            )
            return

        # TODO implement more logic
        if (
            entity_type == "task"
            and changes_data["update_key"] == "assignees"
        ):
            self._handle_task_assignees_change(entity, changes_data)
            return
        self._log.debug("Unhandled entity update event")

    def _handle_task_assignees_change(self, entity, changes_data):
        self._log.info("Handling assignees changes.")
        # Find ftrack task entity
        task_ftrack_id = entity["attrib"].get(FTRACK_ID_ATTRIB)
        # QUESTION try to find entity by path?
        if not task_ftrack_id:
            self._log.info("Task is not linked to ftrack.")
            return

        ft_entity = self._session.query(
            f"Task where id is '{task_ftrack_id}'"
        ).first()
        if ft_entity is None:
            self._log.info(
                f"ftack entity with id '{task_ftrack_id}' was not found."
            )
            return

        changes = changes_data["changes"]
        # Split added and removed assignees
        added_assignees = (
            set(changes["new"]["assignees"])
            - set(changes["old"]["assignees"])
        )
        removed_assignees = (
            set(changes["old"]["assignees"])
            - set(changes["new"]["assignees"])
        )

        ft_users = self._session.query(
            "select id, username, email from User"
        ).all()
        ayon_username_by_ft_id = map_ftrack_users_to_ayon_users(ft_users)
        ft_id_by_ay_username = {
            ayon_username: ft_user_id
            for ft_user_id, ayon_username in ayon_username_by_ft_id.items()
            if ayon_username
        }
        # Skip if there is no valid user mapping for changed assignees
        changed_assignees = added_assignees | removed_assignees
        if not set(ft_id_by_ay_username) & changed_assignees:
            self._log.info(
                "Changed assignees in AYON don't have"
                " valid mapping to ftrack users."
            )
            return

        appointments = self._session.query(
            "select resource_id, context_id from Appointment"
            f" where context_id is '{task_ftrack_id}'"
            " and type is 'assignment'"
        ).all()
        appointments_by_user_ids = {
            appointment["resource_id"]: appointment
            for appointment in appointments
        }
        for ayon_username in added_assignees:
            ftrack_id = ft_id_by_ay_username.get(ayon_username)
            if not ftrack_id:
                continue

            if ftrack_id in appointments_by_user_ids:
                self._log.info(
                    f"AYON user '{ayon_username}'"
                    f" is already assigned in ftrack."
                )
                continue

            self._session.create(
                "Appointment",
                {
                    "resource_id": ftrack_id,
                    "context_id": task_ftrack_id,
                    "type": "assignment",
                }
            )
            self._log.info(
                f"Creating assignment of user '{ayon_username}' in ftrack."
            )

        for ayon_username in removed_assignees:
            ftrack_id = ft_id_by_ay_username.get(ayon_username)
            if not ftrack_id:
                continue
            if ftrack_id not in appointments_by_user_ids:
                self._log.info(
                    f"AYON user '{ayon_username}' is not assigned in ftrack."
                )
                continue
            self._session.delete(appointments_by_user_ids[ftrack_id])
            self._log.info(
                f"Removing assignment of user '{ayon_username}' in ftrack."
            )

        if self._session.recorded_operations:
            self._session.commit()
