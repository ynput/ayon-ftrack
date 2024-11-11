import logging
from typing import Optional, Dict, Any

import ftrack_api
import ayon_api

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

    def _process_reviewable_created(self, source_event: Dict[str, Any]):
        # TODO implement
        pass

    def _process_entity_event(self, source_event: Dict[str, Any]):
        converted_data = self._convert_entity_event(source_event)
        if converted_data is None:
            return

    def _convert_entity_event(
        self, source_event: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
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
