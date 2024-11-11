import logging
from typing import Dict, Any

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
            self._log.error(f"Unknown topic: '{topic}'")

        except Exception:
            job_status = "failed"

        finally:
            ayon_api.update_event(
                job_event["id"],
                status=job_status
            )

    def _process_reviewable_created(self, source_event: Dict[str, Any]):
        pass

    def _process_entity_event(self, source_event: Dict[str, Any]):
        new_event = self._convert_entity_event(source_event)

    def _convert_entity_event(self, source_event: Dict[str, Any]):
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

        output = {
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

        output["action"] = "update"

        payload = source_event["payload"]
        if change_type == "renamed":
            change_type = "name"
        elif change_type.endswith("_changed"):
            change_type = change_type[:-8]

        changes = {}
        return output
