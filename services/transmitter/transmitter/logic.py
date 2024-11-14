import collections
import logging
import typing
from typing import Optional, Dict, Set, Any, TypedDict, Literal
from dataclasses import dataclass

import ayon_api
import ftrack_api
import ftrack_api.operation

from ftrack_common import (
    FTRACK_ID_ATTRIB,
    map_ftrack_users_to_ayon_users,
    is_ftrack_enabled_in_settings,
    join_filter_values,
    query_custom_attribute_values,
)

from .structures import JobEventType

if typing.TYPE_CHECKING:
    import ftrack_api.entity.base

log = logging.getLogger(__name__)

_NOT_SET = object()


def _get_entity_by_id(project_name, entity_type, entity_id, logger=None):
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
    if logger is None:
        logger = log
    logger.warning(f"Unsupported entity type: {entity_type}")
    return None


class EntityDataChangesData(TypedDict):
    new: Dict[str, Any]
    old: Dict[str, Any]


@dataclass
class EntityEventData:
    action: Literal["created", "updated", "deleted"]
    project_name: str
    entity_type: Literal[
        "project", "folder", "task", "product", "version"
    ]
    entity_id : str
    # 'entity_data' is filled when action is 'deleted'
    entity_data: Optional[Dict[str, Any]]
    # 'update_key' and 'changes' are filled when action is 'updated'
    update_key: Optional[str]
    changes: Optional[EntityDataChangesData]
    _ayon_entity: Optional[Dict[str, Any]] = _NOT_SET

    def get_ayon_entity(self) -> Optional[Dict[str, Any]]:
        if self._ayon_entity is _NOT_SET:
            self._ayon_entity = _get_entity_by_id(
                self.project_name,
                self.entity_type,
                self.entity_id,
            )
        return self._ayon_entity


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
            self._log.info("Processing event: %s", job_event["id"])
            topic: str = source_event["topic"]
            if topic == "reviewable.created":
                self._process_reviewable_created(source_event)
            elif topic.startswith("entity"):
                self._process_entity_event(source_event)
            else:
                self._log.error(f"Unknown topic: '{topic}'")
            self._log.info("Processing finished")

        except Exception:
            self._log.warning("Failed to process event.", exc_info=True)
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
        entity_data: Optional[EntityEventData] = self._convert_entity_event(
            source_event
        )
        if entity_data is None:
            return

        if entity_data.action == "updated":
            self._handle_update_event(entity_data)
        else:
            self._log.info(f"Unhandled action '{entity_data.action}'")

    def _convert_entity_event(
        self, source_event: Dict[str, Any]
    ) -> Optional[EntityEventData]:
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

        project_name = source_event["project"]
        entity_id = None
        if entity_type == "project":
            entity_id = project_name
        update_key = changes = entity_data = None
        if change_type == "created":
            action = "created"
            if entity_id is None:
                entity_id = source_event["summary"]["entityId"]

        elif change_type == "deleted":
            action = "deleted"
            entity_data = source_event["payload"]["entityData"]
            if entity_id is None:
                entity_id = entity_data["id"]
        else:
            action = "updated"
            if entity_id is None:
                entity_id = source_event["summary"]["entityId"]
            update_key, changes = self._prepare_update_data(
                source_event, change_type, entity_type
            )
            if update_key is None or changes is None:
                return None

        return EntityEventData(
            action=action,
            project_name=project_name,
            entity_type=entity_type,
            entity_id=entity_id,
            entity_data=entity_data,
            update_key=update_key,
            changes=changes,
        )

    def _prepare_update_data(self, source_event, change_type, entity_type):
        if change_type in (
            "tags", "data", "thumbnail", "active",
        ):
            return None, None

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

        changes = None
        if change_type == "attrib":
            changes = {
                "old": {"attrib": payload["oldValue"]},
                "new": {"attrib": payload["newValue"]},
            }

        if change_type in (
            "name",
            "label",
            "assignees",
            "status",
        ):
            changes = {
                "old": {change_type: payload["oldValue"]},
                "new": {change_type: payload["newValue"]},
            }

        return change_type, changes

    def _handle_update_event(self, entity_data: EntityEventData):
        entity_type = entity_data.entity_type
        self._log.info(
            f"Entity {entity_type} <{entity_data.entity_id}> changed"
            f" in project {entity_data.project_name}"
        )
        # TODO implement all entities
        if entity_type in ("project", "product"):
            self._log.info(
                f"Unhandled change of entity type '{entity_type}'."
            )
            return

        entity = entity_data.get_ayon_entity()
        if entity is None:
            self._log.warning(
                f"Entity with id '{entity_data.entity_id}'"
                f" not found in Project '{entity_data.project_name}'"
            )
            return

        project_name = entity_data.project_name
        project_settings = ayon_api.get_addons_settings(
            project_name=project_name
        )
        if not is_ftrack_enabled_in_settings(project_settings["ftrack"]):
            self._log.info(
                f"Project '{project_name}' is disabled for ftrack."
            )
            return

        # TODO implement more logic
        if entity_data.update_key == "assignees":
            if entity_type == "task":
                self._handle_task_assignees_change(entity_data)
        elif entity_data.update_key == "status":
            self._handle_status_change(entity_data)
        elif entity_data.update_key == "attrib":
            self._handle_attrib_change(entity_data)
        else:
            self._log.info("Unhandled entity update event")

    def _find_ftrack_entity(
        self,
        project_name: str,
        entity_type: str,
        entity_data: Dict[str, Any],
        fields: Optional[Set[str]] = None,
    ):
        ft_entity_type = None
        type_fields = set()
        if entity_type == "version":
            ft_entity_type = "AssetVersion"
        elif entity_type in {"task", "folder"}:
            ft_entity_type = "TypedContext"
            type_fields = {"type_id", "object_type_id"}

        if ft_entity_type is None:
            return

        if fields is None:
            fields = {"id"}
        else:
            fields.add("id")

        fields |= type_fields

        ft_entity = None
        ftrack_id = entity_data["attrib"].get(FTRACK_ID_ATTRIB)
        if ftrack_id:
            joined_fields = ", ".join(fields)
            ft_entity = self._session.query(
                f"select {joined_fields} from {ft_entity_type}"
                f" where id is '{ftrack_id}'"
            ).first()

        if ft_entity is not None:
            return ft_entity

        if entity_type != "version":
            return None

        product_id = entity_data["productId"]
        product_entity = ayon_api.get_product_by_id(
            project_name,
            product_id,
            fields={"name", "productType", "folderId"},
        )
        folder_entity = ayon_api.get_folder_by_id(
            project_name,
            product_entity["folderId"],
            fields={"attrib"},
        )
        folder_ft_entity = self._find_ftrack_entity(
            project_name,
            "folder",
            folder_entity
        )
        if folder_ft_entity is None:
            return None

        product_name = product_entity["name"].lower()
        folder_id = folder_ft_entity["id"]
        assets = self._session.query(
            "select id, name from Asset"
            f" where parent_id is '{folder_id}'"
        ).all()
        matching_asset = None
        for asset in assets:
            if asset["name"].lower() == product_name:
                matching_asset = asset
                break

        if matching_asset is None:
            return None

        asset_id = matching_asset["id"]

        joined_fields = ", ".join(fields)
        version = entity_data["version"]
        return self._session.query(
            f"select {joined_fields} from {ft_entity_type}"
            f" where asset_id is '{asset_id}' and version is {version}"
        ).first()

    def _get_ftrack_entity(
        self,
        entity_data: EntityEventData,
        fields: Optional[Set[str]] = None,
    ):
        return self._find_ftrack_entity(
            entity_data.project_name,
            entity_data.entity_type,
            entity_data.get_ayon_entity(),
            fields
        )

    def _handle_status_change(self, entity_data: EntityEventData):
        self._log.info("Handling status changes.")
        # Status on project and product cannot be changed
        if entity_data.entity_type in {"project", "product"}:
            return

        ftrack_project = self._session.query(
            "select id, project_schema_id from Project"
            f" where full_name is '{entity_data.project_name}'"
        ).first()
        if not ftrack_project:
            self._log.info(
                f"Project '{entity_data.project_name}' not found in ftrack."
            )
            return

        new_status = entity_data.changes["new"]["status"]
        status_by_id = {
            status["id"]: status["name"]
            for status in self._session.query(
                "select id, name from Status"
            ).all()
        }
        filtered_statuses = {
            status_id: status_name
            for status_id, status_name in status_by_id.items()
            if status_name.lower() == new_status.lower()
        }
        if not filtered_statuses:
            self._log.info(
                f"Status '{new_status}' is not found in ftrack."
            )
            return

        ft_entity = self._get_ftrack_entity(entity_data, {"status_id"})
        if ft_entity is None:
            self._log.info("Entity was not found in ftrack.")
            return

        status_id = ft_entity["status_id"]
        # Status is already set
        if filtered_statuses.get(status_id):
            return

        status_ids = self._get_available_ft_statuses(
            ft_entity, ftrack_project["project_schema_id"]
        )
        for status_id in status_ids:
            if filtered_statuses.get(status_id):
                self._log.info("Setting new status in ftrack.")
                ft_entity["status_id"] = status_id
                self._session.commit()
                return

        self._log.info(
            f"Status '{new_status}' is not available for ftrack entity."
        )

    def _get_available_ft_statuses(
        self,
        ft_entity: "ftrack_api.entity.base.Entity",
        project_schema_id: str,
    ) -> Set[str]:
        is_version = is_folder = False
        if ft_entity.entity_type.lower() == "assetversion":
            is_version = True
            fields = {"asset_version_workflow_schema"}
        elif ft_entity.entity_type.lower() == "task":
            fields = {
                "task_workflow_schema",
                "task_workflow_schema_overrides",
            }
        else:
            is_folder = True
            fields = {"object_type_schemas"}

        joined_fields = ", ".join(fields)
        project_schema = self._session.query(
            f"select {joined_fields} from ProjectSchema"
            f" where id is '{project_schema_id}'"
        ).first()

        if is_version:
            av_workflow_schema_id = (
                project_schema["asset_version_workflow_schema"]["id"]
            )
            workflow_statuses = self._session.query(
                "select status_id"
                " from WorkflowSchemaStatus"
                f" where workflow_schema_id is '{av_workflow_schema_id}'"
            ).all()
            return {
                status["id"]
                for status in workflow_statuses
            }

        if is_folder:
            object_type_id = ft_entity["object_type_id"]
            schema_ids = {
                schema["id"]
                for schema in project_schema["object_type_schemas"]
            }
            joined_ids = join_filter_values(schema_ids)
            schema = self._session.query(
                "select id, object_type_id from Schema"
                f" where id in ({joined_ids})"
                f" and object_type_id is '{object_type_id}'"
            ).first()
            if not schema:
                return set()

            schema_id = schema["id"]
            schema_statuses = self._session.query(
                "select status_id from SchemaStatus"
                f" where schema_id is '{schema_id}'"
            ).all()
            return {
                status["status_id"]
                for status in schema_statuses
            }

        type_id = ft_entity["type_id"]
        task_workflow_override_ids = {
            task_override["id"]
            for task_override in
            project_schema["task_workflow_schema_overrides"]
        }
        joined_ids = join_filter_values(task_workflow_override_ids)
        overrides_schema = self._session.query(
            "select workflow_schema_id"
            f" from ProjectSchemaOverride"
            f" where id in ({joined_ids}) and type_id is '{type_id}'"
        ).first()
        workflow_id = project_schema["task_workflow_schema"]["id"]
        if overrides_schema is not None:
            workflow_id = overrides_schema["workflow_schema_id"]
        workflow_statuses = self._session.query(
            "select status_id"
            " from WorkflowSchemaStatus"
            f" where workflow_schema_id is '{workflow_id}'"
        ).all()
        return {
            item["status_id"]
            for item in workflow_statuses
        }

    def _handle_task_assignees_change(self, entity_data: EntityEventData):
        self._log.info("Handling assignees changes.")
        entity = entity_data.get_ayon_entity()
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

        changes = entity_data.changes
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

    def _handle_attrib_change(self, entity_data: EntityEventData):
        new_attribs = entity_data.changes["new"]["attrib"]

        # TODO handle specific cases of AYON attributes that are not
        #   custom attributes in ftrack (e.g. description)
        fields = {
            "id",
            "key",
            "entity_type",
            "object_type_id",
            "is_hierarchical"
        }
        joined_fields = ", ".join(fields)
        joined_keys = join_filter_values(new_attribs)

        attr_configs = self._session.query(
            f"select {joined_fields}"
            " from CustomAttributeConfiguration"
            f" where key in ({joined_keys})"
        ).all()
        attr_configs_by_key = collections.defaultdict(list)
        for attr_config in attr_configs:
            attr_configs_by_key[attr_config["key"]].append(attr_config)

        missing = set(new_attribs) - set(attr_configs_by_key)
        if missing:
            joined_missing = ", ".join([f'"{key}"' for key in missing])
            self._log.info(
                f"Attributes {joined_missing} not found in ftrack."
            )

        if not attr_configs:
            return

        ft_entity = self._get_ftrack_entity(entity_data)
        if ft_entity is None:
            self._log.info("Entity was not found in ftrack.")
            return

        filtered_attr_confs = {}
        valid_conf_ids = set()
        for key, attr_confs in attr_configs_by_key.items():
            valid_confs = []
            for attr_conf in attr_confs:
                if self._is_attr_conf_valid(attr_conf, entity_data):
                    valid_confs.append(attr_conf)
                    valid_conf_ids.add(attr_conf["id"])

            if valid_confs:
                filtered_attr_confs[key] = valid_confs

        if not filtered_attr_confs:
            return

        value_items = query_custom_attribute_values(
            self._session,
            valid_conf_ids,
            {ft_entity["id"]}
        )
        values_by_attr_id = {
            value_item["configuration_id"]: value_item["value"]
            for value_item in value_items
        }
        ayon_entity = entity_data.get_ayon_entity()
        any_changed = False
        for key, attr_confs in filtered_attr_confs.items():
            new_value = new_attribs[key]
            for attr_conf in attr_confs:
                attr_id = attr_conf["id"]
                is_new = attr_id not in values_by_attr_id
                old_value = values_by_attr_id.get(attr_id)
                if new_value is None and not attr_conf["is_hierarchical"]:
                    # NOTE Hack, non-hierarchical attributes will be set
                    #   to current value on entity if new value is 'None'
                    new_value = ayon_entity["attrib"][key]
                if new_value == old_value:
                    continue

                any_changed = True
                op = self._get_ft_attr_value_operation(
                    attr_id,
                    ft_entity["id"],
                    is_new,
                    new_value,
                    old_value
                )
                self._session.recorded_operations.push(op)

        if any_changed:
            try:
                self._session.commit()
            finally:
                self._session.recorded_operations.clear()

    def _is_attr_conf_valid(self, attr_conf, entity_data):
        if attr_conf["is_hierarchical"]:
            return True

        if attr_conf["entity_type"] == "context":
            return entity_data.entity_type in ("project", "folder", "task")

        if attr_conf["entity_type"] == "show":
            return entity_data.entity_type == "project"

        if attr_conf["entity_type"] == "asset":
            return entity_data.entity_type == "product"

        if attr_conf["entity_type"] == "assetversion":
            return entity_data.entity_type == "version"

        if (
            attr_conf["entity_type"] != "task"
            or entity_data.entity_type not in ("folder", "task")
        ):
            return False

        obj_type_id = attr_conf["object_type_id"]
        object_type = self._session.query(
            f"select id, name from ObjectType where id is '{obj_type_id}'"
        ).first()
        if object_type is None:
            return False

        obj_name = object_type["name"].lower()
        if entity_data.entity_type == "task":
            return obj_name == "task"

        ayon_entity = entity_data.get_ayon_entity()
        if ayon_entity:
            return ayon_entity["folderType"].lower() == obj_name
        return False

    def _get_ft_attr_value_operation(
        self,
        conf_id: str,
        entity_id: str,
        is_new: bool,
        new_value: Any,
        old_value: Optional[Any] = None,
    ):
        entity_key = collections.OrderedDict((
            ("configuration_id", conf_id),
            ("entity_id", entity_id)
        ))
        if is_new:
            return ftrack_api.operation.CreateEntityOperation(
                "CustomAttributeValue",
                entity_key,
                {"value": new_value}
            )

        if new_value is None:
            return ftrack_api.operation.DeleteEntityOperation(
                "CustomAttributeValue",
                entity_key
            )

        return ftrack_api.operation.UpdateEntityOperation(
            "CustomAttributeValue",
            entity_key,
            "value",
            old_value,
            new_value
        )
