import collections
import uuid
import json

import ayon_api
import ftrack_api

from ftrack_common import (
    ServerAction,
    get_service_ftrack_icon_url,
    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
    get_ayon_attr_configs,
    query_custom_attribute_values,
)
from processor.lib import SyncFromFtrack


class PrepareProjectServer(ServerAction):
    """Prepare project attributes in Anatomy."""

    default_preset_name = "__default__"
    identifier = "prepare.project.server"
    label = "AYON Admin"
    variant = "- Prepare Project (Server)"
    description = "Set basic attributes on the project"
    icon = get_service_ftrack_icon_url("AYONAdmin.svg")

    settings_key = "prepare_project"

    role_list = ["Pypeclub", "Administrator", "Project Manager"]

    settings_key = "prepare_project"

    item_splitter = {"type": "label", "value": "---"}
    _keys_order = (
        "fps",
        "frameStart",
        "frameEnd",
        "handleStart",
        "handleEnd",
        "clipIn",
        "clipOut",
        "resolutionHeight",
        "resolutionWidth",
        "pixelAspect",
        "applications",
        "tools_env",
        "library_project",
    )

    def discover(self, session, entities, event):
        """Show only on project."""
        if (
            len(entities) != 1
            or entities[0].entity_type.lower() != "project"
        ):
            return False

        return self.valid_roles(session, entities, event)

    def _get_list_items(self, attr_name, attr_def, default):
        default = default or []
        title = attr_def["title"] or attr_name
        output = [
            {
                "type": "label",
                "value": "---"
            },
            {
                "type": "label",
                "value": f"Attribute '{title}' selection:"
            }
        ]
        mapping = {}
        for item in attr_def["enum"]:
            value = item["value"]
            name = uuid.uuid4().hex
            mapping[name] = value
            output.append({
                "type": "boolean",
                "label": item["label"],
                "name": name,
                "value": value in default
           })
        output.append({
            "type": "hidden",
            "value": json.dumps(mapping),
            "name": f"attr_list_{attr_name}"
        })
        return output

    def interface(self, session, entities, event):
        event_values = event["data"].get("values")

        project_entity = entities[0]
        project_name = project_entity["full_name"]
        syncer = SyncFromFtrack(session, project_name, self.log)
        if syncer.project_exists_in_ayon():
            return {
                "message": "Project already exists in Ayon.",
                "success": True
            }

        self.show_message(event, "Preparing data... Please wait", True)
        if not event_values:
            # Inform user that this may take a while
            self.log.debug("Preparing data which will be shown")

            primary_preset = self.default_preset_name
            anatomy_presets = [
                {"label": "Default", "value": self.default_preset_name}
            ]
            for anatomy_preset in ayon_api.get_project_anatomy_presets():
                name = anatomy_preset["name"]
                anatomy_presets.append({"label": name, "value": name})
                if anatomy_preset["primary"]:
                    primary_preset = name

            return {
                "title": "Choose Anatomy Preset",
                "submit_button_label": "Prepare project",
                "items": [
                    {
                        "type": "hidden",
                        "name": "in_attribute_set",
                        "value": False
                    },
                    {
                        "type": "label",
                        "value": "### Choose Anatomy Preset"
                    },
                    {
                        "label": "Anatomy Preset",
                        "type": "enumerator",
                        "name": "anatomy_preset",
                        "data": anatomy_presets,
                        "value": primary_preset
                    },
                    {
                        "label": "Modify attributes",
                        "type": "boolean",
                        "name": "modify_attributes",
                        "value": False
                    },
                    {
                        "label": "Sync project",
                        "type": "boolean",
                        "name": "sync_project",
                        "value": True
                    }
                ]
            }

        # Exit interface once attributes are confirmed
        if event_values["in_attribute_set"]:
            return

        # User did not want to modify default attributes
        modify_attributes = event_values["modify_attributes"]
        if not event_values["modify_attributes"]:
            return

        anatomy_preset = event_values["anatomy_preset"]
        attribute_items = [
            {
                "type": "hidden",
                "name": "in_attribute_set",
                "value": True
            },
            {
                "type": "hidden",
                "name": "anatomy_preset",
                "value": anatomy_preset
            },
            {
                "type": "hidden",
                "name": "sync_project",
                "value": event_values["sync_project"]
            },
            {
                "type": "hidden",
                "name": "modify_attributes",
                "value": modify_attributes
            },
            {
                "type": "label",
                "value": "### Change default attributes"
            }
        ]
        if anatomy_preset == self.default_preset_name:
            anatomy_preset = None
        anatomy_preset_values = ayon_api.get_project_anatomy_preset(
            anatomy_preset)
        anatomy_attribute_values = anatomy_preset_values["attributes"]
        project_attributes = ayon_api.get_attributes_for_type("project")
        unknown_attributes = []
        list_attr_defs = []
        for attr_name, attr_def in project_attributes.items():
            if attr_name in (
                FTRACK_ID_ATTRIB,
                FTRACK_PATH_ATTRIB,
                "startDate",
                "endDate",
                "description",
            ):
                continue
            attr_type = attr_def["type"]
            default = anatomy_attribute_values.get(attr_name)
            if default is None:
                default = attr_def["default"]

            # Not sure how to show this
            if attr_type == "list_of_integers":
                continue

            # List of strings is handled differently
            if attr_type == "list_of_strings":
                list_attr_defs.append((attr_name, attr_def, default))
                continue

            item_base = {
                "name": f"attr_{attr_name}",
                "label": attr_def["title"] or attr_name,
                "value": default
            }
            if attr_type in ("float", "integer",):
                item_base["type"] = "number"

            elif attr_type == "datetime":
                item_base["type"] = "date"

            elif attr_type == "boolean":
                item_base["type"] = "boolean"

            elif attr_type == "string":
                item_base["type"] = "text"

            elif attr_type in ("list_of_strings", "list_of_integers"):
                item_base["type"] = "enumerator"
                item_base["data"] = attr_def["enum"]

            else:
                unknown_attributes.append({
                    "type": "label",
                    "value": f"{attr_name}: {attr_type}"
                })
                self.log.info("Unknown attribute type: {}".format(attr_type))
                continue
            attribute_items.append(item_base)

        for item in list_attr_defs:
            attribute_items.extend(self._get_list_items(*item))

        if unknown_attributes:
            attribute_items.append(
                {"type": "label", "value": "Unknown types"}
            )
            attribute_items.extend(unknown_attributes)

        return {
            "title": "Modify attributes",
            "submit_button_label": "Prepare project",
            "items": attribute_items
        }

    def _set_ftrack_attributes(self, session, project_entity):
        ayon_project = ayon_api.get_project(project_entity["full_name"])
        custom_attrs, hier_custom_attrs = get_ayon_attr_configs(session)
        project_attrs = [
            attr
            for attr in custom_attrs
            if attr["entity_type"] == "show"
        ]
        hier_attrs_by_name = {
            attr["key"]: attr for attr in hier_custom_attrs
        }
        attrs_by_name = {
            attr["key"]: attr for attr in project_attrs
        }

        attr_ids = {
            attr["id"]
            for attr in project_attrs
        } | {
            attr["id"]
            for attr in hier_custom_attrs
        }
        value_items = query_custom_attribute_values(
            session, attr_ids, [project_entity["id"]]
        )
        values_by_attr_id = {}
        for value_item in value_items:
            value = value_item["value"]
            attr_id = value_item["configuration_id"]
            values_by_attr_id[attr_id] = value

        for attr_name, attr_value in ayon_project["attrib"].items():
            attrs = [
                attrs_by_name.get(attr_name),
                hier_attrs_by_name.get(attr_name)
            ]
            for attr in attrs:
                if attr is None:
                    continue
                attr_id = attr["id"]
                is_new = attr_id not in values_by_attr_id
                current_value = values_by_attr_id.get(attr_id)

                entity_key = collections.OrderedDict((
                    ("configuration_id", attr_id),
                    ("entity_id", project_entity["id"])
                ))
                op = None
                if is_new:
                    op = ftrack_api.operation.CreateEntityOperation(
                        "CustomAttributeValue",
                        entity_key,
                        {"value": attr_value}
                    )

                elif current_value != attr_value:
                    op = ftrack_api.operation.UpdateEntityOperation(
                        "CustomAttributeValue",
                        entity_key,
                        "value",
                        current_value,
                        attr_value
                    )

                if op is not None:
                    session.recorded_operations.push(op)

        if session.recorded_operations:
            session.commit()

    def launch(self, session, entities, event):
        event_values = event["data"].get("values")
        if not event_values:
            return

        project_entity = entities[0]
        project_name = project_entity["full_name"]
        syncer = SyncFromFtrack(session, project_name, self.log)
        if syncer.project_exists_in_ayon():
            return {
                "message": "Project already exists in Ayon.",
                "success": True
            }

        attributes = {}
        if event_values["modify_attributes"]:
            list_mapping = {}
            for key, value in event_values.items():
                if key.startswith("attr_list_"):
                    attr_name = key[10:]
                    list_mapping[attr_name] = json.loads(value)
                elif key.startswith("attr_"):
                    attributes[key[5:]] = value

            for attr_name, mapping in list_mapping.items():
                final_value = []
                for item_id, value in mapping.items():
                    item_value = event_values[item_id]
                    if item_value:
                        final_value.append(value)

                attributes[attr_name] = final_value

        anatomy_preset = event_values["anatomy_preset"]
        if anatomy_preset == self.default_preset_name:
            anatomy_preset = None
        syncer.create_project(anatomy_preset, attributes)
        self._set_ftrack_attributes(session, project_entity)

        if event_values["sync_project"]:
            event_data = {
                "actionIdentifier": "sync.to.avalon.server",
                "selection": [{
                    "entityId": project_entity["id"],
                    "entityType": "show"
                }]
            }
            user = session.query(
                "User where username is \"{}\"".format(session.api_user)
            ).one()
            user_data = {
                "username": user["username"],
                "id": user["id"]
            }
            self.trigger_event(
                "ftrack.action.launch",
                event_data=event_data,
                session=session,
                source=user_data,
                event=event,
                on_error="ignore"
            )

        report_items = syncer.report_items
        if report_items:
            self.show_interface(
                report_items,
                title="Prepare Project report",
                event=event
            )
        self.log.info(f"Project '{project_name}' prepared")
        return {
            "message": "Project created in Ayon.",
            "success": True
        }


def register(session):
    PrepareProjectServer(session).register()