import collections
import uuid
import json
import copy

import ayon_api
import ftrack_api

from ftrack_common import (
    CUST_ATTR_AUTO_SYNC,
    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
    ServerAction,
    get_service_ftrack_icon_url,
    get_ayon_attr_configs,
    query_custom_attribute_values,
    map_ftrack_users_to_ayon_users,
)

from processor.lib import SyncFromFtrack


class PrepareProjectServer(ServerAction):
    """Prepare project attributes in Anatomy."""

    default_preset_name = "__default__"
    identifier = "ayon.prepare.project.server"
    label = "AYON Admin"
    variant = "- Prepare Project for AYON"
    description = "Set basic attributes on the project"
    icon = get_service_ftrack_icon_url("AYONAdmin.svg")

    role_list = ["Administrator", "Project Manager"]

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

    def _get_autosync_value(self, session, project_entity):
        custom_attrs, _ = get_ayon_attr_configs(session)
        auto_sync_attr = None
        for attr in custom_attrs:
            if (
                attr["entity_type"] == "show"
                and attr["key"] == CUST_ATTR_AUTO_SYNC
            ):
                auto_sync_attr = attr
                break

        if auto_sync_attr is None:
            return None

        project_id = project_entity["id"]
        attr_id = auto_sync_attr["id"]
        value_items = query_custom_attribute_values(
            session, [attr_id], [project_id]
        )
        for item in value_items:
            value = item["value"]
            if value is not None:
                return value
        return True

    def _first_interface(self, session, project_entity):
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

        ayon_autosync_value = self._get_autosync_value(
            session, project_entity)
        items = [
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
                "label": "AYON Anatomy Preset",
                "type": "enumerator",
                "name": "anatomy_preset",
                "data": anatomy_presets,
                "value": primary_preset
            }
        ]
        if ayon_autosync_value is not None:
            items.append({
                "label": "Enable auto-sync",
                "type": "boolean",
                "name": "auto_sync_project",
                "value": ayon_autosync_value
            })
        else:
            items.append({
                "type": "hidden",
                "name": "auto_sync_project",
                "value": False
            })

        return {
            "title": "Choose AYON Anatomy Preset",
            "submit_button_label": "Continue",
            "items": items
        }

    def _attributes_interface(self, event_values):
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
                "name": "auto_sync_project",
                "value": event_values["auto_sync_project"]
            },
            {
                "type": "label",
                "value": (
                    "<b>You can validate or change your default"
                    " project attributes.</b>"
                )
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
            "title": "Default project attributes",
            "submit_button_label": "Confirm",
            "items": attribute_items
        }

    def interface(self, session, entities, event):
        event_values = event["data"].get("values")

        project_entity = entities[0]
        result = self._slugify_name_handling(session, event, project_entity)
        if result is not None:
            return result

        # Check if project already exists
        # TODO maybe this should be handled with slugify? Give option to
        #   change name/code.
        project_codes = set()
        project_names = set()
        for project in ayon_api.get_projects(fields={"name", "code"}):
            project_codes.add(project["code"])
            project_names.add(project["name"])

        project_name = project_entity["full_name"]
        project_code = project_entity["name"]
        exists_error = None
        if project_name in project_names:
            exists_error = f"name '{project_name}'"
        elif project_code in project_codes:
            exists_error = f"code '{project_code}'"

        if exists_error:
            return {
                "message": f"Project {exists_error} already exists in AYON.",
                "success": True
            }

        self.show_message(event, "Preparing data... Please wait", True)
        if not event_values or "in_attribute_set" not in event_values:
            return self._first_interface(session, project_entity)

        # Exit interface once attributes are confirmed
        if event_values["in_attribute_set"]:
            return

        # User did not want to modify default attributes
        return self._attributes_interface(event_values)

    def _rename_project_handling(self, session, event, project_entity):
        """

        Args:
            session (ftrack_api.Session): ftrack session.
            event (ftrack_api.event.base.Event): Event entity.
            project_entity (ftrack_api.entity.base.Entity): Project entity.

        Returns:
            Union[None, Dict[str, Any]]: None if both name and code are valid,
                otherwise returns interface items or ending messages.
        """

        event_values = event["data"].get("values") or {}
        action = event_values.get("invalid_name_action")
        if action is None or action == "skip_prep":
            return {
                "success": False,
                "message": "Project was <b>not</b> prepared in AYON."
            }

        new_name = event_values.get("new_project_name")
        new_slugified_name = None
        if new_name is not None:
            new_slugified_name = ayon_api.slugify_string(new_name)

        new_code = event_values.get("new_project_code")
        new_slugified_code = None
        if new_code is not None:
            new_slugified_code = ayon_api.slugify_string(new_code)

        name_is_valid = new_slugified_name == new_name
        code_is_valid = new_slugified_code == new_code
        if not name_is_valid or not code_is_valid:
            return self._get_rename_project_items(
                project_entity,
                new_slugified_name,
                new_slugified_code,
                new_name,
                new_code,
            )

        if new_slugified_name is not None:
            project_entity["full_name"] = new_slugified_name
        if new_slugified_code is not None:
            project_entity["name"] = new_slugified_code
        session.commit()
        return None

    def _slugify_name_handling(self, session, event, project_entity):
        """

        Args:
            session (ftrack_api.Session): ftrack session.
            event (ftrack_api.event.base.Event): Event entity.
            project_entity (ftrack_api.entity.base.Entity): Project entity.

        Returns:
            Union[None, Dict[str, Any]]: None if both name and code are valid,
                otherwise returns interface items or ending messages.
        """

        # TODO validate project code too
        project_name = project_entity["full_name"]
        project_code = project_entity["name"]
        slugified_name = ayon_api.slugify_string(project_name)
        slugified_code = ayon_api.slugify_string(project_code)

        if slugified_name == project_name:
            slugified_name = None

        if slugified_code == project_code:
            slugified_code = None

        # Both name and code are valid
        if (
            slugified_name is None
            and slugified_code is None
        ):
            return None

        # Validate user inputs
        if event["data"].get("values"):
            return self._rename_project_handling(
                session, event, project_entity
            )

        # Show interface to user
        return self._get_rename_project_items(
            project_entity,
            slugified_name,
            slugified_code,
        )

    def _get_rename_project_items(
        self,
        project_entity,
        new_name_hint,
        new_code_hint,
        new_name=None,
        new_code=None,
    ):
        """

        Args:
            project_entity (ftrack_api.entity.base.Entity): Project entity.
            new_name_hint (Union[None, str]): New name hint. Slugified current
                name to valid value. Or None if name is valid.
            new_code_hint (Union[None, str]): New code hint. Slugified current
                name to valid value. Or None if code is valid.
            new_name (Optional[str]): New name entered by user.
            new_code (Optional[str]): New code entered by user.

        Returns:
            dict[str, Any]: Interface items.
        """

        project_name = project_entity["full_name"]
        project_code = project_entity["name"]
        invalid_keys = []
        if new_name_hint is not None:
            invalid_keys.append("name")
        if new_code_hint is not None:
            invalid_keys.append("code")
        invalid_keys_s = " and ".join(invalid_keys)
        ending = "s" if len(invalid_keys) > 1 else ""
        repeated = new_name is not None or new_code is not None

        intro_message = (
            f"Project {invalid_keys_s} contain{ending} invalid"
            " characters. Only alphanumeric characters and underscore"
            " are allowed (a-Z0-9_)."
            f"<br/><br/>- Project name: {project_name}"
            f"<br/>- Project code: {project_code}"
        )
        if repeated:
            intro_message = (
                "Entered values are <b>not valid</b>.<br/><br/>"
            ) + intro_message

        items = [
            {
                "type": "label",
                "value": "# Introduction",
            },
            {
                "type": "label",
                "value": intro_message,
            },
            {"type": "label", "value": "---"},
            {
                "type": "label",
                "value": "# Choose action",
            },
            {
                "type": "enumerator",
                "label": "Action",
                "name": "invalid_name_action",
                "value": "rename" if repeated else "skip_prep",
                "data": [
                    {
                        "label": "Skip project preparation",
                        "value": "skip_prep",
                    },
                    {
                        "label": "Rename project",
                        "value": "rename",
                    },
                ],
            },
            {"type": "label", "value": "---"},
            {
                "type": "label",
                "value": "# Rename",
            },
            {
                "type": "label",
                "value": (
                    "Ignore if \"<b>Skip project preparation</b>\" is"
                    " selected."
                ),
            }
        ]

        if new_name_hint is not None:
            label = "New project name"
            if new_name_hint == new_name:
                label += " (valid)"
            elif new_name is not None:
                label += " (invalid)"
            items.extend([
                {"type": "label", "value": "---"},
                {"type": "label", "value": label},
                {
                    "type": "text",
                    "name": "new_project_name",
                    "value": new_name_hint,
                }
            ])

        if new_code_hint is not None:
            label = "New project code"
            if new_code_hint == new_code:
                label += " (valid)"
            elif new_code is not None:
                label += " (invalid)"

            items.extend([
                {"type": "label", "value": "---"},
                {"type": "label", "value": label},
                {
                    "type": "text",
                    "name": "new_project_code",
                    "value": new_code_hint,
                }
            ])
        items.append({
            "type": "label",
            "value": (
                "<br/><b>WARNING</b>: Rename action will change the"
                " project values in ftrack."
            ),
        })

        return {
            "title": f"Invalid project {invalid_keys_s}",
            "submit_button_label": "Confirm",
            "items": items,
        }

    def _convert_value_for_attr_conf(
        self, value, attr_conf, attr_type_names_by_id
    ):
        # TODO validate all value types
        if not isinstance(value, list):
            return value

        attr_name = attr_conf["key"]
        attr_type_name = attr_type_names_by_id[attr_conf["type_id"]]
        attr_config = json.loads(attr_conf["config"])
        # Skip if value is not multiselection enumerator
        if (
            attr_type_name != "enumerator"
            or attr_config["multiSelect"] is False
        ):
            self.log.info(
                f"Skipped attribute '{attr_name}' because value"
                f" type (list) does not match"
                f" ftrack attribute type ({attr_type_name})."
            )
            return None

        attr_config_data = attr_config["data"]
        if isinstance(attr_config_data, str):
            attr_config_data = json.loads(attr_config_data)

        available_values = {
            item["value"]
            for item in attr_config_data
        }
        new_value = [
            item
            for item in value
            if item in available_values
        ]
        value_diff = set(value) - set(new_value)
        if value_diff:
            joined_values = ", ".join({f'"{item}"'for item in value_diff})
            self.log.info(
                f"Skipped invalid '{attr_name}' enumerator"
                f" values {joined_values}."
            )
        return new_value

    def _set_ftrack_attributes(self, session, project_entity, values):
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

        attr_type_names_by_id = {
            attr_type["id"]: attr_type["name"]
            for attr_type in session.query(
                "select id, name from CustomAttributeType"
            ).all()
        }
        for attr_name, attr_value in values.items():
            attrs = [
                attrs_by_name.get(attr_name),
                hier_attrs_by_name.get(attr_name)
            ]
            for attr in attrs:
                if attr is None:
                    continue
                attr_value = self._convert_value_for_attr_conf(
                    attr_value, attr, attr_type_names_by_id
                )
                if attr_value is None:
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
        # TODO validate project code too
        if syncer.project_exists_in_ayon():
            return {
                "message": "Project already exists in AYON.",
                "success": True
            }

        attributes = {}
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

        ayon_users_to_clean_roles = self.get_ayon_users_to_clean_roles(
            session, project_entity
        )

        syncer.create_project(anatomy_preset, attributes)

        for ayon_username in ayon_users_to_clean_roles:
            user = ayon_api.get_user(ayon_username)
            user_data = user["data"]
            user_access_groups = user_data.setdefault("accessGroups", {})
            user_access_groups[project_name] = []
            ayon_api.patch(f"users/{ayon_username}", data=user_data)

        ayon_project = ayon_api.get_project(project_entity["full_name"])
        values = copy.deepcopy(ayon_project["attrib"])
        auto_sync_project = event_values["auto_sync_project"]
        values[CUST_ATTR_AUTO_SYNC] = auto_sync_project
        self._set_ftrack_attributes(session, project_entity, values)

        if not auto_sync_project:
            event_data = {
                "actionIdentifier": "sync.from.ftrack.to.ayon",
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
            "message": "Project created in AYON.",
            "success": True
        }

    def get_ayon_users_to_clean_roles(
        self, session, project_entity
    ):
        """Get AYON usernames that should have removed roles from a project.

        If project is private in ftrack we do remove roles from AYON users if
        they should not see it.

        Args:
            session (ftrack_api.Session): ftrack session.
            project_entity (ftrack_api.entity.base.Entity): Project entity.

        """
        if not project_entity["is_private"]:
            return []

        ayon_users = [
            ayon_user
            for ayon_user in ayon_api.get_users()
            if (
                not ayon_user["isAdmin"]
                and not ayon_user["isManager"]
                and not ayon_user["isService"]
            )
        ]
        ayon_usernames = [
            ayon_user["name"]
            for ayon_user in ayon_users
        ]

        project_id = project_entity["id"]
        role_ids = [
            role["user_security_role_id"]
            for role in session.query(
                "select user_security_role_id"
                " from UserSecurityRoleProject"
                f" where project_id is '{project_id}'"
            ).all()
        ]
        if not role_ids:
            return ayon_usernames
        joined_role_ids = self.join_filter_values(role_ids)
        user_ids = {
            role["user_id"]
            for role in session.query(
                "select user_id"
                " from UserSecurityRole"
                f" where security_role_id in ({joined_role_ids})"
            ).all()
        }
        ftrack_users = []
        if user_ids:
            joined_user_ids = self.join_filter_values(user_ids)
            ftrack_users = session.query(
                "select id, username, email from User"
                f" where id in ({joined_user_ids})"
            ).all()
        users_mapping = map_ftrack_users_to_ayon_users(
            ftrack_users,
            ayon_users
        )

        for ftrack_id, ayon_username in users_mapping.items():
            if ayon_username is None:
                continue

            if ftrack_id not in user_ids:
                ayon_usernames.remove(ayon_username)

        return ayon_usernames

