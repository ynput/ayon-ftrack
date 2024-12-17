"""
This action creates/updates custom attributes.
## First part take care about special attributes
    - AYON attributes defined in code because they use constants

## Second part is based on json file in ftrack module.
File location: `./common/custom_attributes.json`

Data in json file is nested dictionary. Keys in first dictionary level
represents ftrack entity type (task, show, assetversion, user, list, asset)
and dictionary value define attribute.

There is special key for hierchical attributes `is_hierarchical`.

Entity types `task` requires to define task object type (Folder, Shot,
Sequence, Task, Library, Milestone, Episode, Asset Build, etc.) at second
dictionary level, task's attributes are nested more.

*** Not Changeable *********************************************************

group (string)
    - name of group
    - based on attribute `common.constants.CUST_ATTR_GROUP`
        - "pype" by default

*** Required ***************************************************************

label (string)
    - label that will show in ftrack

key (string)
    - must contain only chars [a-z0-9_]

type (string)
    - type of custom attribute
    - possibilities:
        text, boolean, date, enumerator, dynamic enumerator, number

*** Required with conditions ***********************************************

config (dictionary)
    - for each attribute type different requirements and possibilities:
        - enumerator:
            multiSelect = True/False(default: False)
            data = {key_1:value_1,key_2:value_2,..,key_n:value_n}
                - 'data' is Required value with enumerator
                - 'key' must contain only chars [a-z0-9_]

        - number:
            isdecimal = True/False(default: False)

        - text:
            markdown = True/False(default: False)

*** Presetable keys **********************************************************

write_security_roles/read_security_roles (array of strings)
    - default: ["ALL"]
    - strings should be role names (e.g.: ["API", "Administrator"])
    - if set to ["ALL"] - all roles will be availabled
    - if first is 'except' - roles will be set to all except roles in array
        - Warning: Be carefull with except - roles can be different by company
        - example:
            write_security_roles = ["except", "User"]
            read_security_roles = ["ALL"] # (User is can only read)

default
    - default: None
    - sets default value for custom attribute:
        - text -> string
        - number -> integer
        - enumerator -> array with string of key/s
        - boolean -> bool true/false
        - date -> string in format: 'YYYY.MM.DD' or 'YYYY.MM.DD HH:mm:ss'
            - example: "2018.12.24" / "2018.1.1 6:0:0"
        - dynamic enumerator -> DON'T HAVE DEFAULT VALUE!!!

Example:
```
"show": {
    "ayon_auto_sync": {
      "label": "AYON auto-sync",
      "type": "boolean",
      "write_security_roles": ["API", "Administrator"],
      "read_security_roles": ["API", "Administrator"]
    }
},
"is_hierarchical": {
    "fps": {
        "label": "FPS",
        "type": "number",
        "config": {"isdecimal": true}
    }
},
"task": {
    "library": {
        "my_attr_name": {
            "label": "My Attr",
            "type": "number"
        }
    }
}
```
"""
import os
import sys
import json
import traceback
import tempfile
import datetime

import arrow

from ayon_core.settings import get_studio_settings

from ayon_ftrack.common import (
    LocalAction,

    CUST_ATTR_GROUP,
    FPS_KEYS,
    CUST_ATTR_INTENT,

    default_custom_attributes_definition,
    ensure_mandatory_custom_attributes_exists,
)
from ayon_ftrack.lib import get_ftrack_icon_url


class CustAttrException(Exception):
    pass


class CreateUpdateContext:
    def __init__(self, session):
        self._session = session
        self._custom_attribute_types = None
        self._security_roles = None
        self._object_types = None
        self._object_types_by_name = None
        self._ftrack_settings = None
        self._attrs_settings = None

        self._groups = None

        self._generic_error = None
        self._failed_attributes = {}

    @property
    def session(self):
        return self._session

    @property
    def ftrack_settings(self):
        return self._get_ftrack_settings()

    @property
    def attrs_settings(self):
        if self._attrs_settings is not None:
            return self._attrs_settings
        ftrack_settings = self._get_ftrack_settings()
        output = {}
        attr_settings = ftrack_settings["custom_attributes"]
        for entity_type, attr_data in attr_settings.items():
            # Lower entity type
            entity_type = entity_type.lower()
            # Just store if entity type is not "task"
            if entity_type != "task":
                output[entity_type] = attr_data
                continue

            # Prepare empty dictionary for entity type if not set yet
            if entity_type not in output:
                output[entity_type] = {}

            # Store presets per lowered object type
            for obj_type, _preset in attr_data.items():
                output[entity_type][obj_type.lower()] = _preset
        self._attrs_settings = output
        return self._attrs_settings

    def get_custom_attribute_types(self) -> list:
        if self._custom_attribute_types is None:
            self._custom_attribute_types = self._session.query(
                "select id, name from CustomAttributeType"
            ).all()
        return self._custom_attribute_types

    def get_custom_attribute_type(self, type_name: str):
        for attr_type in self.get_custom_attribute_types():
            if attr_type["name"].lower() == type_name:
                return attr_type
        return None

    def get_security_roles(self) -> list:
        if self._security_roles is None:
            self._security_roles = self._session.query(
                "select id, name, type from SecurityRole"
            ).all()
        return self._security_roles

    def get_security_roles_by_names(self, security_roles):
        security_roles_by_name = {
            role["name"].lower(): role
            for role in self.get_security_roles()
        }
        security_roles_lowered = [
            name.lower() for name in security_roles
        ]
        if (
            len(security_roles_lowered) == 0
            or "all" in security_roles_lowered
        ):
            return list(security_roles_by_name.values())

        output = []
        if security_roles_lowered[0] == "except":
            excepts = set(security_roles_lowered[1:])
            for role_name, role in security_roles_by_name.items():
                if role_name not in excepts:
                    output.append(role)

        else:
            for role_name in set(security_roles_lowered):
                if role_name in security_roles_by_name:
                    output.append(security_roles_by_name[role_name])
                    continue
                raise CustAttrException(
                    f"Securit role \"{role_name}\" was not found in ftrack."
                )
        return output

    def get_group(self, group_name):
        if not group_name:
            return None

        if self._groups is None:
            self._groups = {
                group["name"].lower(): group
                for group in self._session.query(
                    f"CustomAttributeGroup where name is \"{group_name}\""
                ).all()
            }

        group_name = group_name.lower()
        if group_name in self._groups:
            return self._groups[group_name]

        groups = self._session.query(
            f"CustomAttributeGroup where name is \"{group_name}\""
        ).all()

        if len(groups) > 1:
            raise CustAttrException(
                "Found more than one group \"{}\"".format(group_name)
            )

        if len(groups) == 1:
            group = next(iter(groups))
            self._groups[group_name] = group
            return group

        self.session.create(
            "CustomAttributeGroup",
            {"name": group_name}
        )
        self.session.commit()
        self._groups[group_name] = self._session.query(
            f"CustomAttributeGroup where name is \"{group_name}\""
        ).first()

        return self._groups[group_name]

    def get_object_type_by_name(self, object_type_name):
        if self._object_types_by_name is None:
            self._object_types_by_name = {
                object_type["name"].lower(): object_type
                for object_type in self._get_object_types()
            }
        object_type_name_low = object_type_name.lower()
        return self._object_types_by_name.get(object_type_name_low)

    def _get_object_types(self):
        if self._object_types is None:
            self._object_types = self._session.query("ObjectType").all()
        return self._object_types

    def _get_ftrack_settings(self):
        if self._ftrack_settings is None:
            self._ftrack_settings = get_studio_settings()["ftrack"]
        return self._ftrack_settings

    def job_failed(self):
        return self._failed_attributes or self._generic_error

    def add_failed_attribute(self, attr_name, message):
        self._failed_attributes[attr_name] = message

    def set_generic_error(self, message, traceback_message):
        self._generic_error = "\n".join([message, traceback_message])

    def get_report_text(self):
        if not self.job_failed():
            return None

        output_messages = []
        if self._generic_error:
            output_messages.append(self._generic_error)

        for attr_name, message in self._failed_attributes.items():
            output_messages.append(f"Attribute \"{attr_name}\": {message}")
        return "\n\n".join(output_messages)


class CustomAttributes(LocalAction):
    identifier = "ayon.create.update.attributes"
    label = "AYON Admin"
    variant = "- Create/Update Custom Attributes"
    description = "Creates required custom attributes in ftrack"
    icon = get_ftrack_icon_url("AYONAdmin.svg")
    settings_key = "create_update_attributes"

    required_keys = ("key", "label", "type")

    presetable_keys = (
        "default",
        "write_security_roles",
        "read_security_roles"
    )
    hierarchical_key = "is_hierarchical"

    type_posibilities = (
        "text",
        "boolean",
        "date",
        "enumerator",
        "dynamic enumerator",
        "number"
    )

    def discover(self, session, entities, event):
        return self.valid_roles(session, entities, event)

    def launch(self, session, entities, event):
        # JOB SETTINGS
        user_id = event["source"]["user"]["id"]
        user = session.query(f"User where id is {user_id}").one()

        job = session.create(
            "Job",
            {
                "user": user,
                "status": "running",
                "data": json.dumps({
                    "description": "Custom Attribute creation."
                })
            }
        )
        session.commit()

        context = CreateUpdateContext(session)

        generic_message = "Custom attributes creation failed."
        try:
            ensure_mandatory_custom_attributes_exists(
                self.session,
                context.ftrack_settings,
                custom_attribute_types=context.get_custom_attribute_types(),
                security_roles=context.get_security_roles(),
            )
            # self.intent_attribute(event)
            self.create_default_custom_attributes(context, event)

        except Exception:
            traceback_message = "".join(
                traceback.format_exception(*sys.exc_info())
            )
            print(traceback_message)
            context.set_generic_error(generic_message, traceback_message)

        finally:
            job_status = "done"
            output = True
            if context.job_failed():
                job_status = "failed"
                output = {
                    "success": False,
                    "message": generic_message
                }
                session.rollback()
                report_text = context.get_report_text()
                self._upload_report(session, job, report_text)


            job["status"] = job_status

            session.commit()

        return output

    def _upload_report(self, session, job, report_text):
        with tempfile.NamedTemporaryFile(
            mode="w", prefix="ayon_ftrack_", suffix=".txt", delete=False
        ) as temp_obj:
            temp_obj.write(report_text)
            temp_filepath = temp_obj.name

        # Upload file with traceback to ftrack server and add it to job
        component_name = "{}_{}".format(
            self.__class__.__name__,
            datetime.datetime.now().strftime("%y-%m-%d-%H%M")
        )
        self.add_file_component_to_job(
            job, session, temp_filepath, component_name
        )
        # Delete temp file
        os.remove(temp_filepath)

    def prepare_attribute_settings(self):
        output = {}
        attr_settings = self.ftrack_settings["custom_attributes"]
        for entity_type, attr_data in attr_settings.items():
            # Lower entity type
            entity_type = entity_type.lower()
            # Just store if entity type is not "task"
            if entity_type != "task":
                output[entity_type] = attr_data
                continue

            # Prepare empty dictionary for entity type if not set yet
            if entity_type not in output:
                output[entity_type] = {}

            # Store presets per lowered object type
            for obj_type, _preset in attr_data.items():
                output[entity_type][obj_type.lower()] = _preset

        return output

    def intent_attribute(self, context, event):
        intent_key_values = context.ftrack_settings["intent"]["items"]

        intent_values = []
        for key, label in intent_key_values.items():
            if not key or not label:
                self.log.info((
                    "Skipping intent row: {{\"{}\": \"{}\"}}"
                    " because of empty key or label."
                ).format(key, label))
                continue

            intent_values.append({key: label})

        if not intent_values:
            return

        intent_custom_attr_data = {
            "label": "Intent",
            "key": CUST_ATTR_INTENT,
            "type": "enumerator",
            "entity_type": "assetversion",
            "group": CUST_ATTR_GROUP,
            "config": {
                "multiselect": False,
                "data": intent_values
            }
        }
        self.process_attr_data(context, intent_custom_attr_data, event)

    def create_default_custom_attributes(self, context, event):
        # Load json with custom attributes configurations
        cust_attr_def = default_custom_attributes_definition()
        attrs_data = []

        # Prepare data of hierarchical attributes
        hierarchical_attrs = cust_attr_def.pop(self.hierarchical_key, {})
        for key, cust_attr_data in hierarchical_attrs.items():
            cust_attr_data["key"] = key
            cust_attr_data["is_hierarchical"] = True
            attrs_data.append(cust_attr_data)

        # Prepare data of entity specific attributes
        for entity_type, cust_attr_datas in cust_attr_def.items():
            if entity_type.lower() != "task":
                for key, cust_attr_data in cust_attr_datas.items():
                    cust_attr_data["key"] = key
                    cust_attr_data["entity_type"] = entity_type
                    attrs_data.append(cust_attr_data)
                continue

            # Task should have nested level for object type
            for object_type, _cust_attr_datas in cust_attr_datas.items():
                for key, cust_attr_data in _cust_attr_datas.items():
                    cust_attr_data["key"] = key
                    cust_attr_data["entity_type"] = entity_type
                    cust_attr_data["object_type"] = object_type
                    attrs_data.append(cust_attr_data)

        # Process prepared data
        for cust_attr_data in attrs_data:
            self.process_attr_data(context, cust_attr_data, event)

    def presets_for_attr_data(self, context, attr_data):
        output = {}

        attr_key = attr_data["key"]
        if attr_data.get("is_hierarchical"):
            entity_key = self.hierarchical_key
        else:
            entity_key = attr_data["entity_type"]

        entity_settings = context.attrs_settings.get(entity_key) or {}
        if entity_key.lower() == "task":
            object_type = attr_data["object_type"]
            entity_settings = entity_settings.get(object_type.lower()) or {}

        key_settings = entity_settings.get(attr_key) or {}
        for key, value in key_settings.items():
            if key in self.presetable_keys and value:
                output[key] = value
        return output

    def process_attr_data(self, context, cust_attr_data, event):
        attr_settings = self.presets_for_attr_data(context, cust_attr_data)
        cust_attr_data.update(attr_settings)

        try:
            data = {}
            # Get key, label, type
            data.update(self.get_required(context, cust_attr_data))
            # Get hierachical/ entity_type/ object_id
            data.update(self.get_entity_type(context, cust_attr_data))
            # Get group, default, security roles
            data.update(self.get_optional(context, cust_attr_data))
            # Process data
            self.process_attribute(data)

        except Exception as exc:
            traceback_message = None
            if not isinstance(exc, CustAttrException):
                traceback_message = "".join(
                    traceback.format_exception(*sys.exc_info())
                )

            cust_attr_name = cust_attr_data.get(
                "label", cust_attr_data["key"]
            )

            if cust_attr_name:
                msg = "Custom attribute error \"{}\" - {}".format(
                    cust_attr_name, str(exc)
                )
            else:
                msg = "Custom attribute error - {}".format(str(exc))
            self.log.warning(msg, exc_info=True)
            self.show_message(event, msg)
            if traceback_message:
                msg = "\n".join([msg, traceback_message])
            context.add_failed_attribute(cust_attr_name, msg)

    def process_attribute(self, data):
        existing_attrs = self.session.query((
            "select is_hierarchical, key, type, entity_type, object_type_id"
            " from CustomAttributeConfiguration"
        )).all()

        matching = []
        is_hierarchical = data.get("is_hierarchical", False)
        for attr in existing_attrs:
            if (
                is_hierarchical != attr["is_hierarchical"]
                or attr["key"] != data["key"]
            ):
                continue

            if attr["type"]["name"] != data["type"]["name"]:
                if data["key"] in FPS_KEYS and attr["type"]["name"] == "text":
                    self.log.info("Kept 'fps' as text custom attribute.")
                    return
                continue

            if is_hierarchical:
                matching.append(attr)

            elif "object_type_id" in data:
                if (
                    attr["entity_type"] == data["entity_type"] and
                    attr["object_type_id"] == data["object_type_id"]
                ):
                    matching.append(attr)
            else:
                if attr["entity_type"] == data["entity_type"]:
                    matching.append(attr)

        if len(matching) == 0:
            self.session.create("CustomAttributeConfiguration", data)
            self.session.commit()
            self.log.debug(
                "Custom attribute \"{}\" created".format(data["label"])
            )

        elif len(matching) == 1:
            attr_update = matching[0]
            for key in data:
                if key not in (
                    "is_hierarchical", "entity_type", "object_type_id"
                ):
                    attr_update[key] = data[key]

            self.session.commit()
            self.log.debug(
                "Custom attribute \"{}\" updated".format(data["label"])
            )

        else:
            raise CustAttrException((
                "Custom attribute is duplicated. Key: \"{}\" Type: \"{}\""
            ).format(data["key"], data["type"]["name"]))

    def get_required(self, context, attr):
        for key in self.required_keys:
            if key not in attr:
                raise CustAttrException(
                    "BUG: Key \"{}\" is required".format(key)
                )

        type_name = attr["type"]
        type_name_l = type_name.lower()
        if type_name_l not in self.type_posibilities:
            raise CustAttrException(
                "Type {} is not valid".format(type_name)
            )

        output = {
            "key": attr["key"],
            "label": attr["label"],
            "type": context.get_custom_attribute_type(type_name_l)
        }

        config = None
        if type_name == "number":
            config = self.get_number_config(attr)
        elif type_name == "text":
            config = self.get_text_config(attr)
        elif type_name == "enumerator":
            config = self.get_enumerator_config(attr)

        # Fake empty config
        if config is None:
            config = json.dumps({})
        output["config"] = config

        return output

    def get_number_config(self, attr):
        config = attr.get("config", {})
        is_decimal = config.get("isdecimal")
        if is_decimal is None:
            is_decimal = False

        config_data = {
            "isdecimal": is_decimal,
        }
        if is_decimal:
            precision = config.get("precision")
            if precision is not None:
                config_data["precision"] = precision

        return json.dumps(config_data)

    def get_text_config(self, attr):
        markdown = attr.get("config", {}).get("markdown")
        if markdown is None:
            markdown = False
        return json.dumps({"markdown": markdown})

    def get_enumerator_config(self, attr):
        if "config" not in attr:
            raise CustAttrException("Missing config with data")
        if "data" not in attr["config"]:
            raise CustAttrException("Missing data in config")

        data = []
        for item in attr["config"]["data"]:
            for key in item:
                data.append({
                    "menu": item[key],
                    "value": key,
                })

        multi_selection = False
        for key, value in attr["config"].items():
            if key.lower() == "multiselect":
                if not isinstance(value, bool):
                    raise CustAttrException("Multiselect must be boolean")
                multi_selection = value
                break

        return json.dumps({
            "multiSelect": multi_selection,
            "data": json.dumps(data)
        })

    def get_default(self, attr):
        attr_type = attr["type"]
        default = attr["default"]
        if default is None:
            return default
        err_msg = "Default value is not"
        if attr_type == "number":
            if isinstance(default, str) and default.isnumeric():
                default = float(default)

            if not isinstance(default, (float, int)):
                raise CustAttrException("{} integer".format(err_msg))
        elif attr_type == "text":
            if not isinstance(default, str):
                raise CustAttrException("{} string".format(err_msg))
        elif attr_type == "boolean":
            if not isinstance(default, bool):
                raise CustAttrException("{} boolean".format(err_msg))
        elif attr_type == "enumerator":
            if not isinstance(default, list):
                raise CustAttrException(
                    "{} array with strings".format(err_msg)
                )
            # TODO check if multiSelect is available
            # and if default is one of data menu
            if not isinstance(default[0], str):
                raise CustAttrException("{} array of strings".format(err_msg))
        elif attr_type == "date":
            date_items = default.split(" ")
            failed = True
            try:
                if len(date_items) == 1:
                    default = arrow.get(default, "YY.M.D")
                    failed = False
                elif len(date_items) == 2:
                    default = arrow.get(default, "YY.M.D H:m:s")
                    failed = False

            except Exception:
                pass

            if failed:
                raise CustAttrException("Date is not in proper format")
        elif attr_type == "dynamic enumerator":
            raise CustAttrException("Dynamic enumerator can't have default")

        return default

    def get_optional(self, context, attr):
        output = {}
        if "group" in attr:
            output["group"] = context.get_group(attr["group"])
        if "default" in attr:
            output["default"] = self.get_default(attr)

        output["read_security_roles"] = context.get_security_roles_by_names(
            attr.get("read_security_roles") or []
        )
        output["write_security_roles"] = context.get_security_roles_by_names(
            attr.get("write_security_roles") or []
        )
        return output

    def get_entity_type(self, context, attr):
        if attr.get("is_hierarchical", False):
            return {
                "is_hierarchical": True,
                "entity_type": attr.get("entity_type") or "show"
            }

        entity_type = attr.get("entity_type")
        if not entity_type:
            raise CustAttrException("Missing entity_type")

        if entity_type.lower() != "task":
            return {"entity_type": entity_type}

        object_type_name = attr.get("object_type")
        if not object_type_name:
            raise CustAttrException("Missing object_type")

        object_type = context.get_object_type_by_name(object_type_name)
        if not object_type:
            raise CustAttrException((
                "Object type with name \"{}\" don't exist"
            ).format(object_type_name))

        return {
            "entity_type": entity_type,
            "object_type_id": object_type["id"]
        }
