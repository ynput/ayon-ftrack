"""
This action creates/updates custom attributes.
## First part take care about special attributes
    - AYON attributes defined in code because they use constants
    - `applications` based on applications usages
    - `tools` based on tools usages

## Second part is based on json file in ftrack module.
File location: `~/OpenPype/pype/modules/ftrack/ftrack_custom_attributes.json`

Data in json file is nested dictionary. Keys in first dictionary level
represents Ftrack entity type (task, show, assetversion, user, list, asset)
and dictionary value define attribute.

There is special key for hierchical attributes `is_hierarchical`.

Entity types `task` requires to define task object type (Folder, Shot,
Sequence, Task, Library, Milestone, Episode, Asset Build, etc.) at second
dictionary level, task's attributes are nested more.

*** Not Changeable *********************************************************

group (string)
    - name of group
    - based on attribute `openpype_modules.ftrack.lib.CUST_ATTR_GROUP`
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

import json
import arrow

from ayon_ftrack.common import (
    BaseAction,

    CUST_ATTR_GROUP,
    CUST_ATTR_KEY_SERVER_ID,
    CUST_ATTR_KEY_SERVER_PATH,
    CUST_ATTR_AUTO_SYNC,
    CUST_ATTR_KEY_SYNC_FAIL,
    FPS_KEYS,
    CUST_ATTR_INTENT,
    CUST_ATTR_APPLICATIONS,
    CUST_ATTR_TOOLS,

    default_custom_attributes_definition,
    app_definitions_from_app_manager,
    tool_definitions_from_app_manager,
)
from ayon_ftrack.lib import get_ftrack_icon_url

from openpype.settings import get_system_settings
from openpype.lib import ApplicationManager


class CustAttrException(Exception):
    pass


class CustomAttributes(BaseAction):
    identifier = "create.update.attributes"
    label = "OpenPype Admin"
    variant = "- Create/Update Custom Attributes"
    description = "Creates required custom attributes in ftrack"
    icon = get_ftrack_icon_url("OpenPypeAdmin.svg")
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

        # TODO how to get custom attributes from different addons?
        self.app_manager = ApplicationManager()

        try:
            self.prepare_global_data(session)
            self.create_ayon_attributes(event)
            self.applications_attribute(event)
            self.tools_attribute(event)
            self.intent_attribute(event)
            self.custom_attributes_from_file(event)

            job["status"] = "done"
            session.commit()

        except Exception:
            session.rollback()
            job["status"] = "failed"
            session.commit()
            self.log.error(
                "Creating custom attributes failed ({})", exc_info=True
            )

        return True

    def prepare_global_data(self, session):
        self.types_per_name = {
            attr_type["name"].lower(): attr_type
            for attr_type in session.query("CustomAttributeType").all()
        }

        self.security_roles = {
            role["name"].lower(): role
            for role in session.query("SecurityRole").all()
        }

        object_types = session.query("ObjectType").all()
        self.object_types_per_id = {
            object_type["id"]: object_type for object_type in object_types
        }
        self.object_types_per_name = {
            object_type["name"].lower(): object_type
            for object_type in object_types
        }

        self.groups = {}

        self.ftrack_settings = get_system_settings()["modules"]["ftrack"]
        self.attrs_settings = self.prepare_attribute_settings()

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

    def create_ayon_attributes(self, event):
        # Set security roles for attribute

        for item in [
            {
                "key": CUST_ATTR_KEY_SERVER_ID,
                "label": "AYON ID",
                "type": "text",
                "default": "",
                "group": CUST_ATTR_GROUP,
                "is_hierarchical": True,
                "config": {"markdown": False}
            },
            {
                "key": CUST_ATTR_KEY_SERVER_PATH,
                "label": "AYON path",
                "type": "text",
                "default": "",
                "group": CUST_ATTR_GROUP,
                "is_hierarchical": True,
                "config": {"markdown": False}
            },
            {
                "key": CUST_ATTR_KEY_SYNC_FAIL,
                "label": "AYON sync failed",
                "type": "boolean",
                "default": "",
                "group": CUST_ATTR_GROUP,
                "is_hierarchical": True,
                "config": {"markdown": False}
            },
            {
                "key": CUST_ATTR_AUTO_SYNC,
                "label": "AYON auto-sync",
                "group": CUST_ATTR_GROUP,
                "type": "boolean",
                "entity_type": "show"
            }
        ]:
            self.process_attr_data(item, event)

    def applications_attribute(self, event):
        apps_data = app_definitions_from_app_manager(self.app_manager)

        applications_custom_attr_data = {
            "label": "Applications",
            "key": CUST_ATTR_APPLICATIONS,
            "type": "enumerator",
            "entity_type": "show",
            "group": CUST_ATTR_GROUP,
            "config": {
                "multiselect": True,
                "data": apps_data
            }
        }
        self.process_attr_data(applications_custom_attr_data, event)

    def tools_attribute(self, event):
        tools_data = tool_definitions_from_app_manager(self.app_manager)

        tools_custom_attr_data = {
            "label": "Tools",
            "key": CUST_ATTR_TOOLS,
            "type": "enumerator",
            "is_hierarchical": True,
            "group": CUST_ATTR_GROUP,
            "config": {
                "multiselect": True,
                "data": tools_data
            }
        }
        self.process_attr_data(tools_custom_attr_data, event)

    def intent_attribute(self, event):
        intent_key_values = self.ftrack_settings["intent"]["items"]

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
        self.process_attr_data(intent_custom_attr_data, event)

    def custom_attributes_from_file(self, event):
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
            # Add group
            cust_attr_data["group"] = CUST_ATTR_GROUP
            self.process_attr_data(cust_attr_data, event)

    def presets_for_attr_data(self, attr_data):
        output = {}

        attr_key = attr_data["key"]
        if attr_data.get("is_hierarchical"):
            entity_key = self.hierarchical_key
        else:
            entity_key = attr_data["entity_type"]

        entity_settings = self.attrs_settings.get(entity_key) or {}
        if entity_key.lower() == "task":
            object_type = attr_data["object_type"]
            entity_settings = entity_settings.get(object_type.lower()) or {}

        key_settings = entity_settings.get(attr_key) or {}
        for key, value in key_settings.items():
            if key in self.presetable_keys and value:
                output[key] = value
        return output

    def process_attr_data(self, cust_attr_data, event):
        attr_settings = self.presets_for_attr_data(cust_attr_data)
        cust_attr_data.update(attr_settings)

        try:
            data = {}
            # Get key, label, type
            data.update(self.get_required(cust_attr_data))
            # Get hierachical/ entity_type/ object_id
            data.update(self.get_entity_type(cust_attr_data))
            # Get group, default, security roles
            data.update(self.get_optional(cust_attr_data))
            # Process data
            self.process_attribute(data)

        except CustAttrException as cae:
            cust_attr_name = cust_attr_data.get("label", cust_attr_data["key"])

            if cust_attr_name:
                msg = "Custom attribute error \"{}\" - {}".format(
                    cust_attr_name, str(cae)
                )
            else:
                msg = "Custom attribute error - {}".format(str(cae))
            self.log.warning(msg, exc_info=True)
            self.show_message(event, msg)

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

    def get_required(self, attr):
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
            "type": self.types_per_name[type_name_l]
        }

        config = None
        if type_name == "number":
            config = self.get_number_config(attr)
        elif type_name == "text":
            config = self.get_text_config(attr)
        elif type_name == "enumerator":
            config = self.get_enumerator_config(attr)

        if config is not None:
            output["config"] = config

        return output

    def get_number_config(self, attr):
        is_decimal = attr.get("config", {}).get("isdecimal")
        if is_decimal is None:
            is_decimal = False

        return json.dumps({"isdecimal": is_decimal})

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
            item_data = {}
            for key in item:
                # TODO key check by regex
                item_data["menu"] = item[key]
                item_data["value"] = key
                data.append(item_data)

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

        return config

    def get_group(self, attr):
        if isinstance(attr, dict):
            group_name = attr["group"].lower()
        else:
            group_name = attr
        if group_name in self.groups:
            return self.groups[group_name]

        query = "CustomAttributeGroup where name is \"{}\"".format(group_name)
        groups = self.session.query(query).all()

        if len(groups) > 1:
            raise CustAttrException(
                "Found more than one group \"{}\"".format(group_name)
            )

        if len(groups) == 1:
            group = next(iter(groups))
            self.groups[group_name] = group
            return group

        group = self.session.create(
            "CustomAttributeGroup",
            {"name": group_name}
        )
        self.session.commit()

        return group

    def get_security_roles(self, security_roles):
        security_roles_lowered = tuple(name.lower() for name in security_roles)
        if (
            len(security_roles_lowered) == 0
            or "all" in security_roles_lowered
        ):
            return list(self.security_roles.values())

        output = []
        if security_roles_lowered[0] == "except":
            excepts = security_roles_lowered[1:]
            for role_name, role in self.security_roles.items():
                if role_name not in excepts:
                    output.append(role)

        else:
            for role_name in security_roles_lowered:
                if role_name in self.security_roles:
                    output.append(self.security_roles[role_name])
                else:
                    raise CustAttrException((
                        "Securit role \"{}\" was not found in Ftrack."
                    ).format(role_name))
        return output

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

    def get_optional(self, attr):
        output = {}
        if "group" in attr:
            output["group"] = self.get_group(attr)
        if "default" in attr:
            output["default"] = self.get_default(attr)

        roles_read = []
        roles_write = []
        if "read_security_roles" in attr:
            roles_read = attr["read_security_roles"]
        if "write_security_roles" in attr:
            roles_write = attr["write_security_roles"]

        output["read_security_roles"] = self.get_security_roles(roles_read)
        output["write_security_roles"] = self.get_security_roles(roles_write)
        return output

    def get_entity_type(self, attr):
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

        object_type_name_low = object_type_name.lower()
        object_type = self.object_types_per_name.get(object_type_name_low)
        if not object_type:
            raise CustAttrException((
                "Object type with name \"{}\" don't exist"
            ).format(object_type_name))

        return {
            "entity_type": entity_type,
            "object_type_id": object_type["id"]
        }


def register(session):
    CustomAttributes(session).register()
