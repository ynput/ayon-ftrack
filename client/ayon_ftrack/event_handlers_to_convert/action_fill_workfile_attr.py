import os
import sys
import json
import collections
import tempfile
import datetime

import ftrack_api
from ayon_api import (
    get_project,
    get_folders,
    get_tasks,
)

from ayon_core.settings import get_project_settings
from ayon_core.lib import StringTemplate
from ayon_core.pipeline import Anatomy
from ayon_core.pipeline.template_data import get_template_data
from ayon_core.pipeline.workfile import get_workfile_template_key
from ayon_ftrack.common import LocalAction, create_chunks
from ayon_ftrack.lib import get_ftrack_icon_url

NOT_SYNCHRONIZED_TITLE = "Not synchronized"


class FillWorkfileAttributeAction(LocalAction):
    """Action fill work filename into custom attribute on tasks.

    Prerequirements are that the project is synchronized so it is possible to
    access project anatomy and project/asset documents. Tasks that are not
    synchronized are skipped too.
    """

    identifier = "fill.workfile.attr"
    label = "AYON Admin"
    variant = "- Fill workfile attribute"
    description = "Precalculate and fill workfile name into a custom attribute"
    icon = get_ftrack_icon_url("AYONAdmin.svg")

    settings_key = "fill_workfile_attribute"

    def discover(self, session, entities, event):
        """ Validate selection. """
        is_valid = False
        for ent in event["data"]["selection"]:
            # Ignore entities that are not tasks or projects
            if ent["entityType"].lower() in ["show", "task"]:
                is_valid = True
                break

        if is_valid:
            is_valid = self.valid_roles(session, entities, event)
        return is_valid

    def launch(self, session, entities, event):
        # Separate entities and get project entity
        ft_project_entity = None
        for entity in entities:
            if ft_project_entity is None:
                ft_project_entity = self.get_project_from_entity(entity)
                break

        if not ft_project_entity:
            return {
                "message": (
                    "Couldn't find project entity."
                    " Could be an issue with permissions."
                ),
                "success": False
            }

        # Get project settings and check if custom attribute where workfile
        #   should be set is defined.
        project_name = ft_project_entity["full_name"]
        project_settings = get_project_settings(project_name)
        custom_attribute_key = (
            project_settings
            .get("ftrack", {})
            .get("user_handlers", {})
            .get(self.settings_key, {})
            .get("custom_attribute_key")
        )
        if not custom_attribute_key:
            return {
                "success": False,
                "message": "Custom attribute key is not set in settings"
            }

        # Try to find the custom attribute
        # - get Task type object id
        task_obj_type = session.query(
            "select id from ObjectType where name is \"Task\""
        ).one()
        # - get text custom attribute type
        text_type = session.query(
            "select id from CustomAttributeType where name is \"text\""
        ).one()
        # - find the attribute
        attr_conf = session.query(
            (
                "select id, key from CustomAttributeConfiguration"
                " where object_type_id is \"{}\""
                " and type_id is \"{}\""
                " and key is \"{}\""
            ).format(
                task_obj_type["id"], text_type["id"], custom_attribute_key
            )
        ).first()
        if not attr_conf:
            return {
                "success": False,
                "message": (
                    "Could not find Task (text) Custom attribute \"{}\""
                ).format(custom_attribute_key)
            }

        # Store report information
        report = collections.defaultdict(list)
        user_entity = session.query(
            "User where id is {}".format(event["source"]["user"]["id"])
        ).one()
        job_entity = session.create("Job", {
            "user": user_entity,
            "status": "running",
            "data": json.dumps({
                "description": "(0/3) Fill of workfiles started"
            })
        })
        session.commit()

        try:
            self.in_job_process(
                session,
                entities,
                job_entity,
                ft_project_entity,
                project_settings,
                attr_conf,
                report
            )
        except Exception:
            self.log.error(
                "Fill of workfiles to custom attribute failed", exc_info=True
            )
            session.rollback()

            description = "Fill of workfiles Failed (Download traceback)"
            self.add_traceback_to_job(
                job_entity, session, sys.exc_info(), description
            )
            return {
                "message": (
                    "Fill of workfiles failed."
                    " Check job for more information"
                ),
                "success": False
            }

        job_entity["status"] = "done"
        job_entity["data"] = json.dumps({
            "description": "Fill of workfiles completed."
        })
        session.commit()
        if report:
            temp_obj = tempfile.NamedTemporaryFile(
                mode="w",
                prefix="ayon_ftrack_",
                suffix=".json",
                delete=False
            )
            temp_obj.close()
            temp_filepath = temp_obj.name
            with open(temp_filepath, "w") as temp_file:
                json.dump(report, temp_file)

            component_name = "{}_{}".format(
                "FillWorkfilesReport",
                datetime.datetime.now().strftime("%y-%m-%d-%H%M")
            )
            self.add_file_component_to_job(
                job_entity, session, temp_filepath, component_name
            )
            # Delete temp file
            os.remove(temp_filepath)
            self._show_report(event, report, project_name)
            return {
                "message": (
                    "Fill of workfiles finished with few issues."
                    " Check job for more information"
                ),
                "success": True
            }

        return {
            "success": True,
            "message": "Finished with filling of work filenames"
        }

    def _show_report(self, event, report, project_name):
        items = []
        title = "Fill workfiles report ({}):".format(project_name)

        for subtitle, lines in report.items():
            if items:
                items.append({
                    "type": "label",
                    "value": "---"
                })
            items.append({
                "type": "label",
                "value": "# {}".format(subtitle)
            })
            items.append({
                "type": "label",
                "value": '<p>{}</p>'.format("<br>".join(lines))
            })

        self.show_interface(
            items=items,
            title=title,
            event=event
        )

    def in_job_process(
        self,
        session,
        entities,
        job_entity,
        ft_project_entity,
        project_settings,
        attr_conf,
        report
    ):
        ft_task_entities = []
        other_entities = []
        project_selected = False
        for entity in entities:
            ent_type_low = entity.entity_type.lower()
            if ent_type_low == "project":
                project_selected = True
                break

            elif ent_type_low == "task":
                ft_task_entities.append(entity)
            else:
                other_entities.append(entity)

        project_name = ft_project_entity["full_name"]

        # Find matchin asset documents and map them by ftrack task entities
        # - result stored to 'folder_entities_with_ft_task_entities' is list
        #   with a tuple `(folder entity, [ftrack task entitis, ...])`
        # Fetch all folder and task entities
        folder_entities = list(get_folders(
            project_name, fields={"id", "folderType", "path", "attrib"}
        ))
        task_entities_by_folder_id = collections.defaultdict(list)
        for task_entity in get_tasks(
            project_name, fields={"id", "taskType", "name", "folderId"}
        ):
            folder_id = task_entity["folderId"]
            task_entities_by_folder_id[folder_id].append(task_entity)

        job_entity["data"] = json.dumps({
            "description": "(1/3) Folder & Task entities queried."
        })
        session.commit()

        # When project is selected then we can query whole project
        if project_selected:
            folder_entities_with_ft_task_entities = self._get_asset_docs_for_project(
                session,
                ft_project_entity,
                folder_entities,
                task_entities_by_folder_id,
                report
            )

        else:
            folder_entities_with_ft_task_entities = self._get_tasks_for_selection(
                session,
                other_entities,
                ft_task_entities,
                folder_entities,
                task_entities_by_folder_id,
                report
            )

        job_entity["data"] = json.dumps({
            "description": "(2/3) Queried related task entities."
        })
        session.commit()

        # Keep placeholders in the template unfilled
        host_name = "{app}"
        extension = "{ext}"
        project_entity = get_project(project_name)
        project_settings = get_project_settings(project_name)
        anatomy = Anatomy(project_name)
        templates_by_key = {}

        operations = []
        for folder_entity, ft_task_entities in folder_entities_with_ft_task_entities:
            folder_id = folder_entity["id"]
            folder_path = folder_entity["path"]
            task_entities_by_name = {
                task_entity["name"]: task_entity
                for task_entity in task_entities_by_folder_id[folder_id]
            }
            task_entities_by_low_name = {
                name.lower(): task_entity
                for name, task_entity in task_entities_by_name.items()
            }
            for ft_task_entity in ft_task_entities:
                task_name = ft_task_entity["name"]
                task_entity = task_entities_by_name.get(task_name)
                if not task_entity:
                    task_entity = task_entities_by_low_name.get(
                        task_name.lower()
                    )

                if not task_entity:
                    self.log.warning(
                        f"Coulnd't find task entity \"{task_name}\""
                        f" for folder \"{folder_path}\""
                    )
                    continue

                workfile_data = get_template_data(
                    project_entity,
                    folder_entity,
                    task_entity,
                    host_name,
                    project_settings
                )
                # Use version 1 for each workfile
                workfile_data["version"] = 1
                workfile_data["ext"] = extension

                task_type = task_entity["taskType"]
                template_key = get_workfile_template_key(
                    task_type,
                    host_name,
                    project_name,
                    project_settings=project_settings
                )
                if template_key in templates_by_key:
                    template = templates_by_key[template_key]
                else:
                    template = StringTemplate(
                        anatomy.templates[template_key]["file"]
                    )
                    templates_by_key[template_key] = template

                result = template.format(workfile_data)
                if not result.solved:
                    # TODO report
                    pass
                else:
                    table_values = collections.OrderedDict((
                        ("configuration_id", attr_conf["id"]),
                        ("entity_id", ft_task_entity["id"])
                    ))
                    operations.append(
                        ftrack_api.operation.UpdateEntityOperation(
                            "ContextCustomAttributeValue",
                            table_values,
                            "value",
                            ftrack_api.symbol.NOT_SET,
                            str(result)
                        )
                    )

        if operations:
            for sub_operations in create_chunks(operations, 50):
                for op in sub_operations:
                    session.recorded_operations.push(op)
                session.commit()

        job_entity["data"] = json.dumps({
            "description": "(3/3) Set custom attribute values."
        })
        session.commit()

    def _get_entity_path(self, entity):
        path_items = []
        for item in entity["link"]:
            if item["type"].lower() != "project":
                path_items.append(item["name"])
        return "/".join(path_items)

    def _get_asset_docs_for_project(
        self,
        session,
        ft_project_entity,
        folder_entities,
        task_entities_by_folder_id,
        report,
    ):
        folder_entity_task_names = {}
        for folder_entity in folder_entities:
            ftrack_id = folder_entity["attrib"].get("ftrackId")
            if not ftrack_id:
                path = folder_entity["path"]
                report[NOT_SYNCHRONIZED_TITLE].append(path)
                continue

            folder_id = folder_entity["id"]
            task_names = {
                task_entity["name"]
                for task_entity in task_entities_by_folder_id[folder_id]
            }
            folder_entity_task_names[ftrack_id] = (folder_entity, task_names)

        ft_task_entities = session.query((
            "select id, name, parent_id, link from Task where project_id is {}"
        ).format(ft_project_entity["id"])).all()
        ft_task_entities_by_parent_id = collections.defaultdict(list)
        for ft_task_entity in ft_task_entities:
            parent_id = ft_task_entity["parent_id"]
            ft_task_entities_by_parent_id[parent_id].append(ft_task_entity)

        output = []
        for ftrack_id, item in folder_entity_task_names.items():
            folder_entity, task_names = item
            valid_ft_task_entities = []
            for ft_task_entity in ft_task_entities_by_parent_id[ftrack_id]:
                if ft_task_entity["name"] in task_names:
                    valid_ft_task_entities.append(ft_task_entity)
                else:
                    path = self._get_entity_path(ft_task_entity)
                    report[NOT_SYNCHRONIZED_TITLE].append(path)

            if valid_ft_task_entities:
                output.append((folder_entity, valid_ft_task_entities))

        return output

    def _get_tasks_for_selection(
        self,
        session,
        other_entities,
        ft_task_entities,
        folder_entities,
        task_entities_by_folder_id,
        report,
    ):
        all_tasks = object()
        folder_entities_by_ftrack_id = {}
        for folder_entity in folder_entities:
            ftrack_id = folder_entity["attrib"].get("ftrackId")
            if ftrack_id:
                folder_entities_by_ftrack_id[ftrack_id] = folder_entity

        missing_entity_ftrack_ids = {}
        all_tasks_ids = set()
        task_names_by_ftrack_id = collections.defaultdict(list)
        for other_entity in other_entities:
            ftrack_id = other_entity["id"]
            if ftrack_id not in folder_entities_by_ftrack_id:
                missing_entity_ftrack_ids[ftrack_id] = None
                continue
            all_tasks_ids.add(ftrack_id)
            task_names_by_ftrack_id[ftrack_id] = all_tasks

        for ft_task_entity in ft_task_entities:
            parent_id = ft_task_entity["parent_id"]
            if parent_id not in folder_entities_by_ftrack_id:
                missing_entity_ftrack_ids[parent_id] = None
                continue

            if all_tasks_ids not in all_tasks_ids:
                task_names_by_ftrack_id[ftrack_id].append(ft_task_entity["name"])

        ftrack_ids = set()
        folder_entity_with_task_names_by_id = {}
        for ftrack_id, task_names in task_names_by_ftrack_id.items():
            folder_entity = folder_entities_by_ftrack_id[ftrack_id]
            folder_id = folder_entity["id"]
            folder_task_names = {
                task_entity["name"]
                for task_entity in task_entities_by_folder_id[folder_id]
            }

            if task_names is all_tasks:
                task_names = list(folder_task_names)
            else:
                new_task_names = []
                for task_name in task_names:
                    if task_name in folder_task_names:
                        new_task_names.append(task_name)
                        continue

                    missing_entity_ftrack_ids.setdefault(ftrack_id, [])
                    if missing_entity_ftrack_ids[ftrack_id] is not None:
                        missing_entity_ftrack_ids[ftrack_id].append(task_name)

                task_names = new_task_names

            if task_names:
                ftrack_ids.add(ftrack_id)
                folder_entity_with_task_names_by_id[ftrack_id] = (
                    folder_entity, task_names
                )

        ft_task_entities = session.query((
            "select id, name, parent_id from Task where parent_id in ({})"
        ).format(self.join_query_keys(ftrack_ids))).all()
        task_entitiy_by_parent_id = collections.defaultdict(list)
        for ft_task_entity in ft_task_entities:
            parent_id = ft_task_entity["parent_id"]
            task_entitiy_by_parent_id[parent_id].append(ft_task_entity)

        output = []
        for ftrack_id, item in folder_entity_with_task_names_by_id.items():
            asset_doc, task_names = item
            valid_ft_task_entities = []
            for ft_task_entity in task_entitiy_by_parent_id[ftrack_id]:
                if ft_task_entity["name"] in task_names:
                    valid_ft_task_entities.append(ft_task_entity)
                else:
                    missing_entity_ftrack_ids.setdefault(ftrack_id, [])
                    if missing_entity_ftrack_ids[ftrack_id] is not None:
                        missing_entity_ftrack_ids[ftrack_id].append(task_name)
            if valid_ft_task_entities:
                output.append((asset_doc, valid_ft_task_entities))

        # Store report information about not synchronized entities
        if missing_entity_ftrack_ids:
            missing_entities = session.query(
                "select id, link from TypedContext where id in ({})".format(
                    self.join_query_keys(missing_entity_ftrack_ids.keys())
                )
            ).all()
            for missing_entity in missing_entities:
                path = self._get_entity_path(missing_entity)
                task_names = missing_entity_ftrack_ids[missing_entity["id"]]
                if task_names is None:
                    report[NOT_SYNCHRONIZED_TITLE].append(path)
                else:
                    for task_name in task_names:
                        task_path = "/".join([path, task_name])
                        report[NOT_SYNCHRONIZED_TITLE].append(task_path)

        return output


def register(session):
    FillWorkfileAttributeAction(session).register()
