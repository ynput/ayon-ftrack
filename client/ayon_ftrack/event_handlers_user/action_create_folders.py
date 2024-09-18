import os
import collections
import copy

import ayon_api

from ayon_core.pipeline import Anatomy
from ayon_ftrack.common import LocalAction
from ayon_ftrack.lib import get_ftrack_icon_url


class CreateFolders(LocalAction):
    identifier = "ayon.create.folders"
    label = "Create Folders"
    icon = get_ftrack_icon_url("CreateFolders.svg")

    def discover(self, session, entities, event):
        for entity_item in event["data"]["selection"]:
            if entity_item.get("entityType").lower() in ("task", "show"):
                return True
        return False

    def interface(self, session, entities, event):
        if event["data"].get("values", {}):
            return

        with_interface = False
        for entity in entities:
            if entity.entity_type.lower() != "task":
                with_interface = True
                break

        if "values" not in event["data"]:
            event["data"]["values"] = {}

        event["data"]["values"]["with_interface"] = with_interface
        if not with_interface:
            return

        title = "Create folders"

        entity_name = entity["name"]
        msg = (
            "<h2>Do you want create folders also"
            " for all children of your selection?</h2>"
        )
        if entity.entity_type.lower() == "project":
            entity_name = entity["full_name"]
            msg = msg.replace(" also", "")
            msg += "<h3>(Project root won't be created if not checked)</h3>"
        items = [
            {
                "type": "label",
                "value": msg.format(entity_name)
            },
            {
                "type": "label",
                "value": "With all chilren entities"
            },
            {
                "name": "children_included",
                "type": "boolean",
                "value": False
            },
            {
                "type": "hidden",
                "name": "with_interface",
                "value": with_interface
            }
        ]

        return {
            "items": items,
            "title": title
        }

    def launch(self, session, entities, event):
        if "values" not in event["data"]:
            return

        with_interface = event["data"]["values"]["with_interface"]
        with_childrens = True
        if with_interface:
            with_childrens = event["data"]["values"]["children_included"]

        filtered_entities = []
        for entity in entities:
            low_context_type = entity["context_type"].lower()
            if low_context_type in ("task", "show"):
                if not with_childrens and low_context_type == "show":
                    continue
                filtered_entities.append(entity)

        if not filtered_entities:
            return {
                "success": True,
                "message": "Nothing was created"
            }

        project_entity = self.get_project_from_entity(filtered_entities[0])

        project_name = project_entity["full_name"]
        ayon_project = ayon_api.get_project(project_name)
        if not ayon_project:
            return {
                "success": False,
                "message": f"Project '{project_name}' was not found in AYON.",
            }

        project_code = project_entity["name"]

        task_entities, other_entities = self.get_all_entities(
            session, entities
        )
        hierarchy = self.get_entities_hierarchy(
            session, task_entities, other_entities
        )
        task_types = session.query("select id, name from Type").all()
        task_type_names_by_id = {
            task_type["id"]: task_type["name"]
            for task_type in task_types
        }

        anatomy = Anatomy(project_name, project_entity=ayon_project)

        work_template = anatomy.get_template_item(
            "work", "default", "directory"
        )
        publish_template = anatomy.get_template_item(
            "publish", "default", "directory"
        )

        project_data = {
            "project": {
                "name": project_name,
                "code": project_code
            }
        }

        collected_paths = []
        for item in hierarchy:
            parent_entity, task_entities = item

            parent_data = copy.deepcopy(project_data)

            parents = parent_entity["link"][1:-1]
            hierarchy_names = [p["name"] for p in parents]
            hierarchy = "/".join(hierarchy_names)

            if hierarchy_names:
                parent_name = hierarchy_names[-1]
            else:
                parent_name = project_name

            parent_data.update({
                "asset": parent_entity["name"],
                "hierarchy": hierarchy,
                "parent": parent_name,
                "folder": {
                    "name": parent_entity["name"]
                }
            })

            if not task_entities:
                # create path for entity
                collected_paths.append(self.compute_template(
                    parent_data, work_template
                ))
                collected_paths.append(self.compute_template(
                    parent_data, publish_template
                ))
                continue

            for task_entity in task_entities:
                task_type_id = task_entity["type_id"]
                task_type_name = task_type_names_by_id[task_type_id]
                task_data = copy.deepcopy(parent_data)
                task_data["task"] = {
                    "name": task_entity["name"],
                    "type": task_type_name
                }

                # Template wok
                collected_paths.append(self.compute_template(
                    task_data, work_template
                ))

                # Template publish
                collected_paths.append(self.compute_template(
                    task_data, publish_template
                ))

        if len(collected_paths) == 0:
            return {
                "success": True,
                "message": "No project folders to create."
            }

        self.log.info("Creating folders:")

        for path in set(collected_paths):
            self.log.info(path)
            if not os.path.exists(path):
                os.makedirs(path)

        return {
            "success": True,
            "message": "Successfully created project folders."
        }

    def get_all_entities(self, session, entities):
        """

        Args:
            session (ftrack_api.session.Session): Ftrack session.
            entities (list[ftrack_api.entity.base.Entity]): List of entities.

        Returns:
            tuple[list, list]: Tuple where first item is list of task entities
                and second item is list of entities that are not task
                entities. All are entities that were passed in and
                their children.
        """

        task_entities = []
        other_entities = []

        query_queue = collections.deque()
        query_queue.append(entities)
        while query_queue:
            entities = query_queue.popleft()
            if not entities:
                continue

            no_task_entities = []
            for entity in entities:
                if entity.entity_type.lower() == "task":
                    task_entities.append(entity)
                else:
                    no_task_entities.append(entity)

            if not no_task_entities:
                continue

            other_entities.extend(no_task_entities)

            no_task_entity_ids = {entity["id"] for entity in no_task_entities}
            next_entities = session.query(
                (
                    "select id, parent_id"
                    " from TypedContext where parent_id in ({})"
                ).format(self.join_query_keys(no_task_entity_ids))
            ).all()
            query_queue.append(next_entities)
        return task_entities, other_entities

    def get_entities_hierarchy(self, session, task_entities, other_entities):
        """

        Args:
            session (ftrack_api.session.Session): Ftrack session.
            task_entities (list[ftrack_api.entity.base.Entity]): List of task
                entities.
            other_entities (list[ftrack_api.entity.base.Entity]): List of
                entities that are not task entities.

        Returns:
            list[tuple[ftrack_api.entity.base.Entity, list]]: List of tuples
                where first item is parent entity and second item is list of
                task entities that are children of parent entity.
        """

        output = []
        task_entity_ids = {entity["id"] for entity in task_entities}
        if not task_entity_ids:
            return output

        full_task_entities = session.query(
            (
                "select id, name, type_id, parent_id"
                " from TypedContext where id in ({})"
            ).format(self.join_query_keys(task_entity_ids))
        ).all()
        task_entities_by_parent_id = collections.defaultdict(list)
        for entity in full_task_entities:
            parent_id = entity["parent_id"]
            task_entities_by_parent_id[parent_id].append(entity)

        if not task_entities_by_parent_id:
            return output

        parent_ids = set(task_entities_by_parent_id.keys())

        other_entities_by_id = {
            entity["id"]: entity
            for entity in other_entities
        }
        parent_ids -= set(other_entities_by_id.keys())

        if parent_ids:
            parent_entities = session.query(
                (
                    "select id, name from TypedContext where id in ({})"
                ).format(self.join_query_keys(parent_ids))
            ).all()
            other_entities_by_id.update({
                entity["id"]: entity
                for entity in parent_entities
            })

        for parent_id, parent_entity in other_entities_by_id.items():
            output.append((
                parent_entity,
                task_entities_by_parent_id[parent_id]
            ))

        return output

    def compute_template(self, data, template):
        filled_template = template.format(data)
        if filled_template.solved:
            return os.path.normpath(filled_template)

        self.log.warning(
            "Template \"{}\" was not fully filled \"{}\"".format(
                filled_template.template, filled_template
            )
        )
        return os.path.normpath(filled_template.split("{")[0])
