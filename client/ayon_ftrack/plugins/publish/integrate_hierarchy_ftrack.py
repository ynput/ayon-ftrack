import collections
from copy import deepcopy

import pyblish.api
import ayon_api

from ayon_core.lib import filter_profiles
from ayon_core.pipeline import KnownPublishError
from ayon_ftrack.common import (
    CUST_ATTR_KEY_SERVER_ID,
    CUST_ATTR_KEY_SERVER_PATH,
    CUST_ATTR_KEY_SYNC_FAIL,

    get_all_attr_configs,
    get_custom_attributes_mapping,
    query_custom_attribute_values,
)
from ayon_ftrack.pipeline import plugin


class IntegrateHierarchyToFtrack(plugin.FtrackPublishContextPlugin):
    """
    Create entities in ftrack based on collected data from premiere
    Example of entry data:
    {
        "ProjectXS": {
            "entity_type": "project",
            "attributes": {
                "fps": 24,
                ...
            },
            "tasks": [
                "Compositing",
                "Lighting",
                ... *task must exist as task type in project schema*
            ],
            "children": {
                "sq01": {
                    "entity_type": "folder",
                    "folder_type": "Sequence",
                    ...
                }
            }
        }
    }
    """

    order = pyblish.api.IntegratorOrder - 0.04
    label = "Integrate Hierarchy To ftrack"
    families = ["shot"]
    hosts = [
        "hiero",
        "resolve",
        "standalonepublisher",
        "flame",
        "traypublisher"
    ]
    optional = False
    create_task_status_profiles = []

    def process(self, context):
        if "hierarchyContext" not in context.data:
            return

        hierarchy_context = self._get_active_hierarchy(context)
        self.log.debug("__ hierarchy_context: {}".format(hierarchy_context))

        session = context.data["ftrackSession"]
        project_name = context.data["projectName"]
        ft_project = session.query(
            'select id, full_name from Project where full_name is "{}"'.format(
                project_name
            )
        ).first()
        if not ft_project:
            raise KnownPublishError(
                "Project \"{}\" was not found on ftrack.".format(project_name)
            )

        # import ftrack hierarchy
        self.import_to_ftrack(
            session, ft_project, context, project_name, hierarchy_context
        )

    def query_ftrack_entitites(self, session, ft_project):
        project_id = ft_project["id"]
        entities = session.query((
            "select id, name, parent_id"
            " from TypedContext where project_id is \"{}\""
        ).format(project_id)).all()

        entities_by_id = {}
        entities_by_parent_id = collections.defaultdict(list)
        for entity in entities:
            entities_by_id[entity["id"]] = entity
            parent_id = entity["parent_id"]
            entities_by_parent_id[parent_id].append(entity)

        ftrack_hierarchy = []
        ftrack_id_queue = collections.deque()
        ftrack_id_queue.append((project_id, ftrack_hierarchy))
        while ftrack_id_queue:
            item = ftrack_id_queue.popleft()
            ftrack_id, parent_list = item
            if ftrack_id == project_id:
                entity = ft_project
                name = entity["full_name"]
            else:
                entity = entities_by_id[ftrack_id]
                name = entity["name"]

            children = []
            parent_list.append({
                "name": name,
                "low_name": name.lower(),
                "entity": entity,
                "children": children,
            })
            for child in entities_by_parent_id[ftrack_id]:
                ftrack_id_queue.append((child["id"], children))
        return ftrack_hierarchy

    def find_matching_ftrack_entities(
        self, hierarchy_context, ftrack_hierarchy
    ):
        walk_queue = collections.deque()
        for entity_name, entity_data in hierarchy_context.items():
            walk_queue.append(
                (entity_name, entity_data, ftrack_hierarchy)
            )

        matching_ftrack_entities = []
        while walk_queue:
            item = walk_queue.popleft()
            entity_name, entity_data, ft_children = item
            matching_ft_child = None
            for ft_child in ft_children:
                if ft_child["low_name"] == entity_name.lower():
                    matching_ft_child = ft_child
                    break

            if matching_ft_child is None:
                continue

            entity = matching_ft_child["entity"]
            entity_data["ft_entity"] = entity
            matching_ftrack_entities.append(entity)

            hierarchy_children = entity_data.get("children")
            if not hierarchy_children:
                continue

            for child_name, child_data in hierarchy_children.items():
                walk_queue.append(
                    (child_name, child_data, matching_ft_child["children"])
                )
        return matching_ftrack_entities

    def query_custom_attribute_values(self, session, entities, attr_ids):
        entity_ids = {
            entity["id"]
            for entity in entities
        }
        output = {
            entity_id: {}
            for entity_id in entity_ids
        }

        for value_item in query_custom_attribute_values(
            session, attr_ids, entity_ids
        ):
            attr_id = value_item["configuration_id"]
            entity_id = value_item["entity_id"]
            output[entity_id][attr_id] = value_item["value"]

        return output

    def import_to_ftrack(
        self, session, ft_project, context, project_name, hierarchy_context
    ):
        ft_task_types = self.get_all_task_types(ft_project)
        ft_task_statuses = self.get_task_statuses(ft_project)
        project_settings = context.data["project_settings"]
        attr_confs = get_all_attr_configs(session)
        attrs_mapping = get_custom_attributes_mapping(
            session, project_settings["ftrack"], attr_confs
        )

        mapped_confs_by_id = {}
        for attr_conf in attr_confs:
            if attr_conf["key"] in {
                CUST_ATTR_KEY_SERVER_ID,
                CUST_ATTR_KEY_SERVER_PATH,
                CUST_ATTR_KEY_SYNC_FAIL,
            }:
                mapped_confs_by_id[attr_conf["id"]] = attr_conf

        for mapping_item in attrs_mapping.values():
            for mapped_conf in mapping_item.attr_confs:
                mapped_confs_by_id[mapped_conf["id"]] = mapped_conf

        # Query user entity (for comments)
        user = session.query(
            f"User where username is \"{session.api_user}\""
        ).first()
        if not user:
            self.log.warning(
                "Was not able to query current User {}".format(
                    session.api_user
                )
            )

        # Query ftrack hierarchy with parenting
        ftrack_hierarchy = self.query_ftrack_entitites(
            session, ft_project)

        # Fill ftrack entities to hierarchy context
        # - there is no need to query entities again
        matching_entities = self.find_matching_ftrack_entities(
            hierarchy_context, ftrack_hierarchy)
        # Query custom attribute values of each entity
        custom_attr_values_by_id = self.query_custom_attribute_values(
            session, matching_entities, set(mapped_confs_by_id.keys()))

        # Get ftrack api module (as they are different per python version)
        ftrack_api = context.data["ftrackPythonModule"]

        self.log.debug(
            "Available task types in ftrack: %s",
            str(ft_task_types)
        )
        self.log.debug(
            "Available task statuses in ftrack: %s",
            str(ft_task_statuses)
        )

        object_types_by_lower_name = {
            obj_type["name"].lower(): obj_type
            for obj_type in ft_project["project_schema"]["object_types"]
        }

        # Use queue of hierarchy items to process
        import_queue = collections.deque()
        for entity_name, entity_data in hierarchy_context.items():
            import_queue.append(
                (entity_name, entity_data, None, "")
            )

        while import_queue:
            item = import_queue.popleft()
            entity_name, entity_data, parent, parent_path = item

            # Entity name did sometimes contain entity path in OpenPype 3.17.7
            # TODO remove this split when we're sure the version is not used
            entity_name = entity_name.split("/")[-1]

            entity_type = entity_data["entity_type"]
            self.log.debug(entity_data)

            entity = entity_data.get("ft_entity")
            if entity is None and entity_type.lower() == "project":
                raise KnownPublishError(
                    "Collected items are not in right order!"
                )

            # Create entity if not exists
            if entity is None:
                # Sanitize against case sensitive folder types.
                folder_type_low = entity_data["folder_type"].lower()
                object_type = object_types_by_lower_name[folder_type_low]
                entity_type = object_type["name"].replace(" ", "")

                entity = session.create(entity_type, {
                    "name": entity_name,
                    "parent": parent
                })
                entity_data["ft_entity"] = entity

            entity_path = ""
            if entity_type.lower() != "project":
                entity_path = f"{parent_path}/{entity_name}"

            # CUSTOM ATTRIBUTES
            attributes = entity_data.get("attributes", {})
            instances = []
            for instance in context:
                instance_folder_path = instance.data.get("folderPath")

                if (
                    instance_folder_path
                    and instance_folder_path.lower() == entity_path.lower()
                ):
                    instances.append(instance)

            for instance in instances:
                instance.data["ftrackEntity"] = entity

            for key, cust_attr_value in attributes.items():
                if cust_attr_value is None:
                    continue

                mapping_item = attrs_mapping.get(key)
                attr_conf = None
                if mapping_item is not None:
                    attr_conf = mapping_item.get_attr_conf_for_entity(entity)

                if attr_conf is None:
                    self.log.warning(
                        f"Missing ftrack custom attribute with name '{key}'"
                    )
                    continue

                attr_id = attr_conf["id"]
                entity_values = custom_attr_values_by_id.get(entity["id"], {})
                # New value is defined by having id in values
                # - it can be set to 'None' (ftrack allows that using API)
                is_new_value = attr_id not in entity_values
                attr_value = entity_values.get(attr_id)

                # Use ftrack operations method to set hiearchical
                # attribute value.
                # - this is because there may be non hiearchical custom
                #   attributes with different properties
                entity_key = collections.OrderedDict((
                    ("configuration_id", attr_conf["id"]),
                    ("entity_id", entity["id"])
                ))
                op = None
                if is_new_value:
                    op = ftrack_api.operation.CreateEntityOperation(
                        "CustomAttributeValue",
                        entity_key,
                        {"value": cust_attr_value}
                    )

                elif attr_value != cust_attr_value:
                    op = ftrack_api.operation.UpdateEntityOperation(
                        "CustomAttributeValue",
                        entity_key,
                        "value",
                        attr_value,
                        cust_attr_value
                    )

                if op is not None:
                    session.recorded_operations.push(op)

            if session.recorded_operations:
                try:
                    session.commit()
                except Exception as exc:
                    session.rollback()
                    session._configure_locations()
                    raise exc

            # TASKS
            instances_by_task_name = collections.defaultdict(list)
            for instance in instances:
                task_name = instance.data.get("task")
                if task_name:
                    instances_by_task_name[task_name.lower()].append(instance)

            ftrack_status_by_task_id = context.data["ftrackStatusByTaskId"]
            tasks = entity_data.get("tasks", [])
            existing_tasks = []
            tasks_to_create = []
            for child in entity["children"]:
                if child.entity_type.lower() == "task":
                    task_name_low = child["name"].lower()
                    existing_tasks.append(task_name_low)

                    for instance in instances_by_task_name[task_name_low]:
                        instance.data["ftrackTask"] = child

            for task_name in tasks:
                task_type = tasks[task_name]["type"]
                if task_name.lower() in existing_tasks:
                    print("Task {} already exists".format(task_name))
                    continue
                tasks_to_create.append((task_name, task_type))

            for task_name, task_type in tasks_to_create:
                task_entity = self.create_task(
                    session,
                    task_name,
                    task_type,
                    entity,
                    ft_task_types,
                    ft_task_statuses,
                    ftrack_status_by_task_id
                )
                for instance in instances_by_task_name[task_name.lower()]:
                    instance.data["ftrackTask"] = task_entity

            # Incoming links.
            self.create_links(session, project_name, entity_data, entity)
            try:
                session.commit()
            except Exception as exc:
                session.rollback()
                session._configure_locations()
                raise exc

            # Create notes.
            entity_comments = entity_data.get("comments")
            if user and entity_comments:
                for comment in entity_comments:
                    entity.create_note(comment, user)

                try:
                    session.commit()
                except Exception as exc:
                    session.rollback()
                    session._configure_locations()
                    raise exc

            # Import children.
            children = entity_data.get("children")
            if not children:
                continue

            for entity_name, entity_data in children.items():
                import_queue.append(
                    (entity_name, entity_data, entity, entity_path)
                )

    def create_links(self, session, project_name, entity_data, entity):
        # WARNING Don't know how does this work?
        #   The logic looks only for 'AssetBuild' entities. Not sure where
        #   value of 'inputs' on entity data comes from.

        # Clear existing links.
        for link in entity.get("incoming_links", []):
            session.delete(link)
            try:
                session.commit()
            except Exception as exc:
                session.rollback()
                session._configure_locations()
                raise exc

        # Create new links.
        input_folder_ids = {
            folder_id
            for folder_id in entity_data.get("inputs", [])
        }
        folder_entities = {}
        if input_folder_ids:
            folder_entities = {
                folder_entity["id"]: folder_entity
                for folder_entity in ayon_api.get_folders(
                    project_name, folder_ids=input_folder_ids
                )
            }

        for folder_id in input_folder_ids:
            folder_entity = folder_entities.get(folder_id)
            ftrack_id = None
            if folder_entity:
                ftrack_id = folder_entity["attrib"].get("ftrackId")
            if not ftrack_id:
                continue

            assetbuild = session.get("AssetBuild", ftrack_id)
            self.log.debug(
                "Creating link from {0} to {1}".format(
                    assetbuild["name"], entity["name"]
                )
            )
            session.create(
                "TypedContextLink", {"from": assetbuild, "to": entity}
            )

    def get_all_task_types(self, project):
        tasks = {}
        proj_template = project["project_schema"]
        temp_task_types = proj_template["_task_type_schema"]["types"]

        for type in temp_task_types:
            if type["name"] not in tasks:
                tasks[type["name"]] = type

        return tasks

    def get_task_statuses(self, project_entity):
        project_schema = project_entity["project_schema"]
        task_workflow_statuses = project_schema["_task_workflow"]["statuses"]
        return {
            status["id"]: status
            for status in task_workflow_statuses
        }

    def create_task(
        self,
        session,
        name,
        task_type,
        parent,
        ft_task_types,
        ft_task_statuses,
        ftrack_status_by_task_id
    ):
        filter_data = {
            "task_names": name,
            "task_types": task_type
        }
        profile = filter_profiles(
            self.create_task_status_profiles,
            filter_data
        )
        status_id = None
        if profile:
            status_name = profile["status_name"]
            status_name_low = status_name.lower()
            for _status_id, status in ft_task_statuses.items():
                if status["name"].lower() == status_name_low:
                    status_id = _status_id
                    status_name = status["name"]
                    break

            if status_id is None:
                self.log.warning(
                    "Task status \"{}\" was not found".format(status_name)
                )

        task = session.create("Task", {
            "name": name,
            "parent": parent
        })
        # TODO not secured!!! - check if task_type exists
        self.log.debug(task_type)
        task["type"] = ft_task_types[task_type]
        if status_id is not None:
            task["status_id"] = status_id

        try:
            session.commit()
        except Exception as exc:
            session.rollback()
            session._configure_locations()
            raise exc

        if status_id is not None:
            ftrack_status_by_task_id[task["id"]] = None
        return task

    def _get_active_hierarchy(self, context):
        """Filter hierarchy context to active folders only."""

        active_folder_paths = set()
        # filter only the active publishing insatnces
        for instance in context:
            if instance.data.get("publish") is False:
                continue

            folder_path = instance.data.get("asset")
            if folder_path:
                active_folder_paths.add(folder_path)

        # remove duplicity in list
        self.log.debug(
            "Active folders:\n{}".format(
                "\n".join(sorted(active_folder_paths))
            )
        )

        hierarchy_context = deepcopy(context.data["hierarchyContext"])

        hierarchy_queue = collections.deque()
        for name, item in hierarchy_context.items():
            hierarchy_queue.append(
                (name, item, "/" + name, hierarchy_context)
            )

        while hierarchy_queue:
            (name, item, path, parent_item) = hierarchy_queue.popleft()
            children = item.get("children")
            if children:
                for child_name, child_item in children.items():
                    child_path = "/".join([path, child_name])
                    hierarchy_queue.append(
                        (child_name, child_item, child_path, item)
                    )

            elif path not in active_folder_paths:
                parent_item.pop(name, None)

        return hierarchy_context
