import collections
import logging

import pyblish.api
import ayon_api

from ayon_core.pipeline import PublishError

from ayon_ftrack.common import FTRACK_ID_ATTRIB
from ayon_ftrack.pipeline import plugin


class CollectFtrackApi(plugin.FtrackPublishContextPlugin):
    """ Collects an ftrack session and the current task id. """

    order = pyblish.api.CollectorOrder + 0.4991
    label = "Collect ftrack Api"

    def process(self, context):
        ftrack_log = logging.getLogger("ftrack_api")
        ftrack_log.setLevel(logging.WARNING)

        # Collect session
        # NOTE Import python module here to know if import was successful
        import ftrack_api

        session = ftrack_api.Session(auto_connect_event_hub=False)
        self.log.debug(f"ftrack user: \"{session.api_user}\"")

        # Collect task
        project_name = context.data["projectName"]
        folder_path = context.data["folderPath"]
        task_name = context.data["task"]

        # Find project entity
        project_entity = session.query(
            f'Project where full_name is "{project_name}"'
        ).first()
        if project_entity is None:
            raise PublishError(
                f"Failed to find project \"{project_name}\" in ftrack.",
                "Project not found in ftrack",
                (
                    f"Project \"{project_name}\" was not found in ftrack."
                    " Make sure the project does exist in ftrack and"
                    f" ftrack user \"{session.api_user}\" has access to it."
                )
            )

        self.log.debug(f"Project found: {project_entity}")

        context_ftrack_entity = None
        if folder_path:
            # Find folder entity
            entities_by_path = self.find_ftrack_entities(
                session, project_entity, [folder_path]
            )
            context_ftrack_entity = entities_by_path[folder_path]
            if context_ftrack_entity is None:
                raise PublishError(
                    (
                        f"Entity with path \"{folder_path}\" not found"
                        f" in ftrack project \"{project_name}\"."
                    ),
                    "Entity not found in project",
                    (
                        f"Entity with path \"{folder_path}\" was not found in"
                        f" ftrack project \"{project_name}\". Make sure the"
                        " entity does exist in ftrack and your ftrack user"
                        f" \"{session.api_user}\" has access to it."
                    )
                )

        self.log.debug(f"Folder found: {context_ftrack_entity}")

        task_entity = None
        # Find task entity if task is set
        if context_ftrack_entity is None:
            self.log.warning(
                "Folder entity is not set. Skipping query of task entity."
            )
        elif not task_name:
            self.log.warning("Task name is not set.")
        else:
            entity_id = context_ftrack_entity["id"]
            task_entity = session.query(
                f'Task where name is "{task_name}"'
                f' and parent_id is "{entity_id}"'
            ).first()
            if task_entity:
                self.log.debug(f"Task entity found: {task_entity}")
            else:
                self.log.warning(
                    f"Task entity with name \"{task_name}\" was not found."
                )

        context.data["ftrackSession"] = session
        context.data["ftrackPythonModule"] = ftrack_api
        context.data["ftrackProject"] = project_entity
        context.data["ftrackEntity"] = context_ftrack_entity
        context.data["ftrackTask"] = task_entity

        self.per_instance_process(
            context, context_ftrack_entity, task_entity, folder_path
        )

    def per_instance_process(
        self,
        context,
        context_ftrack_entity,
        context_task_entity,
        context_folder_path
    ):
        context_task_name = None
        if context_ftrack_entity and context_task_entity:
            context_task_name = context_task_entity["name"]

        instance_by_folder_and_task = {}
        filtered_instances = []
        for instance in context:
            if not instance.data.get("publish", True):
                continue
            filtered_instances.append(instance)
            self.log.debug(
                f"Checking entities of instance \"{instance}\""
            )
            instance_folder_path = instance.data.get("folderPath")
            instance_task_name = instance.data.get("task")

            folder_path = None
            task_name = None
            if not instance_folder_path and not instance_task_name:
                self.log.debug("Instance does not have set context keys.")
                instance.data["ftrackEntity"] = context_ftrack_entity
                instance.data["ftrackTask"] = context_task_entity
                continue

            elif instance_folder_path and instance_task_name:
                if (
                    instance_folder_path == context_folder_path
                    and instance_task_name == context_task_name
                ):
                    self.log.debug(
                        "Instance's context is same as in publish context."
                        f" Folder: {context_folder_path}"
                        f" | Task: {context_task_name}"
                    )
                    instance.data["ftrackEntity"] = context_ftrack_entity
                    instance.data["ftrackTask"] = context_task_entity
                    continue
                folder_path = instance_folder_path
                task_name = instance_task_name

            elif instance_task_name:
                if instance_task_name == context_task_name:
                    self.log.debug(
                        "Instance's context task is same as in publish"
                        f" context. Task: {context_task_name}"
                    )
                    instance.data["ftrackEntity"] = context_ftrack_entity
                    instance.data["ftrackTask"] = context_task_entity
                    continue

                folder_path = context_folder_path
                task_name = instance_task_name

            elif instance_folder_path:
                if instance_folder_path == context_folder_path:
                    self.log.debug(
                        "Instance's context folder is same as in publish"
                        f" context. Folder: {context_folder_path}"
                    )
                    instance.data["ftrackEntity"] = context_ftrack_entity
                    instance.data["ftrackTask"] = context_task_entity
                    continue

                # Do not use context's task name
                task_name = instance_task_name
                folder_path = instance_folder_path

            instance_by_task = instance_by_folder_and_task.setdefault(
                folder_path, {})
            task_instances = (instance_by_task.setdefault(task_name, []))
            task_instances.append(instance)

        if not instance_by_folder_and_task:
            return

        session = context.data["ftrackSession"]
        project_entity = context.data["ftrackProject"]
        project_name = project_entity["full_name"]
        folder_paths = set(instance_by_folder_and_task.keys())

        entities_by_path = self.find_ftrack_entities(
            session, project_entity, folder_paths
        )

        for folder_path, by_task_data in instance_by_folder_and_task.items():
            entity = entities_by_path[folder_path]
            task_entity_by_name = {}
            if not entity:
                self.log.warning(
                    f"Didn't find folder with path \"{folder_path}\""
                    f" in Project \"{project_name}\""
                )
            else:
                entity_id = entity["id"]
                task_entities = session.query(
                    "select id, name from Task"
                    f" where parent_id is \"{entity_id}\""
                ).all()
                for task_entity in task_entities:
                    task_name_low = task_entity["name"].lower()
                    task_entity_by_name[task_name_low] = task_entity

            for task_name, instances in by_task_data.items():
                task_entity = None
                if task_name and entity:
                    task_entity = task_entity_by_name.get(task_name.lower())

                for instance in instances:
                    instance.data["ftrackEntity"] = entity
                    instance.data["ftrackTask"] = task_entity

                    self.log.debug(
                        f"Instance {instance} has own ftrack entities"
                        " as has different context."
                        f" TypedContext: {entity} Task: {task_entity}"
                    )

    def find_ftrack_entities(self, session, project_entity, folder_paths):
        output = {path: None for path in folder_paths}
        folder_paths_s = set(output.keys())

        # We can't use 'folderEntity' and folders must be queried because
        #   we must be assured that 'ownAttrib' is used to avoid collisions
        #   because of hierarchical values.
        folders = ayon_api.get_folders(
            project_entity["full_name"],
            folder_paths=folder_paths_s,
            fields={
                "path",
                f"attrib.{FTRACK_ID_ATTRIB}",
            },
            own_attributes=True
        )
        folders_by_path = {
            folder["path"]: folder
            for folder in folders
        }

        folder_path_by_ftrack_id = {}
        missing_folder_paths = set()
        for folder_path in folder_paths:
            folder = folders_by_path.get(folder_path)
            if folder:
                ftrack_id = folder["ownAttrib"].get(FTRACK_ID_ATTRIB)
                if ftrack_id:
                    folder_path_by_ftrack_id[ftrack_id] = folder_path
                    continue
            missing_folder_paths.add(folder_path)

        entities_by_id = {}
        if folder_path_by_ftrack_id:
            joined_ftrack_ids = ",".join({
                f'"{ftrack_id}"'
                for ftrack_id in folder_path_by_ftrack_id
            })
            entities = session.query(
                f"TypedContext where id in ({joined_ftrack_ids})"
            ).all()
            entities_by_id = {
                entity["id"]: entity
                for entity in entities
            }

        for ftrack_id, folder_path in folder_path_by_ftrack_id.items():
            entity = entities_by_id.get(ftrack_id)
            if entity is None:
                missing_folder_paths.add(folder_path)
                continue
            output[folder_path] = entity

        output.update(self._find_missing_folder_paths(
            session, project_entity, missing_folder_paths
        ))
        return output

    def _find_missing_folder_paths(
        self, session, project_entity, folder_paths
    ):
        output = {}
        if not folder_paths:
            return output

        self.log.debug((
            "Finding ftrack entities by folder paths"
            " because of missing ftrack id on AYON entity:\n{}"
        ).format("\n".join(folder_paths)))

        names = set()
        for folder_path in folder_paths:
            names |= set(folder_path.split("/"))
        names.discard("")

        joined_names = ",".join(f'"{n}"' for n in names)

        entities = session.query(
            (
                "select id, name, parent_id from TypedContext"
                " where project_id is \"{}\" and name in ({})"
            ).format(
                project_entity["id"],
                joined_names
            )
        ).all()
        entities_by_id = {entity["id"]: entity for entity in entities}
        entities_by_parent_id = collections.defaultdict(list)
        for entity in entities:
            parent_id = entity["parent_id"]
            entities_by_parent_id[parent_id].append(entity)

        for folder_path in folder_paths:
            names = folder_path.lstrip("/").split("/")
            entity_id = project_entity["id"]
            for name in names:
                child_id = None
                for child in entities_by_parent_id[entity_id]:
                    if child["name"].lower() == name.lower():
                        child_id = child["id"]
                        break
                entity_id = child_id
                if child_id is None:
                    break
            entity = entities_by_id.get(entity_id)
            if entity is not None:
                output[folder_path] = entity
        return output
