import collections
import logging

import pyblish.api
import ayon_api

from ayon_ftrack.common import FTRACK_ID_ATTRIB
try:
    from openpype.client import get_asset_name_identifier
except ImportError:
    get_asset_name_identifier = None


class CollectFtrackApi(pyblish.api.ContextPlugin):
    """ Collects an ftrack session and the current task id. """

    order = pyblish.api.CollectorOrder + 0.4991
    label = "Collect Ftrack Api"

    settings_category = "ftrack"

    def process(self, context):
        ftrack_log = logging.getLogger('ftrack_api')
        ftrack_log.setLevel(logging.WARNING)
        ftrack_log = logging.getLogger('ftrack_api_old')
        ftrack_log.setLevel(logging.WARNING)

        # Collect session
        # NOTE Import python module here to know if import was successful
        import ftrack_api

        session = ftrack_api.Session(auto_connect_event_hub=False)
        self.log.debug("Ftrack user: \"{0}\"".format(session.api_user))

        # Collect task
        project_name = context.data["projectName"]
        folder_path = context.data["asset"]
        task_name = context.data["task"]

        # Find project entity
        project_query = 'Project where full_name is "{0}"'.format(project_name)
        self.log.debug("Project query: < {0} >".format(project_query))
        project_entities = list(session.query(project_query).all())
        if len(project_entities) == 0:
            raise AssertionError(
                "Project \"{0}\" not found in Ftrack.".format(project_name)
            )
        # QUESTION Is possible to happen?
        elif len(project_entities) > 1:
            raise AssertionError((
                "Found more than one project with name \"{0}\" in Ftrack."
            ).format(project_name))

        project_entity = project_entities[0]

        self.log.debug("Project found: {0}".format(project_entity))

        context_ftrack_entity = None
        if folder_path:
            # Find asset entity
            entities_by_path = self.find_ftrack_entities(
                session, project_entity, [folder_path]
            )
            context_ftrack_entity = entities_by_path[folder_path]
            if context_ftrack_entity is None:
                raise AssertionError((
                    "Entity with path \"{}\" not found"
                    " in Ftrack project \"{}\"."
                ).format(folder_path, project_name))

        self.log.debug("Asset found: {}".format(context_ftrack_entity))

        task_entity = None
        # Find task entity if task is set
        if not context_ftrack_entity:
            self.log.warning(
                "Asset entity is not set. Skipping query of task entity."
            )
        elif not task_name:
            self.log.warning("Task name is not set.")
        else:
            task_query = (
                'Task where name is "{}" and parent_id is "{}"'
            ).format(task_name, context_ftrack_entity["id"])
            self.log.debug("Task entity query: < {} >".format(task_query))
            task_entity = session.query(task_query).first()
            if not task_entity:
                self.log.warning(
                    "Task entity with name \"{0}\" was not found.".format(
                        task_name
                    )
                )
            else:
                self.log.debug("Task entity found: {0}".format(task_entity))

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
                "Checking entities of instance \"{}\"".format(str(instance))
            )
            instance_folder_path = instance.data.get("asset")
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
                    self.log.debug((
                        "Instance's context is same as in publish context."
                        " Asset: {} | Task: {}"
                    ).format(context_folder_path, context_task_name))
                    instance.data["ftrackEntity"] = context_ftrack_entity
                    instance.data["ftrackTask"] = context_task_entity
                    continue
                folder_path = instance_folder_path
                task_name = instance_task_name

            elif instance_task_name:
                if instance_task_name == context_task_name:
                    self.log.debug((
                        "Instance's context task is same as in publish"
                        " context. Task: {}"
                    ).format(context_task_name))
                    instance.data["ftrackEntity"] = context_ftrack_entity
                    instance.data["ftrackTask"] = context_task_entity
                    continue

                folder_path = context_folder_path
                task_name = instance_task_name

            elif instance_folder_path:
                if instance_folder_path == context_folder_path:
                    self.log.debug((
                        "Instance's context asset is same as in publish"
                        " context. Folder: {}"
                    ).format(context_folder_path))
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
        folder_paths = set(instance_by_folder_and_task.keys())

        entities_by_path = self.find_ftrack_entities(
            session, project_entity, folder_paths
        )

        for folder_path, by_task_data in instance_by_folder_and_task.items():
            entity = entities_by_path[folder_path]
            task_entity_by_name = {}
            if not entity:
                self.log.warning((
                    "Didn't find entity with name \"{}\" in Project \"{}\""
                ).format(folder_path, project_entity["full_name"]))
            else:
                task_entities = session.query((
                    "select id, name from Task where parent_id is \"{}\""
                ).format(entity["id"])).all()
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

                    self.log.debug((
                        "Instance {} has own ftrack entities"
                        " as has different context. TypedContext: {} Task: {}"
                    ).format(str(instance), str(entity), str(task_entity)))

    def find_ftrack_entities(self, session, project_entity, folder_paths):
        output = {path: None for path in folder_paths}
        folder_paths_s = set(output.keys())
        # Folder paths are not yet used as unique identifier if
        #   'get_asset_name_identifier' is 'None' so we can query only by name
        if get_asset_name_identifier is None:
            folder_paths_s.discard(None)
            joined_paths = ",".join([
                '"{}"'.format(p) for p in folder_paths_s
            ])
            entities = session.query(
                (
                    "TypedContext where project_id is \"{}\" and name in ({})"
                ).format(project_entity["id"], joined_paths)
            ).all()
            for entity in entities:
                output[entity["name"]] = entity
            return output

        # We can't use 'assetEntity' and folders must be queried because
        #   we must be assured that 'ownAttrib' is used to avoid collisions
        #   because of hierarchical values.
        folders = ayon_api.get_folders(
            project_entity["full_name"],
            folder_paths=folder_paths_s,
            fields={
                "path",
                "attrib.{}".format(FTRACK_ID_ATTRIB),
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
                '"{}"'.format(ftrack_id)
                for ftrack_id in folder_path_by_ftrack_id
            })
            entities = session.query(
                "TypedContext where id in ({})".format(joined_ftrack_ids)
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

        joined_names = ",".join('"{}"'.format(n) for n in names)

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
                output[entity_id] = entity
        return output

