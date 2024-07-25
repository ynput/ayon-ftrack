import copy

import pyblish.api
from ayon_core.lib import filter_profiles

from ayon_ftrack.common import create_chunks
from ayon_ftrack.pipeline import plugin


class CollectFtrackTaskStatuses(plugin.FtrackPublishContextPlugin):
    """Collect available task statuses on the project.

    This is preparation for integration of task statuses.

    Requirements:
        ftrackSession (ftrack_api.Session): Prepared ftrack session.

    Provides:
        ftrackTaskStatuses (dict[str, list[Any]]): Dictionary of available
            task statuses on project by task type id.
        ftrackStatusByTaskId (dict[str, str]): Empty dictionary of task
            statuses by task id. Status on task can be set only once.
            Value should be a name of status.
    """

    # After 'CollectFtrackApi'
    order = pyblish.api.CollectorOrder + 0.4992
    label = "Collect Ftrack Task Statuses"

    def process(self, context):
        ftrack_session = context.data("ftrackSession")
        if ftrack_session is None:
            self.log.info("Ftrack session is not created.")
            return

        # Prepare available task statuses on the project
        project_name = context.data["projectName"]
        project_entity = ftrack_session.query((
            "select project_schema from Project where full_name is \"{}\""
        ).format(project_name)).one()
        project_schema = project_entity["project_schema"]

        task_type_ids = {
            task_type["id"]
            for task_type in ftrack_session.query("select id from Type").all()
        }
        task_statuses_by_type_id = {
            task_type_id: project_schema.get_statuses("Task", task_type_id)
            for task_type_id in task_type_ids
        }
        context.data["ftrackTaskStatuses"] = task_statuses_by_type_id
        context.data["ftrackStatusByTaskId"] = {}
        self.log.debug("Collected ftrack task statuses.")


class IntegrateFtrackStatusBase(plugin.FtrackPublishInstancePlugin):
    """Base plugin for status collection.

    Requirements:
        projectName (str): Name of the project.
        hostName (str): Name of the host.
        ftrackSession (ftrack_api.Session): Prepared ftrack session.
        ftrackTaskStatuses (dict[str, list[Any]]): Dictionary of available
            task statuses on project by task type id.
        ftrackStatusByTaskId (dict[str, str]): Empty dictionary of task
            statuses by task id. Status on task can be set only once.
            Value should be a name of status.
    """

    active = False
    settings_key = None
    status_profiles = []

    @classmethod
    def apply_settings(cls, project_settings):
        if not cls.is_ftrack_enabled(project_settings):
            cls.enabled = False
            return

        settings_key = cls.settings_key
        if settings_key is None:
            settings_key = cls.__name__

        try:
            settings = project_settings["ftrack"]["publish"][settings_key]
        except KeyError:
            return

        for key, value in settings.items():
            setattr(cls, key, value)

    def process(self, instance):
        context = instance.context
        # No profiles -> skip
        profiles = self.get_status_profiles()
        if not profiles:
            project_name = context.data["projectName"]
            self.log.debug((
                "Status profiles are not filled for project \"{}\". Skipping"
            ).format(project_name))
            return

        # Task statuses were not collected -> skip
        task_statuses_by_type_id = context.data.get("ftrackTaskStatuses")
        if not task_statuses_by_type_id:
            self.log.debug(
                "Ftrack task statuses are not collected. Skipping.")
            return

        self.prepare_status_names(context, instance, profiles)

    def get_status_profiles(self):
        """List of profiles to determine status name.

        Example profile item:
            {
                "host_names": ["nuke"],
                "task_types": ["Compositing"],
                "task_names": ["Comp"],
                "product_types": ["render"],
                "product_names": ["renderComp"],
                "status_name": "Rendering",
            }

        Returns:
            list[dict[str, Any]]: List of profiles.
        """

        return self.status_profiles

    def prepare_status_names(self, context, instance, profiles):
        if not self.is_valid_instance(context, instance):
            return

        filter_data = self.get_profile_filter_data(context, instance)
        status_profile = filter_profiles(
            profiles,
            filter_data,
            logger=self.log
        )
        if not status_profile:
            return

        status_name = status_profile["status_name"]
        if status_name:
            self.fill_status(context, instance, status_name)

    def get_profile_filter_data(self, context, instance):
        task_entity = instance.data["ftrackTask"]
        return {
            "host_names": context.data["hostName"],
            "task_types": task_entity["type"]["name"],
            "task_names": task_entity["name"],
            "product_types": instance.data["productType"],
            "product_names": instance.data["productName"],
        }

    def is_valid_instance(self, context, instance):
        """Filter instances that should be processed.

        Ignore instances that are not enabled for publishing or don't have
        filled task. Also skip instances with tasks that already have defined
        status.

        Plugin should do more filtering which is custom for plugin logic.

        Args:
            context (pyblish.api.Context): Pyblish context.
            instance (pyblish.api.Instance): Instance to process.

        Returns:
            list[pyblish.api.Instance]: List of instances that should be
                processed.
        """

        ftrack_status_by_task_id = context.data["ftrackStatusByTaskId"]
        # Skip disabled instances
        if instance.data.get("publish") is False:
            return False

        task_entity = instance.data.get("ftrackTask")
        if not task_entity:
            self.log.debug(
                "Skipping instance {}. Does not have filled task".format(
                    instance.data["productName"]))
            return False

        task_id = task_entity["id"]
        if task_id in ftrack_status_by_task_id:
            self.log.debug("Status for task {} was already defined".format(
                task_entity["name"]
            ))
            return False

        return True

    def fill_status(self, context, instance, status_name):
        """Fill status for instance task.

        If task already had set status, it will be skipped.

        Args:
            context (pyblish.api.Context): Pyblish context.
            instance (pyblish.api.Instance): Pyblish instance.
            status_name (str): Name of status to set.
        """

        task_entity = instance.data["ftrackTask"]
        task_id = task_entity["id"]
        ftrack_status_by_task_id = context.data["ftrackStatusByTaskId"]
        if task_id in ftrack_status_by_task_id:
            self.log.debug("Status for task {} was already defined".format(
                task_entity["name"]
            ))
            return

        ftrack_status_by_task_id[task_id] = status_name
        self.log.info((
            "Task {} will be set to \"{}\" status."
        ).format(task_entity["name"], status_name))


class IntegrateFtrackFarmStatus(IntegrateFtrackStatusBase):
    """Collect task status names for instances that are sent to farm.

    Instance which has set "farm" key in data to 'True' is considered as will
    be rendered on farm thus it's status should be changed.

    Requirements:
        projectName (str): Name of the project.
        hostName (str): Name of the host.
        ftrackSession (ftrack_api.Session): Prepared ftrack session.
        ftrackTaskStatuses (dict[str, list[Any]]): Dictionary of available
            task statuses on project by task type id.
        ftrackStatusByTaskId (dict[str, str]): Empty dictionary of task
            statuses by task id. Status on task can be set only once.
            Value should be a name of status.
    """

    order = pyblish.api.IntegratorOrder + 0.48
    label = "Ftrack Task Status To Farm Status"
    active = True

    farm_status_profiles = []
    status_profiles = None

    def is_valid_instance(self, context, instance):
        if not instance.data.get("farm"):
            self.log.debug("{} Won't be rendered on farm.".format(
                instance.data["productName"]
            ))
            return False
        return super(IntegrateFtrackFarmStatus, self).is_valid_instance(
            context, instance)

    def get_status_profiles(self):
        if self.status_profiles is None:
            profiles = copy.deepcopy(self.farm_status_profiles)
            self.status_profiles = profiles
        return self.status_profiles


class IntegrateFtrackLocalStatus(IntegrateFtrackStatusBase):
    """Collect task status names for instances that are published locally.

    Instance which has set "farm" key in data to 'True' is considered as will
    be rendered on farm thus it's status should be changed.

    Requirements:
        projectName (str): Name of the project.
        hostName (str): Name of the host.
        ftrackSession (ftrack_api.Session): Prepared ftrack session.
        ftrackTaskStatuses (dict[str, list[Any]]): Dictionary of available
            task statuses on project by task type id.
        ftrackStatusByTaskId (dict[str, str]): Empty dictionary of task
            statuses by task id. Status on task can be set only once.
            Value should be a name of status.
    """

    order = IntegrateFtrackFarmStatus.order + 0.001
    label = "Ftrack Task Status Local Publish"
    active = True
    targets = ["local"]
    settings_key = "ftrack_task_status_local_publish"

    def is_valid_instance(self, context, instance):
        if instance.data.get("farm"):
            self.log.debug("{} Will be rendered on farm.".format(
                instance.data["productName"]
            ))
            return False
        return super(IntegrateFtrackLocalStatus, self).is_valid_instance(
            context, instance)


class IntegrateFtrackOnFarmStatus(IntegrateFtrackStatusBase):
    """Collect task status names for instances that are published on farm.

    Requirements:
        projectName (str): Name of the project.
        hostName (str): Name of the host.
        ftrackSession (ftrack_api.Session): Prepared ftrack session.
        ftrackTaskStatuses (dict[str, list[Any]]): Dictionary of available
            task statuses on project by task type id.
        ftrackStatusByTaskId (dict[str, str]): Empty dictionary of task
            statuses by task id. Status on task can be set only once.
            Value should be a name of status.
    """

    order = IntegrateFtrackLocalStatus.order + 0.001
    label = "Ftrack Task Status On Farm Status"
    active = True
    targets = ["farm"]
    settings_key = "ftrack_task_status_on_farm_publish"


class IntegrateFtrackTaskStatus(plugin.FtrackPublishContextPlugin):
    # Use order of Integrate Ftrack Api plugin and offset it before or after
    base_order = pyblish.api.IntegratorOrder + 0.499
    # By default is after Integrate Ftrack Api
    order = base_order + 0.0001
    label = "Integrate Ftrack Task Status"

    @classmethod
    def apply_settings(cls, project_settings):
        """Apply project settings to plugin.

        Args:
            project_settings (dict[str, Any]): Project settings.
        """

        if not cls.is_ftrack_enabled(project_settings):
            cls.enabled = False
            return

        settings = (
            project_settings["ftrack"]["publish"]["IntegrateFtrackTaskStatus"]
        )
        diff = 0.001
        if not settings["after_version_statuses"]:
            diff = -diff
        cls.order = cls.base_order + diff

    def process(self, context):
        task_statuses_by_type_id = context.data.get("ftrackTaskStatuses")
        if not task_statuses_by_type_id:
            self.log.debug("Ftrack task statuses are not collected. Skipping.")
            return

        status_by_task_id = self._get_status_by_task_id(context)
        if not status_by_task_id:
            self.log.debug("No statuses to set. Skipping.")
            return

        ftrack_session = context.data["ftrackSession"]

        task_entities = self._get_task_entities(
            ftrack_session, status_by_task_id)

        for task_entity in task_entities:
            task_path = "/".join([
                item["name"] for item in task_entity["link"]
            ])
            task_id = task_entity["id"]
            type_id = task_entity["type_id"]
            new_status = None
            status_name = status_by_task_id[task_id]
            self.log.debug(
                "Status to set {} on task {}.".format(status_name, task_path))
            status_name_low = status_name.lower()
            available_statuses = task_statuses_by_type_id[type_id]
            for status in available_statuses:
                if status["name"].lower() == status_name_low:
                    new_status = status
                    break

            if new_status is None:
                joined_statuses = ", ".join([
                    "'{}'".format(status["name"])
                    for status in available_statuses
                ])
                self.log.debug((
                    "Status '{}' was not found in available statuses: {}."
                ).format(status_name, joined_statuses))
                continue

            if task_entity["status_id"] != new_status["id"]:
                task_entity["status_id"] = new_status["id"]

                self.log.debug("Changing status of task '{}' to '{}'".format(
                    task_path, status_name
                ))
                ftrack_session.commit()

    def _get_status_by_task_id(self, context):
        status_by_task_id = context.data["ftrackStatusByTaskId"]
        return {
            task_id: status_name
            for task_id, status_name in status_by_task_id.items()
            if status_name
        }

    def _get_task_entities(self, ftrack_session, status_by_task_id):
        task_entities = []
        for chunk_ids in create_chunks(status_by_task_id.keys()):
            joined_ids = ",".join(
                ['"{}"'.format(task_id) for task_id in chunk_ids]
            )
            task_entities.extend(ftrack_session.query((
                "select id, type_id, status_id, link from Task"
                " where id in ({})"
            ).format(joined_ids)).all())
        return task_entities
