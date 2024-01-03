import pyblish.api
from openpype.pipeline.publish import (
    get_plugin_settings,
    apply_plugin_settings_automatically,
)
from ayon_ftrack.common import is_ftrack_enabled_in_settings


class FtrackPublishContextPlugin(pyblish.api.ContextPlugin):
    settings_category = "ftrack"

    @classmethod
    def is_ftrack_enabled(cls, project_settings):
        return is_ftrack_enabled_in_settings(
            project_settings.get("ftrack") or {}
        )

    @classmethod
    def apply_settings(cls, project_settings):
        if not cls.is_ftrack_enabled(project_settings):
            cls.enabled = False
            return

        plugin_settins = get_plugin_settings(
            cls, project_settings, cls.log, None
        )
        apply_plugin_settings_automatically(cls, plugin_settins, cls.log)


class FtrackPublishInstancePlugin(pyblish.api.InstancePlugin):
    settings_category = "ftrack"

    @classmethod
    def is_ftrack_enabled(cls, project_settings):
        return is_ftrack_enabled_in_settings(
            project_settings.get("ftrack") or {}
        )

    @classmethod
    def apply_settings(cls, project_settings):
        if not cls.is_ftrack_enabled(project_settings):
            cls.enabled = False
            return

        plugin_settins = get_plugin_settings(
            cls, project_settings, cls.log, None
        )
        apply_plugin_settings_automatically(cls, plugin_settins, cls.log)
