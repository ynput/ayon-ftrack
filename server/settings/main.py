from pydantic import validator

from ayon_server.settings import (
    BaseSettingsModel,
    ensure_unique_names,
    SettingsField,
)
from ayon_server.settings.enum import secrets_enum

from .service_handlers import (
    FtrackServiceHandlers,
    DEFAULT_SERVICE_HANDLERS_SETTINGS,
)
from .desktopapp_handlers import (
    FtrackDesktopAppHandlers,
    DEFAULT_DESKTOP_HANDLERS_SETTINGS,
)
from .publish_plugins import (
    FtrackPublishPlugins,
    DEFAULT_PUBLISH_SETTINGS,
)
from .custom_attributes import (
    CustomAttributesModel,
    DEFAULT_CUSTOM_ATTRIBUTES_SETTINGS,
)


class FtrackServiceSettings(BaseSettingsModel):
    """ftrack service cares about handling ftrack event and synchronization.

    To be able do that work it is required to listen and process events as one
    of ftrack users. It is recommended to use special user for that purposes
    so you can see which changes happened from service.
    """

    username: str = SettingsField(
        enum_resolver=secrets_enum,
        title="ftrack user name"
    )
    api_key: str = SettingsField(
        enum_resolver=secrets_enum,
        title="ftrack API key"
    )


class PostLaunchHookMapping(BaseSettingsModel):
    name: str = SettingsField("", title="New status")
    value: list[str] = SettingsField(
        default_factory=list,
        title="From statuses",
    )


class PostLaunchHookSettings(BaseSettingsModel):
    """Change task status on application launch.

    Change of status is based on mapping. Each item in mapping defines new
    status which is used based on current status(es). Special value for current
    statuses is `__any__`, in that case the new status is always used. And if
    new status name is `__ignore__`, the change of status is skipped if current
    status is in current statuses list.
    """

    enabled: bool = True
    mapping: list[PostLaunchHookMapping] = SettingsField(default_factory=list)

    @validator("mapping")
    def ensure_unique_names(cls, value):
        """Ensure name fields within the lists have unique names."""

        ensure_unique_names(value)
        return value


class FtrackSettings(BaseSettingsModel):
    """ftrack addon settings."""

    enabled: bool = SettingsField(True)
    ftrack_server: str = SettingsField(
        "",
        title="ftrack server url",
        scope=["studio"],
    )
    service_settings: FtrackServiceSettings = SettingsField(
        default_factory=FtrackServiceSettings,
        title="Service settings",
        scope=["studio"],
    )

    service_event_handlers: FtrackServiceHandlers = SettingsField(
        default_factory=FtrackServiceHandlers,
        title="Server Actions/Events",
    )
    post_launch_hook: PostLaunchHookSettings = SettingsField(
        default_factory=PostLaunchHookSettings,
        title="Status change on application launch"
    )
    user_handlers: FtrackDesktopAppHandlers = SettingsField(
        default_factory=FtrackDesktopAppHandlers,
        title="User Actions/Events",
    )
    publish: FtrackPublishPlugins = SettingsField(
        default_factory=FtrackPublishPlugins,
        title="Publish plugins"
    )
    custom_attributes: CustomAttributesModel = SettingsField(
        title="Custom Attributes",
        default_factory=CustomAttributesModel,
        scope=["studio"],
    )


DEFAULT_VALUES = {
    "enabled": True,
    "ftrack_server": "",
    "service_event_handlers": DEFAULT_SERVICE_HANDLERS_SETTINGS,
    "service_settings": {
        "username": "",
        "api_key": ""
    },
    "user_handlers": DEFAULT_DESKTOP_HANDLERS_SETTINGS,
    "post_launch_hook": {
        "enabled": True,
        "mapping": [
            {
                "name": "In Progress",
                "value": ["__any__"]
            },
            {
                "name": "Ready",
                "value": ["Not Ready"]
            },
            {
                "name": "__ignore__",
                "value": [
                    "in progress",
                    "omitted",
                    "on hold"
                ]
            }
        ]
    },
    "publish": DEFAULT_PUBLISH_SETTINGS,
    "custom_attributes": DEFAULT_CUSTOM_ATTRIBUTES_SETTINGS,
}
