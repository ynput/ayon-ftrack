from pydantic import Field, validator

from ayon_server.settings import BaseSettingsModel, ensure_unique_names
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


class FtrackServiceSettings(BaseSettingsModel):
    """Ftrack service cares about handling ftrack event and synchronization.

    To be able do that work it is required to listen and process events as one
    of ftrack users. It is recommended to use special user for that purposes
    so you can see which changes happened from service.
    """

    username: str = Field(
        enum_resolver=secrets_enum,
        title="Ftrack user name"
    )
    api_key: str = Field(
        enum_resolver=secrets_enum,
        title="Ftrack API key"
    )


class PostLaunchHookMapping(BaseSettingsModel):
    name: str = Field("", title="New status")
    value: list[str] = Field(default_factory=list, title="From statuses")


class PostLaunchHookSettings(BaseSettingsModel):
    """Change task status on application launch.

    Changeo of status is based on mapping. Each item in mapping define new
    status which is used based on current status/es. Special value for current
    statuses is '__any__', in that case the new status is always used. And if
    new status name is '__ignore__', the change of status is skipped if current
    status is in current statuses list.
    """

    enabled: bool = True
    mapping: list[PostLaunchHookMapping] = Field(default_factory=list)

    @validator("mapping")
    def ensure_unique_names(cls, value):
        """Ensure name fields within the lists have unique names."""

        ensure_unique_names(value)
        return value


class FtrackSettings(BaseSettingsModel):
    """Ftrack addon settings."""

    ftrack_server: str = Field(
        "",
        title="Ftrack server url",
        scope=["studio"],
    )

    service_event_handlers: FtrackServiceHandlers = Field(
        default_factory=FtrackServiceHandlers,
        title="Server Actions/Events",
    )
    service_settings: FtrackServiceSettings = Field(
        default_factory=FtrackServiceSettings,
        title="Service settings",
        scope=["studio"],
    )
    post_launch_hook: PostLaunchHookSettings = Field(
        default_factory=PostLaunchHookSettings,
        title="Status change on application launch"
    )
    user_handlers: FtrackDesktopAppHandlers = Field(
        default_factory=FtrackDesktopAppHandlers,
        title="User Actions/Events",
    )
    publish: FtrackPublishPlugins = Field(
        default_factory=FtrackPublishPlugins,
        title="Publish plugins"
    )


DEFAULT_VALUES = {
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
    "publish": DEFAULT_PUBLISH_SETTINGS
}
