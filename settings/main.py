from pydantic import Field

from openpype.settings import BaseSettingsModel

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
        "",
        title="Ftrack user name",
    )
    api_key: str = Field(
        "",
        title="Ftrack API key"
    )


class FtrackSettings(BaseSettingsModel):
    """Ftrack addon settings."""

    ftrack_server: str = Field(
        "",
        title="Ftrack server url",
    )

    service_event_handlers: FtrackServiceHandlers = Field(
        default_factory=FtrackServiceHandlers,
        title="Server Actions/Events",
    )

    service_settings: FtrackServiceSettings = Field(
        default_factory=FtrackServiceSettings,
        title="Service settings",
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
    "publish": DEFAULT_PUBLISH_SETTINGS
}
