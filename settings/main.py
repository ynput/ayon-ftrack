from pydantic import Field

from openpype.settings import BaseSettingsModel

from .service_handlers import FtrackServiceHandlers


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
        "https://ftrackapp.com",
        title="Ftrack server url",
    )

    events: FtrackServiceHandlers = Field(
        title="Server service",
    )

    service_settings: FtrackServiceSettings = Field(
        default_factory=FtrackServiceSettings,
        title="Service settings",
    )
