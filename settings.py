from pydantic import Field

from openpype.settings import BaseSettingsModel


class StatusMapping(BaseSettingsModel):
    _layout = "expanded"
    name: str
    value: list[str] = Field(default_factory=list)


class FtrackStatusUpdate(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    mapping: list[StatusMapping] = Field(default_factory=list, title="Status mapping")


class FtrackServiceHandlers(BaseSettingsModel):
    status_update: FtrackStatusUpdate = Field(title="Update status on task action")


class FtrackServiceSettings(BaseSettingsModel):

    username: str = Field(
        "",
        title="Ftrack user name",
    )

    api_key: str = Field(
        "",
        title="Ftrack API key",
    )


class FtrackSettings(BaseSettingsModel):
    """Test addon settings."""

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
