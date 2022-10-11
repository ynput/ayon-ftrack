from pydantic import Field, validator

from openpype.settings import BaseSettingsModel, ensure_unique_names


class StatusMapping(BaseSettingsModel):
    _layout = "expanded"
    name: str
    value: list[str] = Field(default_factory=list)


class FtrackStatusUpdate(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    mapping: list[StatusMapping] = Field(
        default_factory=list, title="Status mapping")

    @validator("mapping")
    def ensure_unique_names(cls, value):
        """Ensure name fields within the lists have unique names."""
        ensure_unique_names(value)
        return value


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
