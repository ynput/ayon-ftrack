from pydantic import Field

from openpype.settings import BaseSettingsModel


class FtrackSettings(BaseSettingsModel):
    """Test addon settings"""

    server: str = Field(
        "https://ftrackapp.com",
        title="Ftrack server url",
    )

    user: str = Field(
        "",
        title="Ftrack user name",
    )

    key: str = Field(
        "",
        title="Ftrack API key",
    )
