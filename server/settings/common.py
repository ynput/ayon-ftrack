from ayon_server.settings import BaseSettingsModel, SettingsField


ROLES_TITLE = "Roles for action"


class DictWithStrList(BaseSettingsModel):
    """Common model for Dictionary like object with list of strings as value.

    This model requires 'ensure_unique_names' validation.
    """

    _layout = "expanded"
    name: str = SettingsField("")
    value: list[str] = SettingsField(default_factory=list)
