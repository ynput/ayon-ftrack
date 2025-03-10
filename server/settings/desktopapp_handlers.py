from pydantic import validator

from ayon_server.settings import (
    BaseSettingsModel,
    SettingsField,
    ensure_unique_names,
)

from .common import DictWithStrList, ROLES_TITLE


class SimpleLocalAction(BaseSettingsModel):
    enabled: bool = True
    role_list: list[str] = SettingsField(
        title=ROLES_TITLE,
        default_factory=list,
    )


class ApplicationLaunchStatuses(BaseSettingsModel):
    """Application launch statuses

    Change task's status to left side if current task status is in list on right side
    """
    enabled: bool = True
    ignored_statuses: list[str] = SettingsField(
        default_factory=list,
        title="Do not change status if current status is",
    )
    status_change: list[DictWithStrList] = SettingsField(
        title="Status change",
        default_factory=list,
    )

    @validator("status_change")
    def ensure_unique_names(cls, value):
        """Ensure name fields within the lists have unique names."""

        ensure_unique_names(value)
        return value


class CreateUpdateCustomAttributesAction(BaseSettingsModel):
    role_list: list[str] = SettingsField(
        title=ROLES_TITLE,
        default_factory=list,
    )


class PrepareProjectAction(SimpleLocalAction):
    create_project_structure_checked: bool = SettingsField(
        True,
        description="Check \"Create project structure\" by default",
        title="Create project structure",
    )


class FillWorkfileAttr(BaseSettingsModel):
    enabled: bool = True
    custom_attribute_key: str = SettingsField(
        "",
        title="Custom attribute key",
        description=(
            "Custom attribute must be <b>Text</b>"
            " type added to <b>Task</b> entity type"
        ),
    )
    role_list: list[str] = SettingsField(
        title=ROLES_TITLE,
        default_factory=list,
    )


class FtrackDesktopAppHandlers(BaseSettingsModel):
    """Settings for event handlers running in ftrack service."""

    create_update_attributes: CreateUpdateCustomAttributesAction = (
        SettingsField(
            title="Create/Update Custom Attributes",
            default_factory=CreateUpdateCustomAttributesAction,
        )
    )
    prepare_project: PrepareProjectAction = SettingsField(
        title="Prepare Project",
        default_factory=PrepareProjectAction,
    )
    clean_hierarchical_attr: SimpleLocalAction = SettingsField(
        title="Clean hierarchical custom attributes",
        default_factory=SimpleLocalAction
    )
    delete_old_versions: SimpleLocalAction = SettingsField(
        title="Delete old versions",
        default_factory=SimpleLocalAction,
    )
    delivery_action: SimpleLocalAction = SettingsField(
        title="Delivery action",
        default_factory=SimpleLocalAction,
    )
    job_killer: SimpleLocalAction = SettingsField(
        title="Job Killer",
        default_factory=SimpleLocalAction,
    )
    fill_workfile_attribute: FillWorkfileAttr = SettingsField(
        title="Fill workfile Custom attribute",
        default_factory=FillWorkfileAttr,
    )


DEFAULT_DESKTOP_HANDLERS_SETTINGS = {
    "create_update_attributes": {
        "role_list": [
            "Administrator"
        ]
    },
    "prepare_project": {
        "enabled": True,
        "role_list": [
            "Administrator",
            "Project manager"
        ],
        "create_project_structure_checked": False
    },
    "clean_hierarchical_attr": {
        "enabled": True,
        "role_list": [
            "Administrator",
            "Project manager"
        ]
    },
    "delete_ayon_entities": {
        "enabled": True,
        "role_list": [
            "Administrator",
            "Project Manager"
        ]
    },
    "delete_old_versions": {
        "enabled": True,
        "role_list": [
            "Project Manager",
            "Administrator"
        ]
    },
    "delivery_action": {
        "enabled": True,
        "role_list": [
            "Project Manager",
            "Administrator"
        ]
    },
    "job_killer": {
        "enabled": True,
        "role_list": [
            "Administrator"
        ]
    },
    "fill_workfile_attribute": {
        "enabled": False,
        "custom_attribute_key": "",
        "role_list": []
    }
}
