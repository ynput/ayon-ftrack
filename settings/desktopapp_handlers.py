from pydantic import Field, validator

from ayon_server.settings import BaseSettingsModel, ensure_unique_names

from .common import DictWithStrList, ROLES_TITLE


class SimpleAction(BaseSettingsModel):
    enabled: bool = True
    role_list: list[str] = Field(
        title=ROLES_TITLE,
        default_factory=list,
    )


class ApplicationLaunchStatuses(BaseSettingsModel):
    """Application launch statuses

    Change task's status to left side if current task status is in list on right side
    """
    enabled: bool = True
    ignored_statuses: list[str] = Field(
        default_factory=list,
        title="Do not change status if current status is",
    )
    status_change: list[DictWithStrList] = Field(
        title="Status change",
        default_factory=list,
    )

    @validator("status_change")
    def ensure_unique_names(cls, value):
        """Ensure name fields within the lists have unique names."""

        ensure_unique_names(value)
        return value


class CreateUpdateCustomAttributesAction(BaseSettingsModel):
    role_list: list[str] = Field(
        title=ROLES_TITLE,
        default_factory=list,
    )


class PrepareProjectAction(SimpleAction):
    create_project_structure_checked: bool = Field(
        True,
        description="Check \"Create project structure\" by default",
        title="Create project structure",
    )


class FillWorkfileAttr(BaseSettingsModel):
    enabled: bool = True
    custom_attribute_key: str = Field(
        "",
        title="Custom attribute key",
        description=(
            "Custom attribute must be <b>Text</b>"
            " type added to <b>Task</b> entity type"
        ),
    )
    role_list: list[str] = Field(
        title=ROLES_TITLE,
        default_factory=list,
    )


class FtrackDesktopAppHandlers(BaseSettingsModel):
    """Settings for event handlers running in ftrack service."""

    application_launch_statuses: ApplicationLaunchStatuses = Field(
        title="Application - Status change on launch",
        default_factory=ApplicationLaunchStatuses,
    )
    create_update_attributes: CreateUpdateCustomAttributesAction = Field(
        title="Create/Update Avalon Attributes",
        default_factory=CreateUpdateCustomAttributesAction,
    )
    prepare_project: PrepareProjectAction = Field(
        title="Prepare Project",
        default_factory=PrepareProjectAction,
    )
    clean_hierarchical_attr: SimpleAction = Field(
        title="Clean hierarchical custom attributes",
        default_factory=SimpleAction
    )
    delete_asset_subset: SimpleAction = Field(
        title="Delete Asset/Subsets",
        default_factory=SimpleAction,
    )
    delete_old_versions: SimpleAction = Field(
        title="Delete old versions",
        default_factory=SimpleAction,
    )
    delivery_action: SimpleAction = Field(
        title="Delivery action",
        default_factory=SimpleAction,
    )
    job_killer: SimpleAction = Field(
        title="Job Killer",
        default_factory=SimpleAction,
    )
    fill_workfile_attribute: FillWorkfileAttr = Field(
        title="Fill workfile Custom attribute",
        default_factory=FillWorkfileAttr,
    )
    # Removed settings
    # - seed_project
    # - sync_to_avalon_local
    # - store_thubmnail_to_avalon


DEFAULT_DESKTOP_HANDLERS_SETTINGS = {
    "application_launch_statuses": {
        "enabled": True,
        "ignored_statuses": [
            "In Progress",
            "Omitted",
            "On hold",
            "Approved"
        ],
        "status_change": [
            {
                "name": "In Progress",
                "value": []
            }
        ]
    },
    "create_update_attributes": {
        "role_list": [
            "Pypeclub",
            "Administrator"
        ]
    },
    "prepare_project": {
        "enabled": True,
        "role_list": [
            "Pypeclub",
            "Administrator",
            "Project manager"
        ],
        "create_project_structure_checked": False
    },
    "clean_hierarchical_attr": {
        "enabled": True,
        "role_list": [
            "Pypeclub",
            "Administrator",
            "Project manager"
        ]
    },
    "delete_asset_subset": {
        "enabled": True,
        "role_list": [
            "Pypeclub",
            "Administrator",
            "Project Manager"
        ]
    },
    "delete_old_versions": {
        "enabled": True,
        "role_list": [
            "Pypeclub",
            "Project Manager",
            "Administrator"
        ]
    },
    "delivery_action": {
        "enabled": True,
        "role_list": [
            "Pypeclub",
            "Project Manager",
            "Administrator"
        ]
    },
    "job_killer": {
        "enabled": True,
        "role_list": [
            "Pypeclub",
            "Administrator"
        ]
    },
    "fill_workfile_attribute": {
        "enabled": False,
        "custom_attribute_key": "",
        "role_list": []
    }
}
