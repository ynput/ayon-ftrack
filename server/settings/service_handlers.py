from pydantic import Field, validator

from ayon_server.settings import BaseSettingsModel, ensure_unique_names


from .common import DictWithStrList, ROLES_TITLE


class SimpleAction(BaseSettingsModel):
    enabled: bool = True
    role_list: list[str] = Field(
        title=ROLES_TITLE,
        default_factory=list,
    )


class SyncHierarchicalAttributes(BaseSettingsModel):
    enabled: bool = True
    interest_entity_types: list[str] = Field(
        title="Entity types of interest",
        default_factory=list,
    )
    interest_attributes: list[str] = Field(
        title="Attributes to sync",
        default_factory=list,
    )
    action_enabled: bool = Field(
        True,
        title="Enable Action",
    )
    role_list: list[str] = Field(
        title=ROLES_TITLE,
        default_factory=list,
    )


class CloneReviewAction(BaseSettingsModel):
    enabled: bool = True
    role_list: list[str] = Field(default_factory=list, title=ROLES_TITLE)


class ThumbnailHierarchyUpdates(BaseSettingsModel):
    """Push thumbnail from version, up through multiple hierarchy levels."""

    enabled: bool = True
    levels: int = Field(1, title="Levels", ge=0)


class SyncStatusTaskToParentMapping(BaseSettingsModel):
    new_status: str = Field(title="New parent status")
    task_statuses: list[str] = Field(
        title="Task status",
        default_factory=list,
    )


class SyncStatusTaskToParent(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    parent_object_types: list[str] = Field(
        title="Object types",
        default_factory=list,
    )
    parent_status_match_all_task_statuses: list[DictWithStrList] = Field(
        title="Change parent if all tasks match",
        default_factory=list,
    )
    parent_status_by_task_status: list[SyncStatusTaskToParentMapping] = Field(
        title="Change parent status if a single task matches",
        default_factory=list,
    )


class SyncStatusTaskToVersion(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    mapping: list[DictWithStrList] = Field(
        title="Status mapping",
        default_factory=list,
    )
    asset_types_to_skip: list[str] = Field(
        title="Skip on Asset types (short)",
        default_factory=list,
    )

    @validator("mapping")
    def ensure_unique_names(cls, value):
        """Ensure name fields within the lists have unique names."""

        ensure_unique_names(value)
        return value


class SyncStatusVersionToTask(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    mapping: list[DictWithStrList] = Field(
        title="Status mapping",
        default_factory=list,
    )
    asset_types_to_skip: list[str] = Field(
        title="Skip on Asset types (short)",
        default_factory=list,
    )

    @validator("mapping")
    def ensure_unique_names(cls, value):
        """Ensure name fields within the lists have unique names."""

        ensure_unique_names(value)
        return value


class NextTaskStatusMapping(BaseSettingsModel):
    _layout = "expanded"
    name: str
    value: str


class NextTaskUpdate(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    mapping: list[NextTaskStatusMapping] = Field(
        title="Status Mappings",
        default_factory=list,
    )
    ignored_statuses: list[str] = Field(
        title="Ignored statuses",
        default_factory=list,
    )
    name_sorting: bool = True

    @validator("mapping")
    def ensure_unique_names(cls, value):
        """Ensure name fields within the lists have unique names."""

        ensure_unique_names(value)
        return value


class TransferHierNonHierAttrsAction(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    role_list: list[str] = Field(
        title=ROLES_TITLE,
        default_factory=list,
    )


class CreateDailyReviewSession(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    review_session_template: str = Field(
        "",
        title="ReviewSession name template",
    )
    cycle_enabled: bool = Field(
        False,
        title="Run automatically every day",
        section="Automated execution",
    )
    cycle_hour_start: str = Field(
        "00:00:00",
        title="Create daily review session at",
        description="This may take affect on next day",
        widget="time",
        regex="(?:[01]\d|2[0123]):(?:[012345]\d):(?:[012345]\d)",
    )
    role_list: list[str] = Field(
        section="---",
        title=ROLES_TITLE,
        default_factory=list,
    )


class FtrackServiceHandlers(BaseSettingsModel):
    """Settings for event handlers running in ftrack service."""

    prepare_project: SimpleAction = Field(
        title="Prepare Project",
        default_factory=SimpleAction,
    )
    sync_from_ftrack: SimpleAction = Field(
        title="Sync to AYON",
        default_factory=SimpleAction,
    )
    sync_hier_entity_attributes: SyncHierarchicalAttributes = Field(
        title="Sync Hierarchical and Entity Attributes",
        default_factory=SyncHierarchicalAttributes,
    )
    clone_review_session: CloneReviewAction = Field(
        title="Clone Review Session",
        default_factory=CloneReviewAction,
    )
    delete_ayon_entities: SimpleAction = Field(
        title="Delete Folders/Products",
        default_factory=SimpleAction,
    )
    thumbnail_updates: ThumbnailHierarchyUpdates = Field(
        title="Update Hierarchy thumbnails",
        default_factory=ThumbnailHierarchyUpdates,
    )
    status_task_to_parent: SyncStatusTaskToParent = Field(
        title="Sync status from Task to Parent",
        default_factory=SyncStatusTaskToParent,
    )
    status_task_to_version: SyncStatusTaskToVersion = Field(
        title="Sync status from Task to Version",
        default_factory=SyncStatusTaskToVersion,
    )
    status_version_to_task: SyncStatusVersionToTask = Field(
        title="Sync status from Version to Task",
        default_factory=SyncStatusVersionToTask,
    )
    next_task_update: NextTaskUpdate = Field(
        title="Update status on next task",
        default_factory=NextTaskUpdate,
    )
    transfer_values_of_hierarchical_attributes: TransferHierNonHierAttrsAction = Field(
        title="Action to transfer hierarchical attribute values",
        default_factory=TransferHierNonHierAttrsAction,
    )
    create_daily_review_session: CreateDailyReviewSession = Field(
        title="Create daily review session",
        default_factory=CreateDailyReviewSession,
    )


DEFAULT_SERVICE_HANDLERS_SETTINGS = {
    "prepare_project": {
        "enabled": True,
        "role_list": [
            "Administrator",
            "Project manager"
        ]
    },
    "sync_from_ftrack": {
        "enabled": True,
        "role_list": [
            "Administrator",
            "Project manager"
        ]
    },
    "sync_hier_entity_attributes": {
        "enabled": True,
        "interest_entity_types": [
            "Shot",
            "Asset Build"
        ],
        "interest_attributes": [
            "frameStart",
            "frameEnd"
        ],
        "action_enabled": True,
        "role_list": [
            "Administrator",
            "Project Manager"
        ]
    },
    "clone_review_session": {
        "enabled": True,
        "role_list": [
            "Administrator",
            "Project Manager"
        ]
    },
    "thumbnail_updates": {
        "enabled": True,
        "levels": 1
    },
    "status_task_to_parent": {
        "enabled": True,
        "parent_object_types": [
            "Shot",
            "Asset Build"
        ],
        "parent_status_match_all_task_statuses": [
            {
                "name": "Completed",
                "value": [
                    "Approved",
                    "Omitted"
                ]
            }
        ],
        "parent_status_by_task_status": [
            {
                "new_status": "In Progress",
                "task_statuses": [
                    "in progress",
                    "change requested",
                    "retake",
                    "pending review"
                ]
            }
        ]
    },
    "status_task_to_version": {
        "enabled": True,
        "mapping": [],
        "asset_types_filter": []
    },
    "status_version_to_task": {
        "enabled": True,
        "mapping": [],
        "asset_types_to_skip": []
    },
    "next_task_update": {
        "enabled": True,
        "mapping": [
            {
                "name": "Not Ready",
                "value": "Ready"
            }
        ],
        "ignored_statuses": [
            "Omitted"
        ],
        "name_sorting": False
    },
    "transfer_values_of_hierarchical_attributes": {
        "enabled": True,
        "role_list": [
            "Administrator",
            "Project manager"
        ]
    },
    "create_daily_review_session": {
        "enabled": True,
        "role_list": [
            "Administrator",
            "Project Manager"
        ],
        "cycle_enabled": False,
        "cycle_hour_start": "00:00:00",
        "review_session_template": "{yy}{mm}{dd}"
    },
}
