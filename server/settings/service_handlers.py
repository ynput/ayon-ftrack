from pydantic import validator

from ayon_server.settings import (
    BaseSettingsModel,
    SettingsField,
    ensure_unique_names,
)


from .common import DictWithStrList, ROLES_TITLE


class SimpleAction(BaseSettingsModel):
    enabled: bool = True
    role_list: list[str] = SettingsField(
        title=ROLES_TITLE,
        default_factory=list,
    )


class SyncHierarchicalAttributes(BaseSettingsModel):
    enabled: bool = True
    interest_entity_types: list[str] = SettingsField(
        title="Entity types of interest",
        default_factory=list,
    )
    interest_attributes: list[str] = SettingsField(
        title="Attributes to sync",
        default_factory=list,
    )
    action_enabled: bool = SettingsField(
        True,
        title="Enable Action",
    )
    role_list: list[str] = SettingsField(
        title=ROLES_TITLE,
        default_factory=list,
    )


class CloneReviewAction(BaseSettingsModel):
    enabled: bool = True
    role_list: list[str] = SettingsField(
        default_factory=list, title=ROLES_TITLE
    )


class ThumbnailHierarchyUpdates(BaseSettingsModel):
    """Push thumbnail from version, up through multiple hierarchy levels."""

    enabled: bool = True
    levels: int = SettingsField(1, title="Levels", ge=0)


class SyncStatusTaskToParentMapping(BaseSettingsModel):
    new_status: str = SettingsField(title="New parent status")
    task_statuses: list[str] = SettingsField(
        title="Task status",
        default_factory=list,
    )


class SyncStatusTaskToParent(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    parent_object_types: list[str] = SettingsField(
        title="Object types",
        default_factory=list,
    )
    parent_status_match_all_task_statuses: list[DictWithStrList] = (
        SettingsField(
            title="Change parent if all tasks match",
            default_factory=list,
        )
    )
    parent_status_by_task_status: list[SyncStatusTaskToParentMapping] = (
        SettingsField(
            title="Change parent status if a single task matches",
            default_factory=list,
        )
    )


def _allow_deny_enum():
    return [
        {"value": "allow_list", "label": "Allow list"},
        {"value": "deny_list", "label": "Deny list"}
    ]


class SyncStatusTaskToVersion(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    mapping: list[DictWithStrList] = SettingsField(
        title="Status mapping",
        default_factory=list,
    )
    asset_types_filter_type: str = SettingsField(
        title="Asset types Allow/Deny",
        default="allow_list",
        enum_resolver=_allow_deny_enum,
    )
    asset_types: list[str] = SettingsField(
        title="Asset types (short)",
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
    mapping: list[DictWithStrList] = SettingsField(
        title="Status mapping",
        default_factory=list,
    )
    asset_types_filter_type: str = SettingsField(
        title="Asset types Allow/Deny",
        default="deny_list",
        enum_resolver=_allow_deny_enum,
    )
    asset_types: list[str] = SettingsField(
        title="Asset types (short)",
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
    mapping: list[NextTaskStatusMapping] = SettingsField(
        title="Status Mappings",
        default_factory=list,
    )
    ignored_statuses: list[str] = SettingsField(
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
    role_list: list[str] = SettingsField(
        title=ROLES_TITLE,
        default_factory=list,
    )


# class CreateDailyReviewSession(BaseSettingsModel):
#     _isGroup = True
#     enabled: bool = True
#     review_session_template: str = SettingsField(
#         "",
#         title="ReviewSession name template",
#     )
#     cycle_enabled: bool = SettingsField(
#         False,
#         title="Run automatically every day",
#         section="Automated execution",
#     )
#     cycle_hour_start: str = SettingsField(
#         "00:00:00",
#         title="Create daily review session at",
#         description="This may take affect on next day",
#         widget="time",
#         regex="(?:[01]\d|2[0123]):(?:[012345]\d):(?:[012345]\d)",
#     )
#     role_list: list[str] = SettingsField(
#         section="---",
#         title=ROLES_TITLE,
#         default_factory=list,
#     )


def custom_attribute_type():
    return [
        {"value": "bool_value", "label": "Boolean"},
        {"value": "str_value", "label": "String"},
        {"value": "int_value", "label": "Integer"},
        {"value": "float_value", "label": "Float"},
        {"value": "enum_value", "label": "Enumerator"},
    ]


class DailyListCustomAttributesModel(BaseSettingsModel):
    _layout = "expanded"
    attr_name: str = SettingsField("", title="Attribute name")
    attr_type: str = SettingsField(
        "bool_value",
        title="Attribute type",
        enum_resolver=custom_attribute_type,
        conditionalEnum=True,
    )
    bool_value: bool = SettingsField(True, title="Expected value")
    str_value: str = SettingsField("", title="Expected value")
    int_value: int = SettingsField(0, title="Expected value")
    float_value: float = SettingsField(0.0, title="Expected value")
    enum_value: list[str] = SettingsField(
        title="Expected value",
        default_factory=list,
    )


class DailyListFilterModel(BaseSettingsModel):
    _layout = "expanded"
    statuses: list[str] = SettingsField(
        title="Statuses",
        default_factory=list,
    )
    custom_attributes: list[DailyListCustomAttributesModel] = SettingsField(
        title="Custom attributes",
        default_factory=list,
    )


class DailyListItemModel(BaseSettingsModel):
    """Create list with AssetVersions by filter criteria."""

    _layout = "expanded"
    name_template: str = SettingsField("{yy}{mm}{dd}", title="Name template")
    category: str = SettingsField(
        "Dailies",
        title="List category",
        enum_resolver=lambda: ["Default", "Clients", "Dailies"],
    )
    cycle_enabled: bool = SettingsField(
        False,
        title="Run automatically",
    )
    filters: list[DailyListFilterModel] = SettingsField(
        title="Asset version filters",
        default_factory=list,
    )


def week_days():
    return [
        {"label": "Monday", "value": "monday"},
        {"label": "Tuesday", "value": "tuesday"},
        {"label": "Wednesday", "value": "wednesday"},
        {"label": "Thursday", "value": "thursday"},
        {"label": "Friday", "value": "friday"},
        {"label": "Saturday", "value": "saturday"},
        {"label": "Sunday", "value": "sunday"},
    ]


def default_week_days():
    return [
        "monday", "tuesday", "wednesday", "thursday", "friday"
    ]


class CreateDailyListsModel(BaseSettingsModel):
    """Create list with AssetVersions by filter criteria."""

    _isGroup = True
    enabled: bool = True
    cycle_hour_start: str = SettingsField(
        "00:00:00",
        title="Create daily lists at",
        description="This may take affect on next day",
        widget="time",
        regex="(?:[01]\d|2[0123]):(?:[012345]\d):(?:[012345]\d)",
        section="Automated execution",
        scope=["studio"],
    )
    cycle_days: list[str] = SettingsField(
        title="Days of week",
        default_factory=default_week_days,
        enum_resolver=week_days,
        scope=["studio"],
    )
    lists: list[DailyListItemModel] = SettingsField(
        title="Lists",
        default_factory=list,
    )
    role_list: list[str] = SettingsField(
        section="---",
        title=ROLES_TITLE,
        default_factory=list,
    )


class ComponentsSizeCalcModel(BaseSettingsModel):
    # Cannot be turned off per project
    enabled: bool = SettingsField(True, scope=["studio"])
    role_list: list[str] = SettingsField(
        title=ROLES_TITLE,
        default_factory=list,
    )


class FtrackServiceHandlers(BaseSettingsModel):
    """Settings for event handlers running in ftrack service."""

    prepare_project: SimpleAction = SettingsField(
        title="Prepare Project",
        default_factory=SimpleAction,
    )
    sync_from_ftrack: SimpleAction = SettingsField(
        title="Sync to AYON",
        default_factory=SimpleAction,
    )
    sync_hier_entity_attributes: SyncHierarchicalAttributes = SettingsField(
        title="Sync Hierarchical and Entity Attributes",
        default_factory=SyncHierarchicalAttributes,
    )
    clone_review_session: CloneReviewAction = SettingsField(
        title="Clone Review Session",
        default_factory=CloneReviewAction,
    )
    delete_ayon_entities: SimpleAction = SettingsField(
        title="Delete Folders/Products",
        default_factory=SimpleAction,
    )
    thumbnail_updates: ThumbnailHierarchyUpdates = SettingsField(
        title="Update Hierarchy thumbnails",
        default_factory=ThumbnailHierarchyUpdates,
    )
    status_task_to_parent: SyncStatusTaskToParent = SettingsField(
        title="Sync status from Task to Parent",
        default_factory=SyncStatusTaskToParent,
    )
    status_task_to_version: SyncStatusTaskToVersion = SettingsField(
        title="Sync status from Task to Version",
        default_factory=SyncStatusTaskToVersion,
    )
    status_version_to_task: SyncStatusVersionToTask = SettingsField(
        title="Sync status from Version to Task",
        default_factory=SyncStatusVersionToTask,
    )
    next_task_update: NextTaskUpdate = SettingsField(
        title="Update status on next task",
        default_factory=NextTaskUpdate,
    )
    transfer_values_of_hierarchical_attributes: TransferHierNonHierAttrsAction = SettingsField(
        title="Action to transfer hierarchical attribute values",
        default_factory=TransferHierNonHierAttrsAction,
    )
    # create_daily_review_session: CreateDailyReviewSession = SettingsField(
    #     title="Create daily review session",
    #     default_factory=CreateDailyReviewSession,
    # )
    create_daily_lists: CreateDailyListsModel = SettingsField(
        title="Create daily lists",
        default_factory=CreateDailyListsModel,
    )
    project_components_sizes: ComponentsSizeCalcModel = SettingsField(
        title="Calculate project component sizes",
        default_factory=ComponentsSizeCalcModel,
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
        "asset_types_filter_type": "allow_list",
        "asset_types": []
    },
    "status_version_to_task": {
        "enabled": True,
        "mapping": [],
        "asset_types_filter_type": "deny_list",
        "asset_types": []
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
    # "create_daily_review_session": {
    #     "enabled": True,
    #     "role_list": [
    #         "Administrator",
    #         "Project Manager"
    #     ],
    #     "cycle_enabled": False,
    #     "cycle_hour_start": "00:00:00",
    #     "review_session_template": "{yy}{mm}{dd}"
    # },
    "create_daily_lists": {
        "enabled": False,
        "role_list": [
            "Administrator",
            "Project Manager"
        ],
        "cycle_hour_start": "00:00:00",
        "cycle_days": [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday"
        ],
        "lists": [
            {
                "name_template": "{yy}{mm}{dd}",
                "category": "Dailies",
                "cycle_enabled": True,
                "filters": [
                    {
                        "statuses": [
                            "Approved"
                        ],
                        "custom_attributes": []
                    }
                ]
            }
        ],
    },
    "project_components_sizes": {
        "enabled": True,
        "role_list": [
            "Administrator",
            "Project Manager"
        ],
    },
}
