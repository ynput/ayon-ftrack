from pydantic import Field

from openpype.settings import BaseSettingsModel


class CollectFamilyProfile(BaseSettingsModel):
    hosts: list[str] = Field(
        default_factory=list,
        title="Host names",
    )
    families: list[str] = Field(
        default_factory=list,
        title="Families",
    )
    task_types: list[str] = Field(
        default_factory=list,
        title="Task types",
    )
    task_names: list[str] = Field(
        default_factory=list,
        title="Task names",
    )
    add_ftrack_family: bool = Field(
        True,
        title="Add Ftrack Family",
    )


class CollectFtrackFamilyPlugin(BaseSettingsModel):
    enabled: bool = True
    profiles: list[CollectFamilyProfile] = Field(
        default_factory=list,
        title="Profiles",
    )


class CollectFtrackCustomAttributeDataPlugin(BaseSettingsModel):
    enabled: bool = True
    custom_attribute_keys: list[str] = Field(
        title="Custom attribute keys",
        default_factory=list,
    )


class ValidateFtrackAttributesPlugin(BaseSettingsModel):
    enabled: bool = True
    # TODO this is raw-json
    ftrack_custom_attributes: str = Field(
        "", title="Custom attributes to validate"
    )


class IntegrateHierarchyProfile(BaseSettingsModel):
    task_types: list[str] = Field(
        default_factory=list,
        title="Task types",
    )
    task_names: list[str] = Field(
        default_factory=list,
        title="Task names",
    )
    status_name: str = Field("", title="Status name")


class IntegrateHierarchyToFtrackPlugin(BaseSettingsModel):
    create_task_status_profiles: list[IntegrateHierarchyProfile] = Field(
        default_factory=list,
    )


class IntegrateFtrackNotePlugin(BaseSettingsModel):
    enabled: bool = True
    note_template: str = Field(
        "",
        title="Note template",
        description=(
            "Template may contain formatting keys <b>intent</b>,"
            " <b>comment</b>, <b>host_name</b>, <b>app_name</b>,"
            " <b>app_label</b>, <b>published_paths</b> and <b>source</b>."
        )
    )
    note_labels: list[str] = Field(
        title="Note labels",
        default_factory=list,
    )


class IntegrateFtrackDescriptionPlugin(BaseSettingsModel):
    enabled: bool = True
    optional: bool = Field(False, title="Optional")
    active: bool = Field(True, title="Active")
    description_template: str = Field(
        "",
        title="Description template",
        description=(
            "Template may contain formatting keys"
            " <b>intent</b> and <b>comment</b>."
        ),
    )


class IntegrateFtrackComponentOverwritePlugin(BaseSettingsModel):
    enabled: bool = True


class AssetVersionStatusProfile(BaseSettingsModel):
    hosts: list[str] = Field(
        default_factory=list,
        title="Host names",
    )
    families: list[str] = Field(
        default_factory=list,
        title="Families",
    )
    task_types: list[str] = Field(
        default_factory=list,
        title="Task types",
    )
    status: str = Field(
        "",
        title="Status name",
    )


class IntegrateFtrackInstancePlugin(BaseSettingsModel):
    family_mapping: list[str] = Field(
        title="Family Mapping",
        default_factory=list,
    )
    keep_first_subset_name_for_review: bool = Field(
        True,
        title="Make subset name as first asset name",
    )
    asset_versions_status_profiles: list[AssetVersionStatusProfile] = Field(
        title="AssetVersion status on publish",
        default_factory=list,
    )
    # {"openpype_version": "OpenPype version"},
    # {"frame_start": "Frame start"},
    # {"frame_end": "Frame end"},
    # {"duration": "Duration"},
    # {"width": "Resolution width"},
    # {"height": "Resolution height"},
    # {"fps": "FPS"},
    # {"code": "Codec"}
    additional_metadata_keys: list[str] = Field(
        title="Additional metadata keys on components",
        enum_resolver=lambda: [
            "openpype_version",
            "frame_start",
            "frame_end",
            "duration",
            "width",
            "height",
            "fps",
            "codec"
        ],
    )


class IntegrateFarmStartusProfile(BaseSettingsModel):
    hosts: list[str] = Field(
        default_factory=list,
        title="Host names",
    )
    families: list[str] = Field(
        default_factory=list,
        title="Families",
    )
    task_types: list[str] = Field(
        default_factory=list,
        title="Task types",
    )
    task_names: list[str] = Field(
        default_factory=list,
        title="Task names",
    )
    subsets: list[str] = Field(
        title="Subset names",
        default_factory=list,
    )
    status_name: str = Field(
        "",
        title="Status name"
    )


class IntegrateFtrackFarmStatusPlugin(BaseSettingsModel):
    farm_status_profiles: list = Field(
        title="Farm status profiles",
        default_factory=list,
    )


class FtrackPublishPlugins(BaseSettingsModel):
    """Settings for event handlers running in ftrack service."""

    CollectFtrackFamily: CollectFtrackFamilyPlugin = Field(
        title="Collect Ftrack Family",
        default_factory=CollectFtrackFamilyPlugin,
    )
    CollectFtrackCustomAttributeData: CollectFtrackCustomAttributeDataPlugin = Field(
        title="Collect Custom Attribute Data",
        default_factory=CollectFtrackCustomAttributeDataPlugin,
        description=(
            "Collect custom attributes from ftrack for ftrack entities"
            " that can be used in some templates during publishing."
        )
    )
    ValidateFtrackAttributes: ValidateFtrackAttributesPlugin = Field(
        title="Validate Ftrack Attributes",
        default_factory=ValidateFtrackAttributesPlugin,
    )
    IntegrateHierarchyToFtrack: IntegrateHierarchyToFtrackPlugin = Field(
        title="Integrate Hierarchy to ftrack",
        default_factory=IntegrateHierarchyToFtrackPlugin,
        description=(
            "Set task status on new task creation."
            " Ftrack's default status is used otherwise."
        )
    )
    IntegrateFtrackNote: IntegrateFtrackNotePlugin = Field(
        title="Integrate Ftrack Note",
        default_factory=IntegrateFtrackNotePlugin,
    )
    IntegrateFtrackDescription: IntegrateFtrackDescriptionPlugin = Field(
        title="Integrate Ftrack Description",
        default_factory=IntegrateFtrackDescriptionPlugin,
        description="Add description to integrated AssetVersion.",
    )
    IntegrateFtrackComponentOverwrite: IntegrateFtrackComponentOverwritePlugin = Field(
        title="Integrate Ftrack Component Overwrite",
        default_factory=IntegrateFtrackComponentOverwritePlugin,
    )
    IntegrateFtrackInstance: IntegrateFtrackInstancePlugin = Field(
        title="Integrate Ftrack Instance",
        default_factory=IntegrateFtrackInstancePlugin,
    )
    IntegrateFtrackFarmStatus: IntegrateFtrackFarmStatusPlugin = Field(
        title="Integrate Ftrack Farm Status",
        default_factory=IntegrateFtrackFarmStatusPlugin,
        description=(
            "Change status of task when it's subset is submitted to farm"
        ),
    )


DEFAULT_PUBLISH_SETTINGS = {}
