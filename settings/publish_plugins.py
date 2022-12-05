import json
from pydantic import Field, validator

from openpype.settings import (
    BaseSettingsModel,
    ensure_unique_names,
)


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
    _isGroup = True
    enabled: bool = True
    profiles: list[CollectFamilyProfile] = Field(
        default_factory=list,
        title="Profiles",
    )


class CollectFtrackCustomAttributeDataModel(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    custom_attribute_keys: list[str] = Field(
        title="Custom attribute keys",
        default_factory=list,
    )


class ValidateFtrackAttributesModel(BaseSettingsModel):
    _isGroup = True
    enabled: bool = True
    ftrack_custom_attributes: str = Field(
        "{}",
        title="Custom attributes to validate",
        widget="textarea",
    )

    @validator("ftrack_custom_attributes")
    def json_parse(cls, value):
        """Ensure name fields within the lists have unique names."""

        parsed_data = json.loads(value)
        if not isinstance(parsed_data, dict):
            raise AssertionError(
                "Parsed value is {} but object is expected".format(
                    str(type(parsed_data))))
        return value


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


class IntegrateHierarchyToFtrackModel(BaseSettingsModel):
    _isGroup = True
    create_task_status_profiles: list[IntegrateHierarchyProfile] = Field(
        default_factory=list,
    )


class IntegrateFtrackNoteModel(BaseSettingsModel):
    _isGroup = True
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


class IntegrateFtrackDescriptionModel(BaseSettingsModel):
    _isGroup = True
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


class IntegrateFtrackComponentOverwriteModel(BaseSettingsModel):
    _isGroup = True
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


class IntegrateFtrackFamilyMapping(BaseSettingsModel):
    name: str = Field("", title="Family")
    asset_type: str = Field("", title="Asset Type")


def integrate_ftrack_metadata_enum():
    return [
        {"value": "openpype_version", "label": "OpenPype version"},
        {"value": "frame_start", "label": "Frame start"},
        {"value": "frame_end", "label": "Frame end"},
        {"value": "duration", "label": "Duration"},
        {"value": "width", "label": "Resolution width"},
        {"value": "height", "label": "Resolution height"},
        {"value": "fps", "label": "FPS"},
        {"value": "codec", "label": "Codec"}
    ]


class IntegrateFtrackInstanceModel(BaseSettingsModel):
    _isGroup = True
    family_mapping: list[IntegrateFtrackFamilyMapping] = Field(
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
    additional_metadata_keys: list[str] = Field(
        default_factory=list,
        title="Additional metadata keys on components",
        enum_resolver=integrate_ftrack_metadata_enum
    )

    @validator("family_mapping")
    def validate_unique_outputs(cls, value):
        ensure_unique_names(value)
        return value


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


class IntegrateFtrackFarmStatusModel(BaseSettingsModel):
    _isGroup = True
    farm_status_profiles: list[IntegrateFarmStartusProfile] = Field(
        title="Farm status profiles",
        default_factory=list,
    )


class FtrackPublishPlugins(BaseSettingsModel):
    """Settings for event handlers running in ftrack service."""

    CollectFtrackFamily: CollectFtrackFamilyPlugin = Field(
        title="Collect Ftrack Family",
        default_factory=CollectFtrackFamilyPlugin,
    )
    CollectFtrackCustomAttributeData: CollectFtrackCustomAttributeDataModel = (
        Field(
            title="Collect Custom Attribute Data",
            default_factory=CollectFtrackCustomAttributeDataModel,
            description=(
                "Collect custom attributes from ftrack for ftrack entities"
                " that can be used in some templates during publishing."
            )
        )
    )
    ValidateFtrackAttributes: ValidateFtrackAttributesModel = Field(
        title="Validate Ftrack Attributes",
        default_factory=ValidateFtrackAttributesModel,
    )
    IntegrateHierarchyToFtrack: IntegrateHierarchyToFtrackModel = Field(
        title="Integrate Hierarchy to ftrack",
        default_factory=IntegrateHierarchyToFtrackModel,
        description=(
            "Set task status on new task creation."
            " Ftrack's default status is used otherwise."
        )
    )
    IntegrateFtrackNote: IntegrateFtrackNoteModel = Field(
        title="Integrate Ftrack Note",
        default_factory=IntegrateFtrackNoteModel,
    )
    IntegrateFtrackDescription: IntegrateFtrackDescriptionModel = Field(
        title="Integrate Ftrack Description",
        default_factory=IntegrateFtrackDescriptionModel,
        description="Add description to integrated AssetVersion.",
    )
    IntegrateFtrackComponentOverwrite: IntegrateFtrackComponentOverwriteModel = Field(
        title="Integrate Ftrack Component Overwrite",
        default_factory=IntegrateFtrackComponentOverwriteModel,
    )
    IntegrateFtrackInstance: IntegrateFtrackInstanceModel = Field(
        title="Integrate Ftrack Instance",
        default_factory=IntegrateFtrackInstanceModel,
    )
    IntegrateFtrackFarmStatus: IntegrateFtrackFarmStatusModel = Field(
        title="Integrate Ftrack Farm Status",
        default_factory=IntegrateFtrackFarmStatusModel,
        description=(
            "Change status of task when it's subset is submitted to farm"
        ),
    )


DEFAULT_PUBLISH_SETTINGS = {
    "CollectFtrackFamily": {
        "enabled": True,
        "profiles": [
            {
                "hosts": [
                    "standalonepublisher"
                ],
                "families": [],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": True,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "standalonepublisher"
                ],
                "families": [
                    "matchmove",
                    "shot"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": False,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "standalonepublisher"
                ],
                "families": [
                    "plate"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": False,
                "advanced_filtering": [
                    {
                        "families": [
                            "clip",
                            "review"
                        ],
                        "add_ftrack_family": True
                    }
                ]
            },
            {
                "hosts": [
                    "traypublisher"
                ],
                "families": [],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": True,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "traypublisher"
                ],
                "families": [
                    "matchmove",
                    "shot"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": False,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "traypublisher"
                ],
                "families": [
                    "plate",
                    "review",
                    "audio"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": False,
                "advanced_filtering": [
                    {
                        "families": [
                            "clip",
                            "review"
                        ],
                        "add_ftrack_family": True
                    }
                ]
            },
            {
                "hosts": [
                    "maya"
                ],
                "families": [
                    "model",
                    "setdress",
                    "animation",
                    "look",
                    "rig",
                    "camera"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": True,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "tvpaint"
                ],
                "families": [
                    "renderPass"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": False,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "tvpaint"
                ],
                "families": [],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": True,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "nuke"
                ],
                "families": [
                    "write",
                    "render",
                    "prerender"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": False,
                "advanced_filtering": [
                    {
                        "families": [
                            "review"
                        ],
                        "add_ftrack_family": True
                    }
                ]
            },
            {
                "hosts": [
                    "aftereffects"
                ],
                "families": [
                    "render",
                    "workfile"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": True,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "flame"
                ],
                "families": [
                    "plate",
                    "take"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": True,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "houdini"
                ],
                "families": [
                    "usd"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": True,
                "advanced_filtering": []
            },
            {
                "hosts": [
                    "photoshop"
                ],
                "families": [
                    "review"
                ],
                "task_types": [],
                "task_names": [],
                "add_ftrack_family": True,
                "advanced_filtering": []
            }
        ]
    },
    "CollectFtrackCustomAttributeData": {
        "enabled": False,
        "custom_attribute_keys": []
    },
    "IntegrateHierarchyToFtrack": {
        "create_task_status_profiles": []
    },
    "IntegrateFtrackNote": {
        "enabled": True,
        "note_template": "{intent}: {comment}",
        "note_labels": []
    },
    "IntegrateFtrackDescription": {
        "enabled": False,
        "optional": True,
        "active": True,
        "description_template": "{comment}"
    },
    "ValidateFtrackAttributes": {
        "enabled": False,
        "ftrack_custom_attributes": "{}"
    },
    "IntegrateFtrackComponentOverwrite": {
        "enabled": True
    },
    "IntegrateFtrackInstance": {
        "family_mapping": [
            {
                "name": "camera",
                "asset_type": "cam"
            }, {
                "name": "look",
                "asset_type": "look"
            }, {
                "name": "mayaAscii",
                "asset_type": "scene"
            }, {
                "name": "model",
                "asset_type": "geo"
            }, {
                "name": "rig",
                "asset_type": "rig"
            }, {
                "name": "setdress",
                "asset_type": "setdress"
            }, {
                "name": "pointcache",
                "asset_type": "cache"
            }, {
                "name": "render",
                "asset_type": "render"
            }, {
                "name": "prerender",
                "asset_type": "render"
            }, {
                "name": "render2d",
                "asset_type": "render"
            }, {
                "name": "nukescript",
                "asset_type": "comp"
            }, {
                "name": "write",
                "asset_type": "render"
            }, {
                "name": "review",
                "asset_type": "mov"
            }, {
                "name": "plate",
                "asset_type": "img"
            }, {
                "name": "audio",
                "asset_type": "audio"
            }, {
                "name": "workfile",
                "asset_type": "scene"
            }, {
                "name": "animation",
                "asset_type": "cache"
            }, {
                "name": "image",
                "asset_type": "img"
            }, {
                "name": "reference",
                "asset_type": "reference"
            }, {
                "name": "ass",
                "asset_type": "cache"
            }, {
                "name": "mayaScene",
                "asset_type": "scene"
            }, {
                "name": "camerarig",
                "asset_type": "rig"
            }, {
                "name": "yeticache",
                "asset_type": "cache"
            }, {
                "name": "yetiRig",
                "asset_type": "rig"
            }, {
                "name": "xgen",
                "asset_type": "xgen"
            }, {
                "name": "rendersetup",
                "asset_type": "rendersetup"
            }, {
                "name": "assembly",
                "asset_type": "assembly"
            }, {
                "name": "layout",
                "asset_type": "layout"
            }, {
                "name": "unrealStaticMesh",
                "asset_type": "geo"
            }, {
                "name": "vrayproxy",
                "asset_type": "cache"
            }, {
                "name": "redshiftproxy",
                "asset_type": "cache",
            }, {
                "name": "usd",
                "asset_type": "usd"
            }
        ],
        "keep_first_subset_name_for_review": True,
        "asset_versions_status_profiles": [],
        "additional_metadata_keys": []
    },
    "IntegrateFtrackFarmStatus": {
        "farm_status_profiles": []
    }
}
