import json
from pydantic import Field, validator

from openpype.settings import BaseSettingsModel


def parse_json_string(value):
    try:
        return json.dumps(value)
    except Exception:
        raise AssertionError("Your value couldn't be parsed as json")


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
    ftrack_custom_attributes: str = Field(
        "",
        title="Custom attributes to validate",
        widget="textarea",
    )

    @validator("ftrack_custom_attributes")
    def json_parse(cls, value):
        """Ensure name fields within the lists have unique names."""

        parsed_data = parse_json_string(value)
        if not isinstance(parsed_data):
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
    # These values did have a different label then value is
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
        "ftrack_custom_attributes": {}
    },
    "IntegrateFtrackComponentOverwrite": {
        "enabled": True
    },
    "IntegrateFtrackInstance": {
        "family_mapping": {
            "camera": "cam",
            "look": "look",
            "mayaAscii": "scene",
            "model": "geo",
            "rig": "rig",
            "setdress": "setdress",
            "pointcache": "cache",
            "render": "render",
            "prerender": "render",
            "render2d": "render",
            "nukescript": "comp",
            "write": "render",
            "review": "mov",
            "plate": "img",
            "audio": "audio",
            "workfile": "scene",
            "animation": "cache",
            "image": "img",
            "reference": "reference",
            "ass": "cache",
            "mayaScene": "scene",
            "camerarig": "rig",
            "yeticache": "cache",
            "yetiRig": "rig",
            "xgen": "xgen",
            "rendersetup": "rendersetup",
            "assembly": "assembly",
            "layout": "layout",
            "unrealStaticMesh": "geo",
            "vrayproxy": "cache",
            "redshiftproxy": "cache",
            "usd": "usd"
        },
        "keep_first_subset_name_for_review": True,
        "asset_versions_status_profiles": [],
        "additional_metadata_keys": []
    },
    "IntegrateFtrackFarmStatus": {
        "farm_status_profiles": []
    }
}
