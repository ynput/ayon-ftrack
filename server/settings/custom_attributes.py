from ayon_server.settings import BaseSettingsModel, SettingsField


class CustomAttributeModel(BaseSettingsModel):
    write_security_roles: list[str] = SettingsField(
        default_factory=list,
        title="Write roles",
    )
    read_roles: list[str] = SettingsField(
        default_factory=list,
        title="Read roles",
    )


class ProjectCustomAttributesModel(BaseSettingsModel):
    auto_sync_enabled: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="AYON auto-sync",
    )
    library_project: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Library project",
    )
    applications: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Applications",
    )


class HierarchicalAttributesModel(BaseSettingsModel):
    ayon_id: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="AYON ID",
    )
    ayon_path: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="AYON path",
    )
    ayon_sync_failed: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="AYON sync failed",
    )
    tools_env: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Tools",
    )
    fps: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="FPS",
    )
    frameStart: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Frame start",
    )
    frameEnd: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Frame end",
    )
    clipIn: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Clip in",
    )
    clipOut: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Clip out",
    )
    handleStart: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Handle start",
    )
    handleEnd: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Handle end",
    )
    resolutionWidth: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Resolution width",
    )
    resolutionHeight: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Resolution height",
    )
    pixelAspect: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Pixel aspect",
    )


class CustomAttributesModel(BaseSettingsModel):
    show: ProjectCustomAttributesModel = SettingsField(
        default_factory=ProjectCustomAttributesModel,
        title="Project Custom attributes",
    )
    is_hierarchical: HierarchicalAttributesModel = SettingsField(
        default_factory=HierarchicalAttributesModel,
        title="Hierarchical Attributes",
    )


DEFAULT_CUSTOM_ATTRIBUTES_SETTINGS = {
    "show": {
        "auto_sync_enabled": {
            "write_security_roles": [
                "API",
                "Administrator"
            ],
            "read_security_roles": [
                "API",
                "Administrator"
            ]
        },
        "library_project": {
            "write_security_roles": [
                "API",
                "Administrator"
            ],
            "read_security_roles": [
                "API",
                "Administrator"
            ]
        },
        "applications": {
            "write_security_roles": [
                "API",
                "Administrator"
            ],
            "read_security_roles": [
                "API",
                "Administrator"
            ]
        }
    },
    "is_hierarchical": {
        "tools_env": {
            "write_security_roles": [
                "API",
                "Administrator"
            ],
            "read_security_roles": [
                "API",
                "Administrator"
            ]
        },
        "ayon_id": {
            "write_security_roles": [],
            "read_security_roles": [
                "API",
                "Administrator"
            ]
        },
        "ayon_path": {
            "write_security_roles": [],
            "read_security_roles": [
                "API",
                "Administrator"
            ]
        },
        "ayon_sync_failed": {
            "write_security_roles": [
                "API",
                "Administrator"
            ],
            "read_security_roles": [
                "API",
                "Administrator"
            ]
        },
        "fps": {
            "write_security_roles": [],
            "read_security_roles": []
        },
        "frameStart": {
            "write_security_roles": [],
            "read_security_roles": []
        },
        "frameEnd": {
            "write_security_roles": [],
            "read_security_roles": []
        },
        "clipIn": {
            "write_security_roles": [],
            "read_security_roles": []
        },
        "clipOut": {
            "write_security_roles": [],
            "read_security_roles": []
        },
        "handleStart": {
            "write_security_roles": [],
            "read_security_roles": []
        },
        "handleEnd": {
            "write_security_roles": [],
            "read_security_roles": []
        },
        "resolutionWidth": {
            "write_security_roles": [],
            "read_security_roles": []
        },
        "resolutionHeight": {
            "write_security_roles": [],
            "read_security_roles": []
        },
        "pixelAspect": {
            "write_security_roles": [],
            "read_security_roles": []
        }
    }
}
