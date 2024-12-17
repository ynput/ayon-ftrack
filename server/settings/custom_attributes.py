from ayon_server.settings import BaseSettingsModel, SettingsField


def _attr_types():
    return [
        {"value": "hierarchical", "label": "Hierarchical"},
        {"value": "standard", "label": "Standard"},
    ]


class CustomAttributeMappingModel(BaseSettingsModel):
    name: str = SettingsField("", title="AYON attribute")
    attr_type: str = SettingsField(
        "hierarchical",
        title="Attribute type",
        enum_resolver=_attr_types,
        conditionalEnum=True,
    )
    hierarchical: str = SettingsField(
        "",
        title="ftrack attribute name",
    )
    standard: list[str] = SettingsField(
        default_factory=list,
        title="ftrack attribute names",
    )


class CustomAttributesMappingModel(BaseSettingsModel):
    enabled: bool = SettingsField(
        True,
        description="Use custom attributes mapping",
    )
    mapping: list[CustomAttributeMappingModel] = SettingsField(
        default_factory=list,
        title="Attributes mapping",
    )


class CustomAttributeModel(BaseSettingsModel):
    write_security_roles: list[str] = SettingsField(
        default_factory=list,
        title="Write roles",
    )
    read_security_roles: list[str] = SettingsField(
        default_factory=list,
        title="Read roles",
    )


class MandatoryAttributesModel(BaseSettingsModel):
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
    auto_sync_enabled: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="AYON auto-sync",
    )
    library_project: CustomAttributeModel = CustomAttributeModel(
        default_factory=CustomAttributeModel,
        title="Library project",
    )


class CustomAttributesModel(BaseSettingsModel):
    mandatory_attributes: MandatoryAttributesModel = SettingsField(
        default_factory=MandatoryAttributesModel,
        title="Mandatory attributes",
    )
    attributes_mapping: CustomAttributesMappingModel = SettingsField(
        default_factory=CustomAttributesMappingModel,
        title="Attributes mapping",
    )



DEFAULT_CUSTOM_ATTRIBUTES_SETTINGS = {
    "mandatory_attributes": {
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
        "ayon_id": {
            "write_security_roles": [],
            "read_security_roles": []
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
    },
    "attributes_mapping": {
        "enabled": True,
        "mapping": [
            {
                "name": "resolutionWidth",
                "attr_type": "standard",
                "hierarchical": "resolutionWidth",
                "standard": [],
            },
            {
                "name": "resolutionHeight",
                "attr_type": "standard",
                "hierarchical": "resolutionHeight",
                "standard": [],
            },
            {
                "name": "pixelAspect",
                "attr_type": "standard",
                "hierarchical": "pixelAspect",
                "standard": [],
            },
            {
                "name": "fps",
                "attr_type": "standard",
                "hierarchical": "fps",
                "standard": ["fps"],
            },
            {
                "name": "frameStart",
                "attr_type": "standard",
                "hierarchical": "frameStart",
                "standard": ["fstart"],
            },
            {
                "name": "frameEnd",
                "attr_type": "standard",
                "hierarchical": "frameEnd",
                "standard": ["fend"],
            },
            {
                "name": "handleStart",
                "attr_type": "standard",
                "hierarchical": "handleStart",
                "standard": [],
            },
            {
                "name": "handleEnd",
                "attr_type": "standard",
                "hierarchical": "handleEnd",
                "standard": [],
            },
            {
                "name": "clipIn",
                "attr_type": "standard",
                "hierarchical": "clipIn",
                "standard": [],
            },
            {
                "name": "clipOut",
                "attr_type": "standard",
                "hierarchical": "clipOut",
                "standard": [],
            },
        ]
    }
}
