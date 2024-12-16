from ayon_server.settings import BaseSettingsModel, SettingsField


def _attr_types():
    return [
        {"value": "hierarchical", "label": "Hierarchical"},
        {"value": "nonhierarchical", "label": "Non-hierarchical"},
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
    nonhierarchical: list[str] = SettingsField(
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
                "attr_type": "nonhierarchical",
                "hierarchical": "resolutionWidth",
                "nonhierarchical": [],
            },
            {
                "name": "resolutionHeight",
                "attr_type": "nonhierarchical",
                "hierarchical": "resolutionHeight",
                "nonhierarchical": [],
            },
            {
                "name": "pixelAspect",
                "attr_type": "nonhierarchical",
                "hierarchical": "pixelAspect",
                "nonhierarchical": [],
            },
            {
                "name": "fps",
                "attr_type": "nonhierarchical",
                "hierarchical": "fps",
                "nonhierarchical": ["fps"],
            },
            {
                "name": "frameStart",
                "attr_type": "nonhierarchical",
                "hierarchical": "frameStart",
                "nonhierarchical": ["fstart"],
            },
            {
                "name": "frameEnd",
                "attr_type": "nonhierarchical",
                "hierarchical": "frameEnd",
                "nonhierarchical": ["fend"],
            },
            {
                "name": "handleStart",
                "attr_type": "nonhierarchical",
                "hierarchical": "handleStart",
                "nonhierarchical": [],
            },
            {
                "name": "handleEnd",
                "attr_type": "nonhierarchical",
                "hierarchical": "handleEnd",
                "nonhierarchical": [],
            },
            {
                "name": "clipIn",
                "attr_type": "nonhierarchical",
                "hierarchical": "clipIn",
                "nonhierarchical": [],
            },
            {
                "name": "clipOut",
                "attr_type": "nonhierarchical",
                "hierarchical": "clipOut",
                "nonhierarchical": [],
            },
        ]
    }
}
