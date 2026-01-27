import copy
from typing import Any

import semver

from .custom_attributes import DEFAULT_CUSTOM_ATTRIBUTES_SETTINGS


def _convert_product_base_types(overrides):
    publish_settings = overrides.get("publish")
    if not publish_settings:
        return
    for combs in (
        ("CollectFtrackFamily", "profiles"),
        ("IntegrateFtrackInstance", "asset_versions_status_profiles"),
        ("IntegrateFtrackFarmStatus", "farm_status_profiles"),
    ):
        settings = publish_settings
        for key in combs:
            if key not in settings:
                settings = None
                break
            settings = settings[key]
        if not settings:
            continue

        f_profile = settings[0]
        if (
            "product_base_types" in f_profile
            or "product_types" not in f_profile
        ):
            continue

        for profile in settings:
            profile["product_base_types"] = profile.pop("product_types")

    mapping = publish_settings.get("IntegrateFtrackInstance") or {}
    if (
        "product_type_mapping" in mapping
        and "product_base_type_mapping" not in mapping
    ):
        mapping["product_base_type_mapping"] = (
            mapping.pop("product_type_mapping")
        )


def _convert_integrate_ftrack_status_settings(overrides):
    """Convert settings of 'IntegrateFtrackFarmStatus' profiles.

    This change happened in 1.1.0 version of the addon, where the settings
    were converted to use AYON naming convention over OpenPype convention.

    Args:
        overrides (dict[str, Any]): Settings overrides.
    """
    value = overrides
    for key in (
        "publish",
        "IntegrateFtrackFarmStatus",
        "farm_status_profiles",
    ):
        if not isinstance(value, dict) or key not in value:
            return

        value = value[key]

    if not isinstance(value, list):
        return

    for profile in value:
        for src_key, dst_key in (
            ("hosts", "host_names"),
            ("families", "product_types"),
            ("subset_names", "product_names"),
        ):
            if src_key in profile:
                profile[dst_key] = profile.pop(src_key)


def _convert_task_to_version_status_mapping_1_2_0(overrides):
    value = overrides
    for key in (
        "service_event_handlers",
        "status_task_to_version",
    ):
        value = value.get(key)
        if not value:
            return

    if "asset_types_filter" in value:
        value["asset_types"] = value.pop("asset_types_filter")
        value["asset_types_filter_type"] = "allow_list"


def _convert_version_to_task_status_mapping_1_2_0(overrides):
    value = overrides
    for key in (
        "service_event_handlers",
        "status_version_to_task",
    ):
        value = value.get(key)
        if not value:
            return

    if "asset_types_to_skip" in value:
        value["asset_types"] = value.pop("asset_types_to_skip")
        value["asset_types_filter_type"] = "deny_list"


def _apply_1_4_0_custom_attributes(overrides):
    mapping = copy.deepcopy(
        DEFAULT_CUSTOM_ATTRIBUTES_SETTINGS["attributes_mapping"]
    )
    mapping["enabled"] = False
    for item in mapping["mapping"]:
        item["attr_type"] = "hierarchical"

    cust_attr_overrides = overrides.setdefault("custom_attributes", {})
    cust_attr_overrides["attributes_mapping"] = mapping


def _convert_custom_attributes_1_4_0(overrides):
    """Convert custom attributes settings to 1.4.0 version.

    This change happened in 1.4.0 version of the addon, where the settings
    were converted to use AYON naming convention over OpenPype convention.

    Args:
        overrides (dict[str, Any]): Settings overrides.

    """
    if "custom_attributes" not in overrides:
        return

    cust_attr_overrides = overrides["custom_attributes"]
    show_overrides = cust_attr_overrides.pop("show", {})
    hier_overrides = cust_attr_overrides.pop("is_hierarchical", {})
    for attr_overrides, attr_names in [
        (
            show_overrides,
            {"auto_sync_enabled"}
        ),
        (
            hier_overrides,
            {"ayon_id", "ayon_path", "ayon_sync_failed"}
        ),
    ]:
        for key, value in attr_overrides.items():
            if key not in attr_names:
                continue

            new_value = {}
            for key in ("write_security_roles", "read_security_roles"):
                if key in value:
                    new_value[key] = value[key]
            if not new_value:
                continue
            mandatory_overrides = overrides.setdefault(
                "mandatory_attributes", {}
            )
            mandatory_overrides[key] = new_value


async def convert_settings_overrides(
    source_version: str,
    overrides: dict[str, Any],
) -> dict[str, Any]:
    parsed_version = semver.VersionInfo.parse(source_version)
    if parsed_version < (1, 4, 0):
        _apply_1_4_0_custom_attributes(overrides)

    _convert_integrate_ftrack_status_settings(overrides)
    _convert_task_to_version_status_mapping_1_2_0(overrides)
    _convert_version_to_task_status_mapping_1_2_0(overrides)
    _convert_custom_attributes_1_4_0(overrides)
    return overrides
