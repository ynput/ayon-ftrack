from typing import Any


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


def convert_settings_overrides(
    source_version: str,
    overrides: dict[str, Any],
) -> dict[str, Any]:
    _convert_integrate_ftrack_status_settings(overrides)
    _convert_task_to_version_status_mapping_1_2_0(overrides)
    _convert_version_to_task_status_mapping_1_2_0(overrides)
    return overrides
