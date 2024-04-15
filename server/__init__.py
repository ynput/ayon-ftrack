import semver
from typing import Type, Any

from ayon_server.addons import BaseServerAddon, AddonLibrary
from ayon_server.lib.postgres import Postgres

from .settings import FtrackSettings, DEFAULT_VALUES
from .constants import (
    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
)


class FtrackAddon(BaseServerAddon):
    settings_model: Type[FtrackSettings] = FtrackSettings

    async def get_default_settings(self):
        settings_model_cls = self.get_settings_model()
        return settings_model_cls(**DEFAULT_VALUES)

    async def pre_setup(self):
        """Make sure older version of addon use the new way of attributes."""

        # Force older addon versions to skip creation of attributes
        #   - this was added in version 0.2.2
        instance = AddonLibrary.getinstance()
        app_defs = instance.data.get(self.name)
        my_version = semver.Version.parse(self.version)
        for version, addon in app_defs.versions.items():
            if version == self.version:
                continue
            try:
                addon_version = semver.Version.parse(version)
                if addon_version > my_version:
                    continue
            except Exception:
                pass
            if hasattr(addon, "create_ftrack_attributes"):
                addon.create_ftrack_attributes = (
                    self._empty_create_ftrack_attributes)

    async def setup(self):
        need_restart = await self.create_ftrack_attributes()
        if need_restart:
            self.request_server_restart()

    async def _empty_create_ftrack_attributes(self):
        return False

    async def create_ftrack_attributes(self) -> bool:
        """Make sure there are required attributes which ftrack addon needs.

        Returns:
            bool: 'True' if an attribute was created or updated.
        """

        query = "SELECT name, position, scope, data from public.attributes"
        ftrack_id_attribute_data = {
            "type": "string",
            "title": "Ftrack id",
            "inherit": False,
        }
        ftrack_path_attribute_data = {
            "type": "string",
            "title": "Ftrack path",
            "inherit": False,
        }
        ftrack_id_expected_scope = ["project", "folder", "task", "version"]
        ftrack_path_expected_scope = ["project", "folder", "task"]

        ftrack_id_match_position = None
        ftrack_id_matches = False
        ftrack_path_match_position = None
        ftrack_path_matches = False
        position = 1
        if Postgres.pool is None:
            await Postgres.connect()
        async for row in Postgres.iterate(query):
            position += 1
            if row["name"] == FTRACK_ID_ATTRIB:
                # Check if scope is matching ftrack addon requirements
                if not set(ftrack_id_expected_scope) - set(row["scope"]):
                    ftrack_id_matches = True
                ftrack_id_match_position = row["position"]

            elif row["name"] == FTRACK_PATH_ATTRIB:
                if not set(ftrack_path_expected_scope) - set(row["scope"]):
                    ftrack_path_matches = True
                ftrack_path_match_position = row["position"]

        if ftrack_id_matches and ftrack_path_matches:
            return False

        postgre_query = "\n".join((
            "INSERT INTO public.attributes",
            "    (name, position, scope, data)",
            "VALUES",
            "    ($1, $2, $3, $4)",
            "ON CONFLICT (name)",
            "DO UPDATE SET",
            "    scope = $3,",
            "    data = $4",
        ))
        if not ftrack_id_matches:
            # Reuse position from found attribute
            if ftrack_id_match_position is None:
                ftrack_id_match_position = position
                position += 1

            await Postgres.execute(
                postgre_query,
                FTRACK_ID_ATTRIB,
                ftrack_id_match_position,
                ftrack_id_expected_scope,
                ftrack_id_attribute_data,
            )

        if not ftrack_path_matches:
            if ftrack_path_match_position is None:
                ftrack_path_match_position = position
                position += 1

            await Postgres.execute(
                postgre_query,
                FTRACK_PATH_ATTRIB,
                ftrack_path_match_position,
                ftrack_path_expected_scope,
                ftrack_path_attribute_data,
            )
        return True

    async def convert_settings_overrides(
        self,
        source_version: str,
        overrides: dict[str, Any],
    ) -> dict[str, Any]:
        self._convert_integrate_ftrack_status_settings(overrides)
        # Use super conversion
        return await super().convert_settings_overrides(overrides)

    def _convert_integrate_ftrack_status_settings(self, overrides):
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
