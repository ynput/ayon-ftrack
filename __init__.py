from typing import Type

from openpype.addons import BaseServerAddon
from openpype.lib.postgres import Postgres

from .ftrack_common import FTRACK_ID_ATTRIB, FTRACK_PATH_ATTRIB
from .settings import FtrackSettings, DEFAULT_VALUES
from .version import __version__


class FtrackAddon(BaseServerAddon):
    name = "ftrack"
    title = "Ftrack"
    version = __version__
    settings_model: Type[FtrackSettings] = FtrackSettings
    services = {
        "leecher": {"image": "ynput/ayon-ftrack-leecher:0.0.1"},
        "processor": {"image": "ynput/ayon-ftrack-processor:0.0.1"}
    }

    async def get_default_settings(self):
        settings_model_cls = self.get_settings_model()
        return settings_model_cls(**DEFAULT_VALUES)

    async def setup(self):
        need_restart = await self.create_ftrack_attributes()
        if need_restart:
            self.request_server_restart()

    async def create_ftrack_attributes(self) -> bool:
        """Make sure there are required attributes which ftrack addon needs.

        Returns:
            bool: 'True' if an attribute was created or updated.
        """

        query = "SELECT name, position, scope, data from public.attributes"
        ftrack_id_attribute_data = {
            "type": "string",
            "title": "Ftrack id"
        }
        ftrack_path_attribute_data = {
            "type": "string",
            "title": "Ftrack path"
        }
        expected_scope = ["project", "folder", "task"]

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
                if set(row["scope"]) == set(expected_scope):
                    ftrack_id_matches = True
                ftrack_id_match_position = row["position"]

            elif row["name"] == FTRACK_PATH_ATTRIB:
                if set(row["scope"]) == set(expected_scope):
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
                expected_scope,
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
                expected_scope,
                ftrack_path_attribute_data,
            )
        return True

