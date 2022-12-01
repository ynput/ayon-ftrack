from typing import Type

from openpype.addons import BaseServerAddon
from openpype.lib.postgres import Postgres

from .ftrack_common import FTRACK_ID_ATTRIB
from .settings import FtrackSettings, DEFAULT_VALUES
from .version import __version__


class FtrackAddon(BaseServerAddon):
    name = "ftrack"
    title = "ftrack addon"
    version = __version__
    settings_model: Type[FtrackSettings] = FtrackSettings
    services = {
        "leecher": {"image": "openpype/ay-ftrack-leecher:1.0.0"},
        "processor": {"image": "openpype/ay-ftrack-processor:1.0.0"}
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
        attribute_data = {
            "type": "string",
            "title": "Ftrack id"
        }
        expected_scope = ["project", "folder", "task"]

        match_position = None
        position = 1
        if Postgres.pool is None:
            await Postgres.connect()
        async for row in Postgres.iterate(query):
            position += 1
            if row["name"] == FTRACK_ID_ATTRIB:
                # Check if scope is matching ftrack addon requirements
                if set(row["scope"]) == set(expected_scope):
                    return False

                match_position = row["position"]

        # Reuse position from found attribute
        if match_position is not None:
            position = match_position

        await Postgres.execute(
            """
            INSERT INTO public.attributes
                (name, position, scope, data)
            VALUES
                ($1, $2, $3, $4)
            ON CONFLICT (name)
            DO UPDATE SET
                scope = $3
                data = $4
            """,
            FTRACK_ID_ATTRIB,
            position,
            expected_scope,
            attribute_data,
        )
        return True

