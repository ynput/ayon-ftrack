from typing import Type, Any

import semver
from fastapi import Query
from nxtools import logging

from ayon_server.exceptions import AyonException, InvalidSettingsException
from ayon_server.addons import BaseServerAddon, AddonLibrary
from ayon_server.secrets import Secrets
from ayon_server.lib.postgres import Postgres
from ayon_server.api.dependencies import CurrentUser
from ayon_server.types import Field, OPModel

from .settings import (
    FtrackSettings,
    DEFAULT_VALUES,
    convert_settings_overrides,
)
from .constants import (
    FTRACK_ID_ATTRIB,
    FTRACK_PATH_ATTRIB,
)
from .ftrack_session import FtrackSession, InvalidCredentials, ServerError
from .ftrack_import import (
    prepare_attributes_mapping,
    create_update_attributes,
    import_projects,
)


class FtrackCredentialsPayload(OPModel):
    server_url: str = Field(..., title="ftrack server url")
    api_key: str = Field(..., title="ftrack api key")
    username: str = Field(..., title="ftrack username")


class AttributesCreateUpdatePayload(FtrackCredentialsPayload):
    attributes_mapping: dict[str, str] = Field(
        ..., title="Mapping of ftrack attributes to ayon attributes"
    )


class ProjectsSyncPayload(AttributesCreateUpdatePayload):
    project_names: list[str] = Field(
        ..., title="List of ftrack project names to sync"
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

    def initialize(self) -> None:
        self.add_endpoint(
            "/customProcessorHandlers",
            self.get_custom_processor_handlers,
            method="GET",
        )
        self.add_endpoint(
            "/ftrackProjects",
            self.get_ftrack_projects_info,
            method="GET",
        )
        self.add_endpoint(
            "/import/credentials",
            self._check_credentials,
            method="POST",
        )
        self.add_endpoint(
            "/import/ftrackAttributes",
            self._prepare_attr_mapping,
            method="POST",
        )
        self.add_endpoint(
            "/import/attributes",
            self._create_update_attributes,
            method="POST",
        )
        self.add_endpoint(
            "/import/projects",
            self._import_ftrack_projects,
            method="POST",
        )

    async def get_custom_processor_handlers(
        self,
        variant: str = Query("production"),
    ) -> {}:
        bundles = await Postgres.fetch(
            "SELECT name, is_production, is_staging,"
            " is_dev, data->'addons' as addons FROM bundles"
        )
        bundles_by_variant = {
            "production": None,
            "staging": None
        }
        for bundle in bundles:
            if bundle["is_dev"]:
                bundles_by_variant[bundle["name"]] = bundle
                continue

            if bundle["is_production"]:
                bundles_by_variant["production"] = bundle

            if bundle["is_staging"]:
                bundles_by_variant["staging"] = bundle

        handlers = []
        output = {"custom_handlers": handlers}
        if variant not in bundles_by_variant:
            return output
        addons = bundles_by_variant[variant]["addons"]
        addon_library = AddonLibrary.getinstance()
        for addon_name, addon_version in addons.items():
            addons_mapping = addon_library.get(addon_name) or {}
            addon = addons_mapping.get(addon_version)
            if not hasattr(addon, "get_custom_ftrack_handlers_endpoint"):
                continue
            try:
                endpoint = addon.get_custom_ftrack_handlers_endpoint()
                if endpoint:
                    handlers.append({
                        "addon_name": addon_name,
                        "addon_version": addon_version,
                        "endpoint": endpoint,
                    })
            except BaseException as exc:
                logging.warning(
                    f"Failed to receive ftrack handlers from addon"
                    f" {addon_name} {addon_version}. {exc}"
                )

        return output

    async def _prepare_ftrack_session(
        self,
        server_url: str,
        api_key: str,
        username: str,
    ) -> FtrackSession:
        missing = ", ".join([
            key
            for key, value in (
                ("Server url", server_url),
                ("API key", api_key),
                ("Username", username),
            )
            if not value
        ])
        if missing:
            raise AyonException(f"Please fill all fields. Missing {missing}.")

        session = FtrackSession(
            server_url, api_key, username
        )
        # TODO validate if url is valid first
        try:
            await session.validate()
        except InvalidCredentials:
            raise AyonException("Got invalid credentials.")
        except ServerError as exc:
            raise AyonException(
                "Unknown error occurred while connecting to ftrack server."
                f" {exc}"
            )
        return session

    async def get_ftrack_projects_info(
        self,
        user: CurrentUser,
        variant: str = Query("production"),
    ) -> {}:
        # TODO validate user permissions
        # - What permissions user must have to allow this endpoint?
        settings_model = await self.get_studio_settings(variant)
        ftrack_server_url = settings_model.ftrack_server
        service_settings = settings_model.service_settings
        api_key_secret = service_settings.api_key
        username_secret = service_settings.username

        if not ftrack_server_url or not api_key_secret or not username_secret:
            raise InvalidSettingsException("Required settings are not set.")

        ftrack_api_key = await Secrets.get(api_key_secret)
        ftrack_username = await Secrets.get(username_secret)

        if not ftrack_api_key or not ftrack_username:
            raise InvalidSettingsException(
                "Invalid service settings, secrets are not set."
            )

        session = await self._prepare_ftrack_session(
            ftrack_server_url,
            ftrack_api_key,
            ftrack_username,
        )
        projects = [
            {
                "id": project["id"],
                "name": project["full_name"],
                "code": project["name"],
                "active": project["status"] == "active",
            }
            async for project in await session.get_projects()
        ]

        return {
            "projects": projects,
        }

    async def _check_credentials(
        self,
        user: CurrentUser,
        payload: FtrackCredentialsPayload,
    ):
        session = FtrackSession(
            payload.server_url, payload.api_key, payload.username
        )
        user = await session.query(
            f"select id from User where username is '{payload.username}'"
        ).one()
        user_id = user["id"]
        role_types_by_id = {
            role_type["id"]: role_type
            for role_type in await session.query(
                "select id, name, type from SecurityRole"
            ).all()
        }
        security_roles = await session.query(
            "select security_role_id from UserSecurityRole"
            f" where user_id is '{user_id}'"
        ).all()
        role_names = {
            role_types_by_id[security_role["security_role_id"]]["name"].lower()
            for security_role in security_roles
        }
        return {
            "is_valid": True,
            "is_admin": "administrator" in role_names,
        }

    async def _prepare_attr_mapping(
        self,
        user: CurrentUser,
        payload: FtrackCredentialsPayload,
    ):
        session = await self._prepare_ftrack_session(
            payload.server_url,
            payload.api_key,
            payload.username,
        )
        return await prepare_attributes_mapping(session)

    async def _create_update_attributes(
        self,
        user: CurrentUser,
        payload: AttributesCreateUpdatePayload,
    ):
        session = await self._prepare_ftrack_session(
            payload.server_url,
            payload.api_key,
            payload.username,
        )
        return await create_update_attributes(
            session,
            payload.attributes_mapping,
        )

    async def _import_ftrack_projects(
        self,
        user: CurrentUser,
        payload: ProjectsSyncPayload,
    ):
        session = await self._prepare_ftrack_session(
            payload.server_url,
            payload.api_key,
            payload.username,
        )

        return await import_projects(
            session,
            user.name,
            payload.attributes_mapping,
            payload.project_names,
        )

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
            "title": "ftrack id",
            "inherit": False,
        }
        ftrack_path_attribute_data = {
            "type": "string",
            "title": "ftrack path",
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
        await convert_settings_overrides(source_version, overrides)
        return await super().convert_settings_overrides(
            source_version, overrides
        )
