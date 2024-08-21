"""Integrate components into ftrack

Requires:
    context -> ftrackSession - connected ftrack.Session
    instance -> ftrackComponentsList - list of components to integrate

Provides:
    instance -> ftrackIntegratedAssetVersionsData
    # legacy
    instance -> ftrackIntegratedAssetVersions
"""

import os
import collections

import pyblish.api
import clique

from ayon_ftrack.common.constants import FTRACK_ID_ATTRIB
from ayon_ftrack.pipeline import plugin


class IntegrateFtrackApi(plugin.FtrackPublishInstancePlugin):
    """ Commit components to server. """

    order = pyblish.api.IntegratorOrder + 0.499
    label = "Integrate Ftrack Api"
    families = ["ftrack"]

    def process(self, instance):
        component_list = instance.data.get("ftrackComponentsList")
        if not component_list:
            self.log.debug(
                "Instance doesn't have components to integrate to Ftrack."
                " Skipping."
            )
            return

        context = instance.context
        task_entity, parent_entity = self.get_instance_entities(
            instance, context)
        if parent_entity is None:
            self.log.debug((
                "Skipping ftrack integration. Instance \"{}\" does not"
                " have specified ftrack entities."
            ).format(str(instance)))
            return

        session = context.data["ftrackSession"]
        # Reset session operations and reconfigure locations
        session.recorded_operations.clear()
        session._configure_locations()

        try:
            self.integrate_to_ftrack(
                session,
                instance,
                task_entity,
                parent_entity,
                component_list
            )

        except Exception:
            session.reset()
            raise

    def get_instance_entities(self, instance, context):
        parent_entity = None
        task_entity = None
        # If instance has set "ftrackEntity" or "ftrackTask" then use them from
        #   instance. Even if they are set to None. If they are set to None it
        #   has a reason. (like has different context)
        if "ftrackEntity" in instance.data or "ftrackTask" in instance.data:
            task_entity = instance.data.get("ftrackTask")
            parent_entity = instance.data.get("ftrackEntity")

        elif "ftrackEntity" in context.data or "ftrackTask" in context.data:
            task_entity = context.data.get("ftrackTask")
            parent_entity = context.data.get("ftrackEntity")

        if task_entity:
            parent_entity = task_entity["parent"]

        return task_entity, parent_entity

    def integrate_to_ftrack(
        self,
        session,
        instance,
        task_entity,
        parent_entity,
        component_list
    ):
        default_asset_name = None
        if task_entity:
            default_asset_name = task_entity["name"]

        if not default_asset_name:
            default_asset_name = parent_entity["name"]

        # Change status on task
        asset_version_status_ids_by_name = {}
        project_entity = instance.context.data.get("ftrackProject")
        if project_entity:
            project_schema = project_entity["project_schema"]
            asset_version_statuses = (
                project_schema.get_statuses("AssetVersion")
            )
            asset_version_status_ids_by_name = {
                status["name"].lower(): status["id"]
                for status in asset_version_statuses
            }

        # Prepare AssetTypes
        asset_types_by_short = self._ensure_asset_types_exists(
            session, component_list
        )
        self._fill_component_locations(session, component_list)

        asset_versions_data_by_id = {}
        used_asset_versions = []

        # Iterate over components and publish
        for data in component_list:
            self.log.debug("data: {}".format(data))

            # AssetType
            asset_type_short = data["assettype_data"]["short"]
            asset_type_entity = asset_types_by_short[asset_type_short]

            # Asset
            asset_data = data.get("asset_data") or {}
            if "name" not in asset_data:
                asset_data["name"] = default_asset_name
            asset_entity = self._ensure_asset_exists(
                session,
                asset_data,
                asset_type_entity["id"],
                parent_entity["id"]
            )

            # Asset Version
            asset_version_data = data.get("assetversion_data") or {}
            asset_version_entity = self._ensure_asset_version_exists(
                session,
                asset_version_data,
                asset_entity["id"],
                task_entity,
                asset_version_status_ids_by_name
            )

            # Store asset version and components items that were
            version_id = asset_version_entity["id"]
            if version_id not in asset_versions_data_by_id:
                asset_versions_data_by_id[version_id] = {
                    "asset_version": asset_version_entity,
                    "component_items": []
                }

            asset_versions_data_by_id[version_id]["component_items"].append(
                data
            )

            # Backwards compatibility
            if asset_version_entity not in used_asset_versions:
                used_asset_versions.append(asset_version_entity)

            # for version attributes `IntegrateVersionAttributes`
            version_attributes = instance.data.setdefault(
                "versionAttributes", {}
            )
            version_attributes[FTRACK_ID_ATTRIB] = version_id

        self._create_components(session, asset_versions_data_by_id)

        instance.data["ftrackIntegratedAssetVersionsData"] = (
            asset_versions_data_by_id
        )

        # Backwards compatibility
        asset_versions_key = "ftrackIntegratedAssetVersions"
        if asset_versions_key not in instance.data:
            instance.data[asset_versions_key] = []

        for asset_version in used_asset_versions:
            if asset_version not in instance.data[asset_versions_key]:
                instance.data[asset_versions_key].append(asset_version)

    def _fill_component_locations(self, session, component_list):
        components_by_location_name = collections.defaultdict(list)
        components_by_location_id = collections.defaultdict(list)
        for component_item in component_list:
            # Location entity can be prefilled
            # - this is not recommended as connection to ftrack server may
            #   be lost and in that case the entity is not valid when gets
            #   to this plugin
            location = component_item.get("component_location")
            if location is not None:
                continue

            # Collect location id
            location_id = component_item.get("component_location_id")
            if location_id:
                components_by_location_id[location_id].append(
                    component_item
                )
                continue

            location_name = component_item.get("component_location_name")
            if location_name:
                components_by_location_name[location_name].append(
                    component_item
                )
                continue

        # Skip if there is nothing to do
        if not components_by_location_name and not components_by_location_id:
            return

        # Query locations
        query_filters = []
        if components_by_location_id:
            joined_location_ids = ",".join([
                '"{}"'.format(location_id)
                for location_id in components_by_location_id
            ])
            query_filters.append("id in ({})".format(joined_location_ids))

        if components_by_location_name:
            joined_location_names = ",".join([
                '"{}"'.format(location_name)
                for location_name in components_by_location_name
            ])
            query_filters.append("name in ({})".format(joined_location_names))

        locations = session.query(
            "select id, name from Location where {}".format(
                " or ".join(query_filters)
            )
        ).all()
        # Fill locations in components
        for location in locations:
            location_id = location["id"]
            location_name = location["name"]
            if location_id in components_by_location_id:
                for component in components_by_location_id[location_id]:
                    component["component_location"] = location

            if location_name in components_by_location_name:
                for component in components_by_location_name[location_name]:
                    component["component_location"] = location

    def _ensure_asset_types_exists(self, session, component_list):
        """Make sure that all AssetType entities exists for integration.

        Returns:
            dict: All asset types by short name.
        """
        # Query existing asset types
        asset_types = session.query("select id, short from AssetType").all()
        # Stpore all existing short names
        asset_type_shorts = {asset_type["short"] for asset_type in asset_types}
        # Check which asset types are missing and store them
        asset_type_names_by_missing_shorts = {}
        default_short_name = "upload"
        for data in component_list:
            asset_type_data = data.get("assettype_data") or {}
            asset_type_short = asset_type_data.get("short")
            if not asset_type_short:
                # Use default asset type name if not set and change the
                #   input data
                asset_type_short = default_short_name
                asset_type_data["short"] = asset_type_short
                data["assettype_data"] = asset_type_data

            if (
                # Skip if short name exists
                asset_type_short in asset_type_shorts
                # Skip if short name was already added to missing types
                #   and asset type name is filled
                # - if asset type name is missing then try use name from other
                #   data
                or asset_type_names_by_missing_shorts.get(asset_type_short)
            ):
                continue

            asset_type_names_by_missing_shorts[asset_type_short] = (
                asset_type_data.get("name")
            )

        # Create missing asset types if there are any
        if asset_type_names_by_missing_shorts:
            self.log.info("Creating asset types with short names: {}".format(
                ", ".join(asset_type_names_by_missing_shorts.keys())
            ))
            for missing_short, type_name in (
                asset_type_names_by_missing_shorts.items()
            ):
                # Use short for name if name is not defined
                if not type_name:
                    type_name = missing_short
                # Use short name also for name
                #   - there is not other source for 'name'
                session.create(
                    "AssetType",
                    {
                        "short": missing_short,
                        "name": type_name
                    }
                )

            # Commit creation
            session.commit()
            # Requery asset types
            asset_types = session.query(
                "select id, short from AssetType"
            ).all()

        return {asset_type["short"]: asset_type for asset_type in asset_types}

    def _ensure_asset_exists(
        self, session, asset_data, asset_type_id, parent_id
    ):
        asset_name = asset_data["name"]
        asset_entity = self._query_asset(
            session, asset_name, asset_type_id, parent_id
        )
        if asset_entity is not None:
            return asset_entity

        asset_data = {
            "name": asset_name,
            "type_id": asset_type_id,
            "context_id": parent_id
        }
        self.log.debug("Created new Asset with data: {}.".format(asset_data))
        session.create("Asset", asset_data)
        session.commit()
        return self._query_asset(session, asset_name, asset_type_id, parent_id)

    def _query_asset(self, session, asset_name, asset_type_id, parent_id):
        return session.query(
            (
                "select id from Asset"
                " where name is \"{}\""
                " and type_id is \"{}\""
                " and context_id is \"{}\""
            ).format(asset_name, asset_type_id, parent_id)
        ).first()

    def _ensure_asset_version_exists(
        self,
        session,
        asset_version_data,
        asset_id,
        task_entity,
        status_ids_by_name
    ):
        task_id = None
        if task_entity:
            task_id = task_entity["id"]

        status_name = asset_version_data.pop("status_name", None)

        # Try query asset version by criteria (asset id and version)
        version = asset_version_data.get("version") or 0
        asset_version_entity = self._query_asset_version(
            session, version, asset_id
        )

        # Prepare comment value
        comment = asset_version_data.get("comment") or ""
        if asset_version_entity is not None:
            changed = False
            if comment != asset_version_entity["comment"]:
                asset_version_entity["comment"] = comment
                changed = True

            if task_id != asset_version_entity["task_id"]:
                asset_version_entity["task_id"] = task_id
                changed = True

            if changed:
                session.commit()

        else:
            # Convert '0' version to string `"0"`
            # - ftrack handles `0` as empty value
            if version == 0:                
                version = "0"

            new_asset_version_data = {
                "version": version,
                "asset_id": asset_id
            }
            if task_id:
                new_asset_version_data["task_id"] = task_id

            if comment:
                new_asset_version_data["comment"] = comment

            self.log.debug("Created new AssetVersion with data {}".format(
                new_asset_version_data
            ))
            session.create("AssetVersion", new_asset_version_data)
            session.commit()
            asset_version_entity = self._query_asset_version(
                session, version, asset_id
            )

        if status_name:
            status_id = status_ids_by_name.get(status_name.lower())
            if not status_id:
                self.log.info((
                    "Ftrack status with name \"{}\""
                    " for AssetVersion was not found."
                ).format(status_name))

            elif asset_version_entity["status_id"] != status_id:
                asset_version_entity["status_id"] = status_id
                session.commit()

        # Set custom attributes if there were any set
        custom_attrs = asset_version_data.get("custom_attributes") or {}
        for attr_key, attr_value in custom_attrs.items():
            if attr_key in asset_version_entity["custom_attributes"]:
                try:
                    asset_version_entity["custom_attributes"][attr_key] = (
                        attr_value
                    )
                    session.commit()
                    continue
                except Exception:
                    session.rollback()
                    session._configure_locations()

            self.log.warning(
                (
                    "Custom Attribute \"{0}\" is not available for"
                    " AssetVersion <{1}>. Can't set it's value to: \"{2}\""
                ).format(
                    attr_key, asset_version_entity["id"], str(attr_value)
                )
            )

        return asset_version_entity

    def _query_asset_version(self, session, version, asset_id):
        return session.query(
            (
                "select id, task_id, comment from AssetVersion"
                " where version is \"{}\" and asset_id is \"{}\""
            ).format(version, asset_id)
        ).first()

    def create_component(self, session, asset_version_entity, data):
        component_data = data.get("component_data") or {}

        if not component_data.get("name"):
            component_data["name"] = "main"

        version_id = asset_version_entity["id"]
        component_data["version_id"] = version_id
        component_entity = session.query(
            (
                "select id, name from Component where name is \"{}\""
                " and version_id is \"{}\""
            ).format(component_data["name"], version_id)
        ).first()

        component_overwrite = data.get("component_overwrite", False)
        location = data.get("component_location", session.pick_location())

        # Overwrite existing component data if requested.
        if component_entity and component_overwrite:
            origin_location = session.query(
                "Location where name is \"ftrack.origin\""
            ).one()

            # Removing existing members from location
            components = list(component_entity.get("members", []))
            components += [component_entity]
            for component in components:
                for loc in component["component_locations"]:
                    if location["id"] == loc["location_id"]:
                        location.remove_component(
                            component, recursive=False
                        )

            # Deleting existing members on component entity
            for member in component_entity.get("members", []):
                session.delete(member)
                del(member)

            session.commit()

            # Reset members in memory
            if "members" in component_entity.keys():
                component_entity["members"] = []

            # Add components to origin location
            try:
                collection = clique.parse(data["component_path"])
            except ValueError:
                # Assume its a single file
                # Changing file type
                name, ext = os.path.splitext(data["component_path"])
                component_entity["file_type"] = ext

                origin_location.add_component(
                    component_entity, data["component_path"]
                )
            else:
                # Changing file type
                component_entity["file_type"] = collection.format("{tail}")

                # Create member components for sequence.
                for member_path in collection:

                    size = 0
                    try:
                        size = os.path.getsize(member_path)
                    except OSError:
                        pass

                    name = collection.match(member_path).group("index")

                    member_data = {
                        "name": name,
                        "container": component_entity,
                        "size": size,
                        "file_type": os.path.splitext(member_path)[-1]
                    }

                    component = session.create(
                        "FileComponent", member_data
                    )
                    origin_location.add_component(
                        component, member_path, recursive=False
                    )
                    component_entity["members"].append(component)

            # Add components to location.
            location.add_component(
                component_entity, origin_location, recursive=True
            )

            data["component"] = component_entity
            self.log.info(
                (
                    "Overwriting Component with path: {0}, data: {1},"
                    " location: {2}"
                ).format(
                    data["component_path"],
                    component_data,
                    location
                )
            )

        # Extracting metadata, and adding after entity creation. This is
        # due to a ftrack_api bug where you can't add metadata on creation.
        component_metadata = component_data.pop("metadata", {})

        # Create new component if none exists.
        new_component = False
        if not component_entity:
            component_entity = asset_version_entity.create_component(
                data["component_path"],
                data=component_data,
                location=location
            )
            data["component"] = component_entity
            self.log.debug(
                (
                    "Created new Component with path: {0}, data: {1},"
                    " metadata: {2}, location: {3}"
                ).format(
                    data["component_path"],
                    component_data,
                    component_metadata,
                    location
                )
            )
            new_component = True

        # Adding metadata
        existing_component_metadata = component_entity["metadata"]
        existing_component_metadata.update(component_metadata)
        component_entity["metadata"] = existing_component_metadata

        # if component_data['name'] = 'ftrackreview-mp4-mp4':
        #     assetversion_entity["thumbnail_id"]

        # Setting assetversion thumbnail
        if data.get("thumbnail"):
            asset_version_entity["thumbnail_id"] = component_entity["id"]

        # Inform user about no changes to the database.
        if (
            component_entity
            and not component_overwrite
            and not new_component
        ):
            data["component"] = component_entity
            self.log.info(
                "Found existing component, and no request to overwrite. "
                "Nothing has been changed."
            )
        else:
            # Commit changes.
            session.commit()

    def _create_components(self, session, asset_versions_data_by_id):
        for item in asset_versions_data_by_id.values():
            asset_version_entity = item["asset_version"]
            component_items = item["component_items"]

            component_entities = session.query(
                (
                    "select id, name from Component where version_id is \"{}\""
                ).format(asset_version_entity["id"])
            ).all()

            existing_component_names = {
                component["name"]
                for component in component_entities
            }

            contain_review = "ftrackreview-mp4" in existing_component_names
            thumbnail_component_item = None
            for component_item in component_items:
                component_data = component_item.get("component_data") or {}
                component_name = component_data.get("name")
                if component_name == "ftrackreview-mp4":
                    contain_review = True
                elif component_name == "ftrackreview-image":
                    thumbnail_component_item = component_item

            if contain_review and thumbnail_component_item:
                thumbnail_component_item["component_data"]["name"] = (
                    "thumbnail"
                )

            # Component
            for component_item in component_items:
                self.create_component(
                    session, asset_version_entity, component_item
                )
