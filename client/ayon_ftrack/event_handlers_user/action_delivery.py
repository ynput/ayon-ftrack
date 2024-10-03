import os
import copy
import json
import collections

from ayon_api import (
    get_attributes_for_type,
    get_project,
    get_folders,
    get_products,
    get_versions,
    get_representations,
)

from ayon_ftrack.common import (
    LocalAction,
    query_custom_attribute_values,
    CUST_ATTR_KEY_SERVER_ID,
    FTRACK_ID_ATTRIB,
)
from ayon_ftrack.lib import get_ftrack_icon_url

from ayon_core.lib.dateutils import get_datetime_data
from ayon_core.pipeline import Anatomy
from ayon_core.pipeline.load import get_representation_path_with_anatomy
from ayon_core.pipeline.delivery import (
    get_format_dict,
    check_destination_path,
    deliver_single_file,
    deliver_sequence,
)


class Delivery(LocalAction):
    identifier = "delivery.action"
    label = "Delivery"
    description = "Deliver data to client"
    role_list = ["Administrator", "Project manager"]
    icon = get_ftrack_icon_url("Delivery.svg")
    settings_key = "delivery_action"

    def discover(self, session, entities, event):
        is_valid = False
        for entity in entities:
            if entity.entity_type.lower() in ("assetversion", "reviewsession"):
                is_valid = True
                break

        if is_valid:
            is_valid = self.valid_roles(session, entities, event)
        return is_valid

    def interface(self, session, entities, event):
        if event["data"].get("values"):
            return

        title = "Delivery data to Client"

        items = []
        item_splitter = {"type": "label", "value": "---"}

        project_entity = self.get_project_from_entity(entities[0])
        project_name = project_entity["full_name"]
        project_entity = get_project(project_name, fields=["name"])
        if not project_entity:
            return {
                "success": False,
                "message": f"Project \"{project_name}\" not found in AYON."
            }

        repre_names = self._get_repre_names(project_name, session, entities)

        items.append({
            "type": "hidden",
            "name": "__project_name__",
            "value": project_name
        })

        # Prepare anatomy data
        anatomy = Anatomy(project_name)
        first = None
        delivery_templates = anatomy.templates.get("delivery") or {}
        default_keys = {
            "frame", "version", "frame_padding", "version_padding"
        }
        delivery_templates_items = []
        for key, template in delivery_templates.items():
            if key in default_keys:
                continue
            # Use only keys with `{root}` or `{root[*]}` in value
            delivery_templates_items.append({
                "label": key,
                "value": key
            })
            if first is None:
                first = key

        skipped = False
        # Add message if there are any common components
        if not repre_names or not delivery_templates_items:
            skipped = True
            items.append({
                "type": "label",
                "value": "<h1>Something went wrong:</h1>"
            })

        items.append({
            "type": "hidden",
            "name": "__skipped__",
            "value": skipped
        })

        if not repre_names:
            if len(entities) == 1:
                items.append({
                    "type": "label",
                    "value": (
                        "- Selected entity doesn't have components to deliver."
                    )
                })
            else:
                items.append({
                    "type": "label",
                    "value": (
                        "- Selected entities don't have common components."
                    )
                })

        # Add message if delivery anatomies are not set
        if not delivery_templates_items:
            items.append({
                "type": "label",
                "value": (
                    "- `\"delivery\"` anatomy key is not set in config."
                )
            })

        # Skip if there are any data shortcomings
        if skipped:
            return {
                "items": items,
                "title": title
            }

        items.append({
            "value": "<h1>Choose Components to deliver</h1>",
            "type": "label"
        })

        for repre_name in repre_names:
            items.append({
                "type": "boolean",
                "value": False,
                "label": repre_name,
                "name": repre_name
            })

        items.append(item_splitter)

        items.append({
            "value": "<h2>Location for delivery</h2>",
            "type": "label"
        })

        items.append({
            "type": "label",
            "value": (
                "<i>NOTE: It is possible to replace `root` key in anatomy.</i>"
            )
        })

        items.append({
            "type": "text",
            "name": "__location_path__",
            "empty_text": "Type location path here...(Optional)"
        })

        items.append(item_splitter)

        items.append({
            "value": "<h2>Anatomy of delivery files</h2>",
            "type": "label"
        })

        items.append({
            "type": "label",
            "value": (
                "<p><i>NOTE: These can be set in Anatomy.yaml"
                " within `delivery` key.</i></p>"
            )
        })

        items.append({
            "type": "enumerator",
            "name": "__delivery_template__",
            "data": delivery_templates_items,
            "value": first
        })

        return {
            "items": items,
            "title": title
        }

    def launch(self, session, entities, event):
        values = event["data"].get("values")
        if not values:
            return {
                "success": True,
                "message": "Nothing to do"
            }

        if FTRACK_ID_ATTRIB not in get_attributes_for_type("folder"):
            return {
                "success": False,
                "message": (
                    f"AYON server does not have '{FTRACK_ID_ATTRIB}'"
                    " attribute available."
                )
            }

        skipped = values.pop("__skipped__")
        if skipped:
            return {
                "success": False,
                "message": "Action skipped"
            }

        user_id = event["source"]["user"]["id"]
        user_entity = session.query(
            "User where id is {}".format(user_id)
        ).one()

        job = session.create("Job", {
            "user": user_entity,
            "status": "running",
            "data": json.dumps({
                "description": "Delivery processing."
            })
        })
        session.commit()

        try:
            report = self.real_launch(session, entities, event)

        except Exception as exc:
            report = {
                "success": False,
                "title": "Delivery failed",
                "items": [{
                    "type": "label",
                    "value": (
                        "Error during delivery action process:<br>{}"
                        "<br><br>Check logs for more information."
                    ).format(str(exc))
                }]
            }
            self.log.warning(
                "Failed during processing delivery action.",
                exc_info=True
            )

        finally:
            if report["success"]:
                job["status"] = "done"
            else:
                job["status"] = "failed"
            session.commit()

        if not report["success"]:
            self.show_interface(
                items=report["items"],
                title=report["title"],
                event=event
            )
            return {
                "success": False,
                "message": "Errors during delivery process. See report."
            }

        return report

    def real_launch(self, session, entities, event):
        self.log.info("Delivery action just started.")
        report_items = collections.defaultdict(list)

        values = event["data"]["values"]

        location_path = values.pop("__location_path__")
        anatomy_name = values.pop("__delivery_template__")
        project_name = values.pop("__project_name__")

        repre_names = set()
        for key, value in values.items():
            if value is True:
                repre_names.add(key)

        if not repre_names:
            return {
                "success": True,
                "message": "No selected components to deliver."
            }

        location_path = location_path.strip()
        if location_path:
            location_path = os.path.normpath(location_path)
            if not os.path.exists(location_path):
                os.makedirs(location_path)

        self.log.debug("Collecting representations to process.")
        version_ids = self._get_interest_version_ids(
            project_name, session, entities
        )
        repres_to_deliver = list(get_representations(
            project_name,
            representation_names=repre_names,
            version_ids=version_ids
        ))
        anatomy = Anatomy(project_name)

        format_dict = get_format_dict(anatomy, location_path)

        datetime_data = get_datetime_data()
        for repre in repres_to_deliver:
            source_path = repre["attrib"]["path"]
            debug_msg = "Processing representation {}".format(repre["id"])
            if source_path:
                debug_msg += " with published path {}.".format(source_path)
            self.log.debug(debug_msg)

            anatomy_data = copy.deepcopy(repre["context"])

            if "product" not in anatomy_data:
                product_value = {}

                product_name = anatomy_data.get("subset")
                if product_name is not None:
                    product_value["name"] = product_name

                product_type = anatomy_data.get("family")
                if product_type is not None:
                    product_value["type"] = product_type

                anatomy_data["product"] = product_value

            if "folder" not in anatomy_data:
                folder_value = {}
                folder_name = anatomy_data.get("asset")
                if folder_name is not None:
                    folder_value["name"] = folder_name
                anatomy_data["folder"] = folder_value

            repre_report_items = check_destination_path(
                repre["id"],
                anatomy,
                anatomy_data,
                datetime_data,
                anatomy_name
            )

            if repre_report_items:
                report_items.update(repre_report_items)
                continue

            # Get source repre path
            frame = repre["context"].get("frame")

            if frame:
                repre["context"]["frame"] = len(str(frame)) * "#"

            repre_path = get_representation_path_with_anatomy(repre, anatomy)
            # TODO add backup solution where root of path from component
            # is replaced with root
            args = (
                repre_path,
                repre,
                anatomy,
                anatomy_name,
                anatomy_data,
                format_dict,
                report_items,
                self.log
            )
            if not frame:
                deliver_single_file(*args)
            else:
                deliver_sequence(*args)

        return self.report(report_items)

    def report(self, report_items):
        """Returns dict with final status of delivery (success, fail etc.).

        Args:
            report_items (dict[str, Union[str, list[str]]]: Dict with report
                items to be shown to user.

        Returns:
            dict[str, Any]: Dict with final status of delivery.
        """

        all_items = []

        for msg, items in report_items.items():
            if not items:
                continue

            if all_items:
                all_items.append({"type": "label", "value": "---"})

            all_items.append({
                "type": "label",
                "value": "# {}".format(msg)
            })
            if not isinstance(items, (list, tuple)):
                items = [items]

            all_items.append({
                "type": "label",
                "value": "<p>{}</p>".format(
                    "<br>".join([str(item) for item in items])
                )
            })

        if not all_items:
            return {
                "success": True,
                "message": "Delivery Finished"
            }

        return {
            "items": all_items,
            "title": "Delivery report",
            "success": False
        }

    def _get_repre_names(self, project_name, session, entities):
        """

        Args:
            project_name (str): Project name.
            session (ftrack_api.Session): ftrack session.
            entities (list[ftrack_api.entity.base.Entity]): List of entities.

        Returns:
            list[str]: List of representation names.
        """

        version_ids = self._get_interest_version_ids(
            project_name, session, entities
        )
        if not version_ids:
            return []
        repre_entities = get_representations(
            project_name,
            version_ids=version_ids,
            fields=["name"]
        )
        repre_names = {
            repre_entity["name"]
            for repre_entity in repre_entities
        }
        return list(sorted(repre_names))

    def _get_interest_version_ids(self, project_name, session, entities):
        """

        Args:
            project_name (str): Project name.
            session (ftrack_api.Session): ftrack session.
            entities (list[ftrack_api.entity.base.Entity]): List of entities.

        Returns:
            set[str]: Set of AYON version ids.
        """

        # Extract AssetVersion entities
        asset_versions = self._extract_asset_versions(session, entities)
        # Prepare Asset ids
        asset_ids = {
            asset_version["asset_id"]
            for asset_version in asset_versions
        }
        # Query Asset entities
        assets = session.query((
            "select id, name, context_id from Asset where id in ({})"
        ).format(self.join_query_keys(asset_ids))).all()
        assets_by_id = {
            asset["id"]: asset
            for asset in assets
        }
        parent_ids = set()
        product_names = set()
        version_nums = set()
        for asset_version in asset_versions:
            asset_id = asset_version["asset_id"]
            asset = assets_by_id[asset_id]

            parent_ids.add(asset["context_id"])
            product_names.add(asset["name"])
            version_nums.add(asset_version["version"])

        folders_by_ftrack_id = self._get_folder_entities(
            project_name, session, parent_ids
        )
        product_entities = self._get_product_entities(
            project_name,
            folders_by_ftrack_id,
            product_names,
            asset_versions,
            assets_by_id
        )
        version_entities = self._get_version_entities(
            project_name,
            folders_by_ftrack_id,
            product_entities,
            version_nums,
            asset_versions,
            assets_by_id
        )

        return {version_entity["id"] for version_entity in version_entities}

    def _extract_asset_versions(self, session, entities):
        asset_version_ids = set()
        review_session_ids = set()
        for entity in entities:
            entity_type_low = entity.entity_type.lower()
            if entity_type_low == "assetversion":
                asset_version_ids.add(entity["id"])
            elif entity_type_low == "reviewsession":
                review_session_ids.add(entity["id"])

        for version_id in self._get_asset_version_ids_from_review_sessions(
            session, review_session_ids
        ):
            asset_version_ids.add(version_id)

        asset_versions = session.query((
            "select id, version, asset_id from AssetVersion where id in ({})"
        ).format(self.join_query_keys(asset_version_ids))).all()

        return asset_versions

    def _get_asset_version_ids_from_review_sessions(
        self, session, review_session_ids
    ):
        if not review_session_ids:
            return set()
        review_session_objects = session.query((
            "select version_id from ReviewSessionObject"
            " where review_session_id in ({})"
        ).format(self.join_query_keys(review_session_ids))).all()

        return {
            review_session_object["version_id"]
            for review_session_object in review_session_objects
        }

    def _get_folder_entities(self, project_name, session, parent_ids):
        """

        Args:
            project_name (str): Project name.
            session (ftrack_api.Session): ftrack session.
            parent_ids (set[str]): Set of ftrack ids parents to Asset.

        Returns:
            dict[str, dict[str, Any]]: Folder entities by ftrack id.
        """

        folder_entities = list(get_folders(
            project_name, fields={
                "id",
                "path",
                f"attrib.{FTRACK_ID_ATTRIB}"
            }
        ))

        folders_by_id = {}
        folders_by_path = {}
        folders_by_ftrack_id = {}
        for folder_entity in folder_entities:
            folder_id = folder_entity["id"]
            folder_path = folder_entity["path"]
            ftrack_id = folder_entity["attrib"].get(FTRACK_ID_ATTRIB)

            folders_by_id[folder_id] = folder_entity
            folders_by_path[folder_path] = folder_entity
            if ftrack_id:
                folders_by_ftrack_id[ftrack_id] = folder_entity

        attr_def = session.query((
            "select id from CustomAttributeConfiguration where key is \"{}\""
        ).format(CUST_ATTR_KEY_SERVER_ID)).first()
        if attr_def is None:
            return folders_by_ftrack_id

        ayon_id_values = query_custom_attribute_values(
            session, [attr_def["id"]], parent_ids
        )
        missing_ids = set(parent_ids)
        for item in ayon_id_values:
            if not item["value"]:
                continue
            folder_id = item["value"]
            entity_id = item["entity_id"]
            folder_entity = folders_by_id.get(folder_id)
            if folder_entity:
                folders_by_ftrack_id[entity_id] = folder_entity
                missing_ids.remove(entity_id)

        entity_ids_by_path = {}
        if missing_ids:
            not_found_entities = session.query((
                "select id, link from TypedContext where id in ({})"
            ).format(self.join_query_keys(missing_ids))).all()
            for ftrack_entity in not_found_entities:
                # TODO use 'slugify_name' function
                link_names = [item["name"] for item in ftrack_entity["link"]]
                # Change project name to empty string
                link_names[0] = ""
                entity_path = "/".join(link_names)
                entity_ids_by_path[entity_path] = ftrack_entity["id"]

        for entity_path, ftrack_id in entity_ids_by_path.items():
            folder_entity = folders_by_path.get(entity_path)
            if folder_entity:
                folders_by_ftrack_id[ftrack_id] = folder_entity

        return folders_by_ftrack_id

    def _get_product_entities(
        self,
        project_name,
        folders_by_ftrack_id,
        product_names,
        asset_versions,
        assets_by_id
    ):
        """

        Args:
            project_name (str): Project name.
            folders_by_ftrack_id (dict[str, dict[str, Any]]): Folder entities
                by ftrack id.
            product_names (set[str]): Set of product names.
            asset_versions (list[dict[str, Any]]): ftrack AssetVersion
                entities.
            assets_by_id (dict[str, dict[str, Any]]): ftrack Asset entities
                by id.

        Returns:
            list[dict[str, Any]]: Product entities.
        """

        output = []
        if not folders_by_ftrack_id:
            return output

        folder_ids = {
            folder["id"]
            for folder in folders_by_ftrack_id.values()
        }
        product_entities = list(get_products(
            project_name,
            folder_ids=folder_ids,
            product_names=product_names
        ))
        products_by_folder_id = {}
        for product in product_entities:
            folder_id = product["folderId"]
            product_name = product["name"]
            products_by_name = products_by_folder_id.setdefault(folder_id, {})
            products_by_name[product_name] = product

        for asset_version in asset_versions:
            asset_id = asset_version["asset_id"]
            asset = assets_by_id[asset_id]

            parent_id = asset["context_id"]
            folder = folders_by_ftrack_id.get(parent_id)
            if not folder:
                continue

            products_by_name = products_by_folder_id.get(folder["id"])
            if not products_by_name:
                continue

            product_name = asset["name"]
            product_entity = products_by_name.get(product_name)
            if product_entity:
                output.append(product_entity)
        return output

    def _get_version_entities(
        self,
        project_name,
        folders_by_ftrack_id,
        product_entities,
        version_nums,
        asset_versions,
        assets_by_id
    ):
        """

        Args:
            project_name (str): Project name.
            folders_by_ftrack_id (dict[str, dict[str, Any]]): Folder entities
                by ftrack id.
            product_entities (list[dict[str, Any]]): Product entities.
            version_nums (set[str]): Set of version numbers.
            asset_versions (list[dict[str, Any]]): ftrack AssetVersion
                entities.
            assets_by_id (dict[str, dict[str, Any]]): ftrack Asset entities
                by id.

        Returns:
            list[dict[str, Any]]: Set of AYON version ids.
        """

        product_entities_by_id = {
            product_entity["id"]: product_entity
            for product_entity in product_entities
        }
        version_entities = list(get_versions(
            project_name,
            product_ids=product_entities_by_id.keys(),
            versions=version_nums
        ))
        version_docs_by_parent_id = {}
        for version_entity in version_entities:
            product_id = version_entity["productId"]
            product_entity = product_entities_by_id[product_id]

            folder_id = product_entity["folderId"]
            product_name = product_entity["name"]
            version = version_entity["version"]

            folder_values = version_docs_by_parent_id.setdefault(folder_id, {})
            product_values = folder_values.setdefault(product_name, {})
            product_values[version] = version_entity

        filtered_versions = []
        for asset_version in asset_versions:
            asset_id = asset_version["asset_id"]
            version = asset_version["version"]

            asset = assets_by_id[asset_id]
            parent_id = asset["context_id"]
            product_name = asset["name"]

            folder_entity = folders_by_ftrack_id.get(parent_id)
            if not folder_entity:
                continue

            product_values = version_docs_by_parent_id.get(
                folder_entity["id"]
            )
            if not product_values:
                continue

            version_entities_by_version = product_values.get(product_name)
            if not version_entities_by_version:
                continue

            version_entity = version_entities_by_version.get(version)
            if version_entity:
                filtered_versions.append(version_entity)

        return filtered_versions
