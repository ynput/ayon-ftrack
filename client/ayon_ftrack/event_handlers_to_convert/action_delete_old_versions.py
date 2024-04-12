import os
import collections
import uuid

import clique

from ayon_api import (
    get_folders,
    get_products,
    get_versions,
    get_representations,
)
from ayon_api.operations import OperationsSession

from ayon_core.lib import (
    StringTemplate,
    TemplateUnsolved,
    format_file_size,
)
from ayon_core.pipeline import Anatomy
from ayon_ftrack.common import LocalAction
from ayon_ftrack.lib import get_ftrack_icon_url


class DeleteOldVersions(LocalAction):

    identifier = "delete.old.versions"
    label = "AYON Admin"
    variant = "- Delete old versions"
    description = (
        "Delete files from older publishes so project can be"
        " archived with only lates versions."
    )
    icon = get_ftrack_icon_url("AYONAdmin.svg")

    settings_key = "delete_old_versions"

    inteface_title = "Choose your preferences"
    splitter_item = {"type": "label", "value": "---"}
    sequence_splitter = "__sequence_splitter__"

    def discover(self, session, entities, event):
        """ Validation. """
        is_valid = False
        for entity in entities:
            if entity.entity_type.lower() == "assetversion":
                is_valid = True
                break

        if is_valid:
            is_valid = self.valid_roles(session, entities, event)
        return is_valid

    def interface(self, session, entities, event):
        # TODO Add roots existence validation
        items = []
        values = event["data"].get("values")
        if values:
            versions_count = int(values["last_versions_count"])
            if versions_count >= 1:
                return
            items.append({
                "type": "label",
                "value": (
                    "# You have to keep at least 1 version!"
                )
            })

        items.append({
            "type": "label",
            "value": (
                "<i><b>WARNING:</b> This will remove published files of older"
                " versions from disk so we don't recommend use"
                " this action on \"live\" project.</i>"
            )
        })

        items.append(self.splitter_item)

        # How many versions to keep
        items.append({
            "type": "label",
            "value": "## Choose how many versions you want to keep:"
        })
        items.append({
            "type": "label",
            "value": (
                "<i><b>NOTE:</b> We do recommend to keep 2 versions.</i>"
            )
        })
        items.append({
            "type": "number",
            "name": "last_versions_count",
            "label": "Versions",
            "value": 2
        })

        items.append(self.splitter_item)

        items.append({
            "type": "label",
            "value": (
                "## Remove publish folder even if there"
                " are other than published files:"
            )
        })
        items.append({
            "type": "label",
            "value": (
                "<i><b>WARNING:</b> This may remove more than you want.</i>"
            )
        })
        items.append({
            "type": "boolean",
            "name": "force_delete_publish_folder",
            "label": "Are You sure?",
            "value": False
        })

        items.append(self.splitter_item)

        items.append({
            "type": "label",
            "value": (
                "<i>This will <b>NOT</b> delete any files and only return the "
                "total size of the files.</i>"
            )
        })
        items.append({
            "type": "boolean",
            "name": "only_calculate",
            "label": "Only calculate size of files.",
            "value": False
        })

        return {
            "items": items,
            "title": self.inteface_title
        }

    def launch(self, session, entities, event):
        values = event["data"].get("values")
        if not values:
            return

        versions_count = int(values["last_versions_count"])
        force_to_remove = values["force_delete_publish_folder"]
        only_calculate = values["only_calculate"]

        _val1 = "OFF"
        if force_to_remove:
            _val1 = "ON"

        _val3 = "s"
        if versions_count == 1:
            _val3 = ""

        self.log.debug((
            "Process started. Force to delete publish folder is set to [{0}]"
            " and will keep {1} latest version{2}."
        ).format(_val1, versions_count, _val3))

        project = None
        folder_paths = []
        asset_versions_by_parent_id = collections.defaultdict(list)
        product_names_by_folder_path = collections.defaultdict(list)

        ftrack_assets_by_name = {}
        for entity in entities:
            ftrack_asset = entity["asset"]

            parent_ent = ftrack_asset["parent"]
            parent_ftrack_id = parent_ent["id"]

            path_items = [item["name"] for item in entity["link"]]
            path_items[0] = ""
            folder_path = "/".join(path_items)

            if folder_path not in folder_paths:
                folder_paths.append(folder_path)

            # Group asset versions by parent entity
            asset_versions_by_parent_id[parent_ftrack_id].append(entity)

            # Get project
            if project is None:
                project = parent_ent["project"]

            # Collect product names per asset
            product_name = ftrack_asset["name"]
            product_names_by_folder_path[folder_path].append(product_name)

            if product_name not in ftrack_assets_by_name:
                ftrack_assets_by_name[product_name] = ftrack_asset

        # Set Mongo collection
        project_name = project["full_name"]
        anatomy = Anatomy(project_name)
        self.log.debug("Project is set to {}".format(project_name))

        # Fetch folders
        folder_path_by_id = {
            folder_entity["id"]: folder_entity["path"]
            for folder_entity in get_folders(
                project_name, folder_paths=folder_paths
            )
        }
        folder_ids = set(folder_path_by_id.keys())

        self.log.debug("Collected assets ({})".format(len(folder_ids)))

        # Get product entities
        product_entities_by_id = {
            product_entity["id"]: product_entity
            for product_entity in get_products(
                project_name, folder_ids=folder_ids
            )
        }
        # Filter products by available product names
        for product_entity in product_entities_by_id.values():
            folder_id = product_entity["folderId"]
            folder_path = folder_path_by_id[folder_id]

            available_products = product_names_by_folder_path[folder_path]
            if product_entity["name"] not in available_products:
                product_id = product_entity["id"]
                product_entities_by_id.pop(product_id)

        product_ids = set(product_entities_by_id.keys())

        self.log.debug("Collected products ({})".format(len(product_ids)))

        # Get Versions
        version_entities_by_id = {
            version_entity["id"]: version_entity
            for version_entity in get_versions(
                project_name,
                product_ids=product_ids,
                hero=False,
                active=None
            )
        }

        # Store all versions by product id even inactive entities
        versions_by_parent = collections.defaultdict(list)
        for version_entity in version_entities_by_id.values():
            product_id = version_entity["productId"]
            versions_by_parent[product_id].append(version_entity)

        def sort_func(ent):
            return ent["version"]

        # Filter latest versions
        for parent_id, version_entities in versions_by_parent.items():
            for idx, version_entity in enumerate(
                sorted(version_entities, key=sort_func, reverse=True)
            ):
                if idx >= versions_count:
                    break
                version_entities_by_id.pop(version_entity["id"])

        self.log.debug(
            "Collected versions ({})".format(len(version_entities_by_id))
        )

        # Update versions_by_parent without filtered versions
        versions_by_parent = collections.defaultdict(list)
        for version_entity in version_entities_by_id.values():
            # Filter already deactivated versions
            if not version_entity["active"]:
                continue
            product_id = version_entity["productId"]
            versions_by_parent[product_id].append(version_entity)

        version_ids = set(version_entities_by_id.keys())

        self.log.debug(
            "Filtered versions to delete ({})".format(len(version_ids))
        )

        if not version_ids:
            msg = "Skipping processing. Nothing to delete."
            self.log.debug(msg)
            return {
                "success": True,
                "message": msg
            }

        repre_entities = list(
            get_representations(project_name, version_ids=version_ids)
        )

        self.log.debug(
            "Collected representations to remove ({})".format(
                len(repre_entities)
            )
        )

        dir_paths = {}
        file_paths_by_dir = collections.defaultdict(list)
        for repre_entity in repre_entities:
            file_path, seq_path = self.path_from_represenation(
                repre_entity, anatomy
            )
            if file_path is None:
                self.log.warning((
                    "Could not format path for represenation \"{}\""
                ).format(str(repre_entity)))
                continue

            dir_path = os.path.dirname(file_path)
            dir_id = None
            for _dir_id, _dir_path in dir_paths.items():
                if _dir_path == dir_path:
                    dir_id = _dir_id
                    break

            if dir_id is None:
                dir_id = uuid.uuid4()
                dir_paths[dir_id] = dir_path

            file_paths_by_dir[dir_id].append([file_path, seq_path])

        dir_ids_to_pop = []
        for dir_id, dir_path in dir_paths.items():
            if os.path.exists(dir_path):
                continue

            dir_ids_to_pop.append(dir_id)

        # Pop dirs from both dictionaries
        for dir_id in dir_ids_to_pop:
            dir_paths.pop(dir_id)
            paths = file_paths_by_dir.pop(dir_id)
            # TODO report of missing directories?
            paths_msg = ", ".join([
                "'{}'".format(path[0].replace("\\", "/")) for path in paths
            ])
            self.log.warning((
                "Folder does not exist. Deleting it's files skipped: {}"
            ).format(paths_msg))

        # Size of files.
        if only_calculate:
            if force_to_remove:
                size = self.delete_whole_dir_paths(
                    dir_paths.values(), delete=False
                )
            else:
                size = self.delete_only_repre_files(
                    dir_paths, file_paths_by_dir, delete=False
                )

            msg = "Total size of files: {}".format(format_file_size(size))

            self.log.warning(msg)

            return {"success": True, "message": msg}

        if force_to_remove:
            size = self.delete_whole_dir_paths(dir_paths.values())
        else:
            size = self.delete_only_repre_files(dir_paths, file_paths_by_dir)

        op_session = OperationsSession()
        for version_entity in version_entities_by_id.values():
            op_session.update_entity(
                project_name,
                "version",
                version_entity["id"],
                {"active": False}
            )

        op_session.commit()

        # Set attribute `is_published` to `False` on ftrack AssetVersions
        for product_id, _versions in versions_by_parent.items():
            product_entity = product_entities_by_id.get(product_id)
            if product_entity is None:
                self.log.warning(
                    "Product with ID `{}` was not found.".format(str(product_id))
                )
                continue

            product_name = product_entity["name"]

            ftrack_asset = ftrack_assets_by_name.get(product_name)
            if not ftrack_asset:
                self.log.warning((
                    "Could not find Ftrack asset with name `{}`"
                ).format(product_name))
                continue

            version_numbers = [int(ver["name"]) for ver in _versions]
            for version in ftrack_asset["versions"]:
                if int(version["version"]) in version_numbers:
                    version["is_published"] = False

        try:
            session.commit()

        except Exception:
            msg = (
                "Could not set `is_published` attribute to `False`"
                " for selected AssetVersions."
            )
            self.log.warning(msg, exc_info=True)

            return {
                "success": False,
                "message": msg
            }

        msg = "Total size of files deleted: {}".format(format_file_size(size))

        self.log.warning(msg)

        return {"success": True, "message": msg}

    def delete_whole_dir_paths(self, dir_paths, delete=True):
        size = 0

        for dir_path in dir_paths:
            # Delete all files and fodlers in dir path
            for root, dirs, files in os.walk(dir_path, topdown=False):
                for name in files:
                    file_path = os.path.join(root, name)
                    size += os.path.getsize(file_path)
                    if delete:
                        os.remove(file_path)
                        self.log.debug("Removed file: {}".format(file_path))

                for name in dirs:
                    if delete:
                        os.rmdir(os.path.join(root, name))

            if not delete:
                continue

            # Delete even the folder and it's parents folders if they are empty
            while True:
                if not os.path.exists(dir_path):
                    dir_path = os.path.dirname(dir_path)
                    continue

                if len(os.listdir(dir_path)) != 0:
                    break

                os.rmdir(os.path.join(dir_path))

        return size

    def delete_only_repre_files(self, dir_paths, file_paths, delete=True):
        size = 0

        for dir_id, dir_path in dir_paths.items():
            dir_files = os.listdir(dir_path)
            collections, remainders = clique.assemble(dir_files)
            for file_path, seq_path in file_paths[dir_id]:
                file_path_base = os.path.split(file_path)[1]
                # Just remove file if `frame` key was not in context or
                # filled path is in remainders (single file sequence)
                if not seq_path or file_path_base in remainders:
                    if not os.path.exists(file_path):
                        self.log.warning(
                            "File was not found: {}".format(file_path)
                        )
                        continue

                    size += os.path.getsize(file_path)

                    if delete:
                        os.remove(file_path)
                        self.log.debug("Removed file: {}".format(file_path))

                    if file_path_base in remainders:
                        remainders.remove(file_path_base)
                    continue

                seq_path_base = os.path.split(seq_path)[1]
                head, tail = seq_path_base.split(self.sequence_splitter)

                final_col = None
                for collection in collections:
                    if head != collection.head or tail != collection.tail:
                        continue
                    final_col = collection
                    break

                if final_col is not None:
                    # Fill full path to head
                    final_col.head = os.path.join(dir_path, final_col.head)
                    for _file_path in final_col:
                        if os.path.exists(_file_path):

                            size += os.path.getsize(_file_path)

                            if delete:
                                os.remove(_file_path)
                                self.log.debug(
                                    "Removed file: {}".format(_file_path)
                                )

                    _seq_path = final_col.format("{head}{padding}{tail}")
                    self.log.debug("Removed files: {}".format(_seq_path))
                    collections.remove(final_col)

                elif os.path.exists(file_path):
                    size += os.path.getsize(file_path)

                    if delete:
                        os.remove(file_path)
                        self.log.debug("Removed file: {}".format(file_path))
                else:
                    self.log.warning(
                        "File was not found: {}".format(file_path)
                    )

        # Delete as much as possible parent folders
        if not delete:
            return size

        for dir_path in dir_paths.values():
            while True:
                if not os.path.exists(dir_path):
                    dir_path = os.path.dirname(dir_path)
                    continue

                if len(os.listdir(dir_path)) != 0:
                    break

                self.log.debug("Removed folder: {}".format(dir_path))
                os.rmdir(dir_path)

        return size

    def path_from_represenation(self, representation, anatomy):
        try:
            template = representation["data"]["template"]

        except KeyError:
            return (None, None)

        sequence_path = None
        try:
            context = representation["context"]
            context["root"] = anatomy.roots
            path = StringTemplate.format_strict_template(template, context)
            if "frame" in context:
                context["frame"] = self.sequence_splitter
                sequence_path = os.path.normpath(
                    StringTemplate.format_strict_template(
                        template, context
                    )
                )

        except (KeyError, TemplateUnsolved):
            # Template references unavailable data
            return (None, None)

        return (os.path.normpath(path), sequence_path)


def register(session):
    '''Register plugin. Called when used as an plugin.'''

    DeleteOldVersions(session).register()
