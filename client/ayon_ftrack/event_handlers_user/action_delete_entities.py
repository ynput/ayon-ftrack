import json
import copy
import collections

from ayon_api import (
    get_project,
    get_folders,
    get_products,
    send_batch_operations,
)

from ayon_ftrack.common import create_chunks, LocalAction
from ayon_ftrack.lib import get_ftrack_icon_url


class AyonData:
    """Helper structure to hold AYON data.

    Args:
        project_name (str): Project name.
        folders_by_id (dict[str, dict[str, Any]]): Folders by id.
        folders_by_path (dict[str, dict[str, Any]]): Folders by path.
        folders_by_parent_id (dict[str, list[dict[str, Any]]]): Folders by
            parent id.
        not_found_paths (set[str]): Folder paths not found in AYON.
        selected_folder_ids (set[str]): Selected folder ids.
        folder_ids_to_delete (set[str]): Folder ids to delete. Contains all
            folder children of selected folder ids.
    """

    def __init__(
        self,
        project_name,
        folders_by_id,
        folders_by_path,
        folders_by_parent_id,
        not_found_paths,
        selected_folder_ids,
        folder_ids_to_delete,
    ):
        self.project_name = project_name
        self.folders_by_id = folders_by_id
        self.folders_by_path = folders_by_path
        self.folders_by_parent_id = folders_by_parent_id
        self.not_found_paths = not_found_paths
        self.selected_folder_ids = selected_folder_ids
        self.folder_ids_to_delete = folder_ids_to_delete


class DeleteEntitiesAction(LocalAction):
    identifier = "delete.ayon.entities"
    label = "Delete Folders/Products"
    description = (
        "Remove entities from AYON and from ftrack with all children"
    )
    icon = get_ftrack_icon_url("DeleteAsset.svg")

    settings_key = "delete_ayon_entities"

    def discover(self, session, entities, event):
        """

        Args:
            session (ftrack_api.Session): Ftrack session processing event.
            entities (list[ftrack_api.entity.base.Entity]): List of entities
                selected in Ftrack.
            event (ftrack_api.event.base.Event): Event data.

        Returns:
            bool: True if action is valid for given selection.
        """

        task_ids = set()
        for ent_info in event["data"]["selection"]:
            if ent_info.get("entityType") == "task":
                task_ids.add(ent_info["entityId"])

        is_valid = False
        for entity in entities:
            if (
                entity["id"] in task_ids
                and entity.entity_type.lower() != "task"
            ):
                is_valid = True
                break

        if not is_valid:
            return False
        return self.valid_roles(session, entities, event)

    def launch(self, session, entities, event):
        values = event["data"].get("values")
        if not values:
            return self._first_interface(session, entities, event)

        # Entity type interface should be showed to select which entities
        #   should be deleted
        if not self._waits_for_confirmation(event):
            self.show_message(event, "Preparing data...", True)
            project_selected, ftrack_ids = self._filter_selection_from_event(
                entities, event
            )
            if project_selected:
                msg = (
                    "It is not possible to use this action on project entity."
                )
                self.show_message(event, msg, True)

            if values.get("entity_type") == "folders":
                return self._interface_folders(
                    session, entities, event, ftrack_ids
                )
            return self._interface_products(
                session, entities, event, ftrack_ids
            )

        # Confirmation fails (misspelled 'delete' etc.)
        if not self._is_confirmed(event):
            return self._prepare_delete_interface(event)
        # User confirmed deletion
        project_selected, ftrack_ids = self._filter_selection_from_event(
            entities, event
        )
        if values.get("entity_type") == "folders":
            return self._delete_folders(session, entities, event, ftrack_ids)
        return self._delete_products(session, entities, event, ftrack_ids)

    def _event_values_to_hidden(self, values):
        """Take event values and convert them to hidden items.

        Args:
            values (dict[str, Any]): Ftrack event values.

        Returns:
            list[dict[str, Any]]: List of hidden items.
        """

        return [
            {
                "type": "hidden",
                "name": key,
                "value": value
            }
            for key, value in values.items()
        ]

    def _waits_for_confirmation(self, event):
        """Check if action waits for confirmation.

        Args:
            event (ftrack_api.event.base.Event): Event data.

        Returns:
            bool: True if action waits for confirmation.
        """

        values = event["data"].get("values")
        if values:
            return "delete_confirm_value" in values
        return False

    def _is_confirmed(self, event):
        values = event["data"].get("values")
        if values is None:
            return False

        confirm_value = values.get("delete_confirm_value") or ""
        expected_value = values.get("delete_confirm_expected")
        return confirm_value.lower() == expected_value

    def _prepare_delete_interface(self, event, post_items=None):
        # Create copy of values to be able to create hidden items
        values = copy.deepcopy(event["data"]["values"])
        attempt = values.pop("delete_attempt_count", 0) + 1

        # Make sure post items is iterable
        if post_items is None:
            post_items = json.loads(values.pop("post_items"))

        post_item_names = {
            post_item.get("name")
            for post_item in post_items
        }
        post_item_names.discard(None)

        for key in {
            "delete_confirm_value",
            "delete_confirm_expected",
        } | post_item_names:
            values.pop(key, None)

        if values["action_type"] == "delete":
            submit_button_label = "Delete"
            expected_value = "delete"
        else:
            submit_button_label = "Archive"
            expected_value = "archive"

        items = [
            {
                "type": "label",
                "value": f"# Please enter '{expected_value}' to confirm #"
            }
        ]
        if attempt > 3:
            additional_info = (
                "Read the instructions carefully please."
                f" You've failed {attempt - 1}"
                f" times to enter '{expected_value}'."
            )
            if attempt > 4:
                additional_info += "<br/>(Can I hear the grass grow?)"
            items.append({
                "type": "label",
                "value": additional_info
            })

        items.extend([
            {
                "name": "delete_confirm_value",
                "type": "text",
                "value": "",
                "empty_text": f"Type {expected_value} here...",
            },
            {
                "type": "hidden",
                "name": "delete_confirm_expected",
                "value": expected_value,
            },
            {
                "type": "hidden",
                "name": "delete_attempt_count",
                "value": attempt,
            },
            {
                "type": "hidden",
                "name": "post_items",
                "value": json.dumps(post_items),
            }
        ])
        items.extend(self._event_values_to_hidden(values))
        items.extend(post_items)

        return {
            "items": items,
            "title": f"Confirm {expected_value} action",
            "submit_button_label": submit_button_label,
        }

    def _filter_selection_from_event(self, entities, event):
        """

        Args:
            entities (list[ftrack_api.entity.base.Entity]): List of entities
                selected in Ftrack.
            event (ftrack_api.event.base.Event): Event data.

        Returns:
            tuple[bool, set[str]]: Project is selected and set of selected
                ftrack ids.
        """

        project_in_selection = False
        ftrack_ids = set()
        selection = event["data"].get("selection")
        if not selection:
            return project_in_selection, ftrack_ids

        for entity in selection:
            entity_type = (entity.get("entityType") or "").lower()
            if entity_type == "show":
                project_in_selection = True

            elif entity_type == "task":
                ftrack_id = entity.get("entityId")
                if ftrack_id:
                    ftrack_ids.add(ftrack_id)

        # Filter event even more (skip task entities)
        # - task entities are not relevant for AYON delete
        for entity in entities:
            ftrack_id = entity["id"]
            if (
                ftrack_id in ftrack_ids
                and entity.entity_type.lower() == "task"
            ):
                ftrack_ids.discard(ftrack_id)

        return project_in_selection, ftrack_ids

    def _get_ayon_data_from_selection(self, session, entities, ftrack_ids):
        """Get folders from selection.

        Args:
            session (ftrack_api.Session): Ftrack session processing event.
            entities (list[ftrack_api.entity.base.Entity]): List of entities
                selected in Ftrack.
            ftrack_ids (set[str]): Selected ftrack ids.

        Returns:
            AyonData: Folders data.
        """

        filtered_entities = [
            entity
            for entity in entities
            if entity["id"] in ftrack_ids
        ]

        project = self.get_project_from_entity(entities[0], session)
        project_name = project["full_name"]

        folder_entities = list(get_folders(
            project_name,
            fields={"id", "path", "parentId"}
        ))
        folders_by_id = {}
        folders_by_path = {}
        folders_by_parent_id = collections.defaultdict(list)
        for folder_entity in folder_entities:
            folders_by_id[folder_entity["id"]] = folder_entity
            folders_by_path[folder_entity["path"]] = folder_entity
            folders_by_parent_id[folder_entity["parentId"]].append(
                folder_entity
            )

        not_found_paths = set()
        selected_folder_ids = set()
        for entity in filtered_entities:
            # TODO use slugify name
            ent_path_items = [ent["name"] for ent in entity["link"]]
            # Replace project name with empty string
            ent_path_items[0] = ""
            path = "/".join(ent_path_items)

            folder_entity = folders_by_path.get(path)
            if folder_entity:
                selected_folder_ids.add(folder_entity["id"])
            else:
                not_found_paths.add(path)

        folder_ids_queue = collections.deque(selected_folder_ids)
        folder_ids_to_delete = set()
        while folder_ids_queue:
            folder_id = folder_ids_queue.popleft()
            folder_ids_to_delete.add(folder_id)
            for folder_entity in folders_by_parent_id[folder_id]:
                folder_ids_queue.append(folder_entity["id"])

        return AyonData(
            project_name,
            folders_by_id,
            folders_by_path,
            folders_by_parent_id,
            not_found_paths,
            selected_folder_ids,
            folder_ids_to_delete,
        )

    def _interface_folders(self, session, entities, event, ftrack_ids):
        """Interface for folder entities.

        Args:
            session (ftrack_api.Session): Ftrack session processing event.
            entities (list[ftrack_api.entity.base.Entity]): List of entities
                selected in Ftrack.
            event (ftrack_api.event.base.Event): Event data.
            ftrack_ids (set[str]): Selected ftrack ids.

        Returns:
            dict[str, Any]: Interface data.
        """

        ayon_data = self._get_ayon_data_from_selection(
            session, entities, ftrack_ids
        )
        # TODO prepare main label
        count = len(ayon_data.folder_ids_to_delete)
        main_label = (
            f"You're going to delete {count} folders with all children from"
            f" ftrack and AYON. <b>This action cannot be undone</b>."
        )
        return self._prepare_delete_interface(
            event,
            [
                {"type": "label", "value": "---"},
                {"type": "label", "value": main_label},
            ]
        )

    def _interface_products_selection(
        self, session, entities, event, ftrack_ids
    ):
        """

        Args:
            session (ftrack_api.Session): Ftrack session processing event.
            entities (list[ftrack_api.entity.base.Entity]): List of entities
                selected in Ftrack.
            event (ftrack_api.event.base.Event): Event data.
            ftrack_ids (set[str]): Selected ftrack ids.

        Returns:
            Union[dict[str, Any], None]: Interface data.
        """

        ayon_data = self._get_ayon_data_from_selection(
            session, entities, ftrack_ids
        )
        values = event["data"]["values"]
        if values["entity_type"] == "products_all":
            folder_ids = set(ayon_data.folder_ids_to_delete)
        else:
            folder_ids = set(ayon_data.selected_folder_ids)
        products = get_products(
            ayon_data.project_name,
            folder_ids=folder_ids,
            fields={"name"}
        )
        counts_by_product_name = {}
        for product in products:
            product_name = product["name"]
            counts_by_product_name.setdefault(product_name, 0)
            counts_by_product_name[product_name] += 1

        if not counts_by_product_name:
            self.show_interface(
                items=[{
                    "type": "label",
                    "value": "No products found for selected entities."
                }],
                title="No products to delete found",
                submit_btn_label="Close",
                event=event,
            )
            return None

        product_names = list(sorted(counts_by_product_name.keys()))
        product_name_items = [
            {"label": product_name, "value": product_name}
            for product_name in product_names
        ]
        items = [
            {
                "type": "label",
                "value": "## Products to delete ##",
            },
            {
                "type": "label",
                "value": "Uncheck product names you want to keep.",
            },
            {
                "type": "hidden",
                "name": "counts_by_product_name",
                "value": json.dumps(counts_by_product_name),
            },
            {
                "type": "enumerator",
                "multi_select": True,
                "name": "product_names",
                "data": product_name_items,
                "value": product_names
            },
        ]
        items.extend(self._event_values_to_hidden(values))
        return {
            "title": "Choose product names to delete",
            "submit_button_label": "Confirm action",
            "items": items,
        }

    def _interface_products(self, session, entities, event, ftrack_ids):
        """Interface for product entities.

        Args:
            session (ftrack_api.Session): Ftrack session processing event.
            entities (list[ftrack_api.entity.base.Entity]): List of entities
                selected in Ftrack.
            event (ftrack_api.event.base.Event): Event data.
        """

        values = event["data"]["values"]
        # Ask which product names should be removed first
        if "counts_by_product_name" not in values:
            return self._interface_products_selection(
                session, entities, event, ftrack_ids
            )

        counts_by_product_name = json.loads(values.pop(
            "counts_by_product_name"
        ))
        product_names_to_delete = values.pop("product_names")
        if not product_names_to_delete:
            return {
                "success": True,
                "message": "Nothing was selected to delete"
            }

        all_count = 0
        for product_name in product_names_to_delete:
            all_count += counts_by_product_name[product_name]

        # TODO prepare main label
        main_label = (
            f"You're going to delete {all_count} products with all versions"
            " from ftrack and AYON. <b>This action cannot be undone</b>."
        )
        return self._prepare_delete_interface(
            event,
            [
                {"type": "label", "value": "---"},
                {"type": "label", "value": main_label},
                {
                    "type": "hidden",
                    "name": "product_names",
                    "value": json.dumps(product_names_to_delete),
                },
            ]
        )

    def _first_interface(self, session, entities, event):
        """First interface asks for action type and entity type.

        Action type is to choose if entities in AYON should be deactived
            or deleted.
        Entity type is to choose if user wants to work with folders
            or products.

        Args:
            session (ftrack_api.Session): Ftrack session processing event.
            entities (list[ftrack_api.entity.base.Entity]): List of entities
                selected in Ftrack.
            event (ftrack_api.event.base.Event): Event data.

        Returns:
            dict[str, Any]: Interface data.
        """

        # Check if project exists in AYON
        project = self.get_project_from_entity(entities[0], session)
        project_name = project["full_name"]
        if not get_project(project_name):
            return {
                "success": False,
                "message": f"Project '{project_name}' not found in AYON."
            }

        # Validate selection
        project_selected, ftrack_ids = self._filter_selection_from_event(
            entities, event
        )
        if not ftrack_ids:
            return {
                "success": False,
                "message": "Invalid selection for this action."
            }

        return {
            "title": "Choose action",
            "submit_button_label": "Confirm action",
            "items": [
                {
                    "type": "label",
                    "value": (
                        "This action will delete entities from Ftrack"
                        " and archive or delete them from AYON."
                        "<br/><br/>NOTE: Does not remove files on disk."
                    )
                },
                {"type": "label", "value": "---"},
                {
                    "type": "enumerator",
                    "name": "entity_type",
                    "label": "Entity type:",
                    "value": "folders",
                    "data": [
                        {
                            "label": "Folders",
                            "value": "folders"
                        },
                        {
                            "label": "Products (Selected folders only)",
                            "value": "products_selection"
                        },
                        {
                            "label": "Products (Selected + children folders)",
                            "value": "products_all"
                        },
                    ]
                },
                {
                    "type": "enumerator",
                    "name": "action_type",
                    "label": "Action in AYON:",
                    "value": "archive",
                    "data": [
                        {
                            "label": "Archive",
                            "value": "archive"
                        },
                        {
                            "label": "Delete",
                            "value": "delete"
                        }
                    ]
                },
                {"type": "label", "value": "---"},
                {
                    "type": "label",
                    "value": (
                        "- Option <b>'Archive in AYON'</b> only hide"
                        " selected entities in AYON UI without actually"
                        " deleting them."
                    )
                },
                {
                    "type": "label",
                    "value": (
                        "- Option <b>'Delete in AYON'</b> will delete"
                        " selected entities from AYON with all children."
                        "<br/><b>WARNING: There is no way back.</b>"
                    )
                },
            ],
        }

    def _query_ftrack_entities(self, session, ftrack_ids, fields=None):
        """Query ftrack hierarchy entities with all children.

        Args:
            session (ftrack_api.Session): Ftrack session processing event.
            ftrack_ids (set[str]): Selected ftrack ids.
            fields (Optional[Iterable[str]]): Fields to query.

        Returns:
            list[ftrack_api.entity.base.Entity]: List of ftrack entities.
        """

        if not fields:
            fields = {"id", "parent_id", "object_type_id"}

        joined_fields = ", ".join(fields)

        joined_ids = self.join_query_keys(ftrack_ids)
        all_entities = session.query(
            f"select {joined_fields} from TypedContext"
            f" where id in ({joined_ids})"
        ).all()
        ftrack_ids_queue = collections.deque()
        ftrack_ids_queue.append(ftrack_ids)
        while ftrack_ids_queue:
            ftrack_ids = ftrack_ids_queue.popleft()
            if not ftrack_ids:
                continue

            joined_ids = self.join_query_keys(ftrack_ids)
            entities = session.query(
                "select id, parent_id, object_type_id from TypedContext"
                f" where parent_id in ({joined_ids})"
            ).all()
            all_entities.extend(entities)
            new_ftrack_ids = {
                entity["id"]
                for entity in entities
                if entity.entity_type.lower() != "task"
            }
            ftrack_ids_queue.append(new_ftrack_ids)
        return all_entities

    def _archive_folders_in_ayon(self, ayon_data):
        """Archive folders in AYON.

        Args:
            ayon_data (AyonData): Folders data.
        """

        operations = [
            {
                "type": "update",
                "entityType": "folder",
                "entityId": folder_id,
                "data": {
                    "active": False
                }
            }
            for folder_id in ayon_data.selected_folder_ids
        ]

        self.log.debug("Archiving ({}) folders:\n{}".format(
            len(ayon_data.selected_folder_ids),
            ", ".join(ayon_data.selected_folder_ids),
        ))
        send_batch_operations(ayon_data.project_name, operations)

    def _delete_folders_in_ayon(self, ayon_data):
        """Delete folders in AYON.

        Args:
            ayon_data (AyonData): Folders data.
        """

        project_name = ayon_data.project_name
        # First find all products and delete them.
        #   Folders cannot be deleted if they contain products.
        product_ids = {
            product["id"]
            for product in get_products(
                project_name,
                folder_ids=ayon_data.folder_ids_to_delete,
                fields={"id"},
            )
        }
        self.log.debug("Deleting {} products".format(len(product_ids)))
        for chunk_ids in create_chunks(product_ids):
            send_batch_operations(
                project_name,
                [
                    {
                        "type": "delete",
                        "entityType": "product",
                        "entityId": product_id,
                    }
                    for product_id in chunk_ids
                ]
            )

        # Delete folders in correct order from bottom to top.
        #   This is just to avoid errors AYON.
        folder_ids_by_parent_id = collections.defaultdict(set)
        parents_queue = collections.deque()
        for folder_id in ayon_data.folder_ids_to_delete:
            parents_queue.append(folder_id)
            folder_entity = ayon_data.folders_by_id[folder_id]
            folder_ids_by_parent_id[folder_entity["parentId"]].add(folder_id)

        sorted_folder_ids = []
        while parents_queue:
            folder_id = parents_queue.popleft()
            if folder_ids_by_parent_id[folder_id]:
                parents_queue.append(folder_id)
                continue

            sorted_folder_ids.append(folder_id)
            folder_entity = ayon_data.folders_by_id[folder_id]
            parent_id = folder_entity["parentId"]
            if parent_id in folder_ids_by_parent_id:
                folder_ids_by_parent_id[parent_id].discard(folder_id)

        self.log.debug("Deleting {} folders".format(len(sorted_folder_ids)))
        for chunk_ids in create_chunks(sorted_folder_ids):
            send_batch_operations(
                project_name,
                [
                    {
                        "type": "delete",
                        "entityType": "folder",
                        "entityId": folder_id,
                    }
                    for folder_id in chunk_ids
                ]
            )

    def _delete_folders(self, session, entities, event, ftrack_ids):
        """Delete folders in AYON and ftrack.

        Args:
            session (ftrack_api.Session): Ftrack session processing event.
            entities (list[ftrack_api.entity.base.Entity]): List of entities
                selected in Ftrack.
            event (ftrack_api.event.base.Event): Event data.
            ftrack_ids (set[str]): Selected ftrack ids.
        """

        values = event["data"]["values"]
        ayon_data = self._get_ayon_data_from_selection(
            session, entities, ftrack_ids
        )
        if values["action_type"] == "archive":
            message = "Archiving folders finished."
            self._archive_folders_in_ayon(ayon_data)
        else:
            message = "Delete folders finished."
            self._delete_folders_in_ayon(ayon_data)

        entities = self._query_ftrack_entities(session, ftrack_ids)
        task_entities = []
        entities_by_id = {}
        for entity in entities:
            if entity.entity_type.lower() == "task":
                task_entities.append(entity)
            else:
                entities_by_id[entity["id"]] = entity

        for task_entity in task_entities:
            session.delete(task_entity)

        entity_ids_by_parent_id = collections.defaultdict(set)
        parents_queue = collections.deque()
        for entity_id, entity in entities_by_id.items():
            parents_queue.append(entity_id)
            entity_ids_by_parent_id[entity["parent_id"]].add(entity_id)

        while parents_queue:
            entity_id = parents_queue.popleft()
            entity = entities_by_id[entity_id]
            if entity_ids_by_parent_id[entity_id]:
                parents_queue.append(entity_id)
                continue
            parent_id = entity["parent_id"]

            session.delete(entity)
            session.commit()

            if parent_id not in entity_ids_by_parent_id:
                continue
            entity_ids_by_parent_id[parent_id].discard(entity_id)

        return {
            "success": True,
            "message": message
        }

    def _handle_products_in_ayon(
        self, ayon_data, product_names, selection_only, archive
    ):
        """Archive products in AYON.

        Args:
            ayon_data (AyonData): Folders data.
            product_names (list[str]): List of product names to archive.
            selection_only (bool): True if only selected folders should be
                used.
            archive (bool): True if products should be archived.
        """

        if selection_only:
            folder_ids = ayon_data.selected_folder_ids
        else:
            folder_ids = ayon_data.folder_ids_to_delete
        product_ids = {
            product["id"]
            for product in get_products(
                ayon_data.project_name,
                product_names=product_names,
                folder_ids=folder_ids,
                fields={"id"},
            )
        }
        if archive:
            base_operation = {
                "type": "update",
                "entityType": "product",
                "data": {"active": False}
            }
        else:
            base_operation = {
                "type": "delete",
                "entityType": "product",
            }

        for chunk_ids in create_chunks(product_ids):
            operations = []
            for product_id in chunk_ids:
                operation = copy.deepcopy(base_operation)
                operation["entityId"] = product_id
                operations.append(operation)

            send_batch_operations(ayon_data.project_name, operations)

    def _delete_products(self, session, entities, event, ftrack_ids):
        """Delete or archive products in AYON and ftrack.

        Args:
            session (ftrack_api.Session): Ftrack session processing event.
            entities (list[ftrack_api.entity.base.Entity]): List of entities
                selected in Ftrack.
            event (ftrack_api.event.base.Event): Event data.
            ftrack_ids (set[str]): Selected ftrack ids.
        """

        values = event["data"]["values"]
        product_names = json.loads(values["product_names"])
        ayon_data = self._get_ayon_data_from_selection(
            session, entities, ftrack_ids
        )
        selection_only = values["entity_type"] == "products_selection"
        archive = values["action_type"] == "archive"

        if archive:
            message = "Archiving products finished."
        else:
            message = "Delete products finished."

        self._handle_products_in_ayon(
            ayon_data, product_names, selection_only, archive
        )

        ftrack_entities = self._query_ftrack_entities(
            session, ftrack_ids, {"id"}
        )
        all_ftrack_ids = {
            entity["id"]
            for entity in ftrack_entities
        }
        # Delete assets in ftrack - that should automatically delete all
        #   their asset versions
        for chunk in create_chunks(all_ftrack_ids):
            joined_ids = self.join_query_keys(chunk)
            for asset in session.query(
                "select id, name from Asset"
                f" where context_id in ({joined_ids})"
            ).all():
                # NOTE: Asset name may also contain representation name
                #   '{product[name]}_{representation}', not sure if we should
                #   try to resolve that here?
                if asset["name"] in product_names:
                    session.delete(asset)
            session.commit()

        return {
            "success": True,
            "message": message
        }


def register(session):
    """

    Args:
        session (ftrack_api.Session): Ftrack session.
    """

    DeleteEntitiesAction(session).register()
