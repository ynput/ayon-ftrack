import collections
import ftrack_api

from ayon_ftrack.common import (
    LocalAction,
    create_chunks,
    query_custom_attribute_values,
)
from ayon_ftrack.lib import get_ftrack_icon_url


class CleanHierarchicalAttrsAction(LocalAction):
    identifier = "ayon.clean.hierarchical.attr"
    label = "AYON Admin"
    variant = "- Clean hierarchical custom attributes"
    description = "Unset empty hierarchical attribute values."
    icon = get_ftrack_icon_url("AYONAdmin.svg")

    settings_key = "clean_hierarchical_attr"

    def discover(self, session, entities, event):
        """Show only on project entity."""
        if (
            len(entities) != 1
            or entities[0].entity_type.lower() != "project"
        ):
            return False

        return self.valid_roles(session, entities, event)

    def launch(self, session, entities, event):
        project_id = entities[0]["id"]

        user_message = "This may take some time"
        self.show_message(event, user_message, result=True)
        self.log.debug("Preparing entities for cleanup.")

        all_entities = session.query(
            "select id from TypedContext"
            f" where project_id is \"{project_id}\""
        ).all()

        entity_ids = {
            entity["id"]
            for entity in all_entities
            if entity.entity_type.lower() != "task"
        }
        self.log.debug(
            f"Collected {len(entity_ids)} entities to process."
        )

        all_attr_confs = session.query(
            "select id, key, is_hierarchical"
            " from CustomAttributeConfiguration"
        ).all()
        hier_attr_conf_by_id = {
            attr_conf["id"]: attr_conf
            for attr_conf in all_attr_confs
            if attr_conf["is_hierarchical"]
        }
        self.log.debug(
            f"Looking for cleanup of {len(hier_attr_conf_by_id)}"
            " hierarchical custom attributes."
        )
        attr_value_items = query_custom_attribute_values(
            session, hier_attr_conf_by_id.keys(), entity_ids
        )
        values_by_attr_id = {
            attr_id: []
            for attr_id in hier_attr_conf_by_id
        }
        for value_item in attr_value_items:
            attr_id = value_item["configuration_id"]
            if value_item["value"] is None:
                values_by_attr_id[attr_id].append(value_item)

        for attr_id, none_values in values_by_attr_id.items():
            if not none_values:
                continue

            attr = hier_attr_conf_by_id[attr_id]
            attr_key = attr["key"]
            self.log.debug(
                f"Attribute \"{attr_key}\" has {len(none_values)}"
                " empty values. Cleaning up."
            )
            for item in none_values:
                entity_id = item["entity_id"]
                entity_key = collections.OrderedDict((
                    ("configuration_id", attr_id),
                    ("entity_id", entity_id)
                ))
                session.recorded_operations.push(
                    ftrack_api.operation.DeleteEntityOperation(
                        "CustomAttributeValue",
                        entity_key
                    )
                )
            session.commit()

        return True
