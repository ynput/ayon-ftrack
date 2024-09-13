from ayon_ftrack.common import LocalAction
from ayon_ftrack.lib import get_ftrack_icon_url


class MultipleNotes(LocalAction):
    identifier = "ayon.multiple.notes"
    label = "Multiple Notes"
    description = "Add same note to multiple entities"
    icon = get_ftrack_icon_url("MultipleNotes.svg")

    def discover(self, session, entities, event):
        # Check for multiple selection.
        if len(entities) < 2:
            return False

        # Check for valid entities.
        for entity in entities:
            if entity.entity_type.lower() not in {"assetversion", "task"}:
                return False

        return True

    def interface(self, session, entities, event):
        if event["data"].get("values"):
            return None

        category_data = [{
            "label": "- None -",
            "value": "none"
        }]
        all_categories = session.query(
            "select id, name from NoteCategory"
        ).all()
        for cat in all_categories:
            category_data.append({
                "label": cat["name"],
                "value": cat["id"]
            })
        category_value = {
            "type": "enumerator",
            "name": "category",
            "data": category_data,
            "value": "none"
        }

        return [
            {
                "type": "label",
                "value": "# Enter note: #"
            },
            {
                "name": "note",
                "type": "textarea"
            },
            {
                "type": "label",
                "value": "---"
            },
            {
                "type": "label",
                "value": "## Category: ##"
            },
            category_value,
        ]

    def launch(self, session, entities, event):
        if "values" not in event["data"]:
            return

        values = event["data"].get("values")
        if not values or "note" not in values:
            return False
        # Get Note text
        note_value = values["note"]
        if note_value.lower().strip() == "":
            return False

        # Get User
        user = session.query(
            f"User where username is \"{session.api_user}\""
        ).one()
        # Base note data
        note_data = {
            "content": note_value,
            "author": user
        }
        # Get category
        category_value = values["category"]
        if category_value != "none":
            category = session.query(
                f"NoteCategory where id is \"{category_value}\""
            ).one()
            note_data["category"] = category
        # Create notes for entities
        for entity in entities:
            new_note = session.create("Note", note_data)
            entity["notes"].append(new_note)
            session.commit()
        return True
