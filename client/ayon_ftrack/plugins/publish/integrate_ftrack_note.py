"""
Requires:
    context > hostName
    context > appName
    context > appLabel
    context > comment
    context > ftrackSession
    instance > ftrackIntegratedAssetVersionsData
"""

import sys
import copy

import six
import pyblish.api
from openpype.lib import StringTemplate


class IntegrateFtrackNote(pyblish.api.InstancePlugin):
    """Create comments in Ftrack."""

    # Must be after integrate asset new
    order = pyblish.api.IntegratorOrder + 0.4999
    label = "Integrate Ftrack note"
    families = ["ftrack"]
    optional = True
    settings_category = "ftrack"

    # Can be set in presets:
    # - Allows only `intent` and `comment` keys
    note_template = None
    # Backwards compatibility
    note_with_intent_template = "{intent}: {comment}"
    # - note label must exist in Ftrack
    note_labels = []

    def process(self, instance):
        # Check if there are any integrated AssetVersion entities
        asset_versions_key = "ftrackIntegratedAssetVersionsData"
        asset_versions_data_by_id = instance.data.get(asset_versions_key)
        if not asset_versions_data_by_id:
            self.log.info("There are any integrated AssetVersions")
            return

        context = instance.context
        host_name = context.data["hostName"]
        app_name = context.data["appName"]
        app_label = context.data["appLabel"]
        comment = instance.data["comment"]
        if not comment:
            self.log.debug("Comment is not set.")
        else:
            self.log.debug("Comment is set to `{}`".format(comment))

        session = context.data["ftrackSession"]

        intent = instance.context.data.get("intent")
        intent_label = None
        if intent:
            value = intent["value"]
            if value:
                intent_label = intent["label"] or value

        # if intent label is set then format comment
        # - it is possible that intent_label is equal to "" (empty string)
        if intent_label:
            self.log.debug(
                "Intent label is set to `{}`.".format(intent_label)
            )

        else:
            self.log.debug("Intent is not set.")

        user = session.query(
            "User where username is \"{}\"".format(session.api_user)
        ).first()
        if not user:
            self.log.warning(
                "Was not able to query current User {}".format(
                    session.api_user
                )
            )

        labels = []
        if self.note_labels:
            all_labels = session.query("select id, name from NoteLabel").all()
            labels_by_low_name = {lab["name"].lower(): lab for lab in all_labels}
            for _label in self.note_labels:
                label = labels_by_low_name.get(_label.lower())
                if not label:
                    self.log.warning(
                        "Note Label `{}` was not found.".format(_label)
                    )
                    continue

                labels.append(label)

        base_format_data = {
            "host_name": host_name,
            "app_name": app_name,
            "app_label": app_label,
            "source": instance.data.get("source", '')
        }
        if comment:
            base_format_data["comment"] = comment
        for asset_version_data in asset_versions_data_by_id.values():
            asset_version = asset_version_data["asset_version"]
            component_items = asset_version_data["component_items"]

            published_paths = set()
            for component_item in component_items:
                published_paths.add(component_item["component_path"])

            # Backwards compatibility for older settings using
            #   attribute 'note_with_intent_template'
            template = self.note_template
            if template is None:
                template = self.note_with_intent_template
            format_data = copy.deepcopy(base_format_data)
            format_data["published_paths"] = "<br/>".join(
                sorted(published_paths)
            )
            if intent:
                if "{intent}" in template:
                    format_data["intent"] = intent_label
                else:
                    format_data["intent"] = intent

            note_text = StringTemplate.format_template(template, format_data)
            if not note_text.solved:
                self.log.debug((
                    "Note template require more keys then can be provided."
                    "\nTemplate: {}\nMissing values for keys:{}\nData: {}"
                ).format(template, note_text.missing_keys, format_data))
                continue

            if not note_text:
                self.log.debug((
                    "Note for AssetVersion {} would be empty. Skipping."
                    "\nTemplate: {}\nData: {}"
                ).format(asset_version["id"], template, format_data))
                continue
            asset_version.create_note(note_text, author=user, labels=labels)

            try:
                session.commit()
                self.log.debug("Note added to AssetVersion \"{}\"".format(
                    str(asset_version)
                ))
            except Exception:
                tp, value, tb = sys.exc_info()
                session.rollback()
                session._configure_locations()
                six.reraise(tp, value, tb)
