"""
Requires:
    context > hostName
    context > appName
    context > appLabel
    context > comment
    context > ftrackSession
    instance > ftrackIntegratedAssetVersionsData
"""

import copy

import pyblish.api

from ayon_core.lib import StringTemplate

from ayon_ftrack.pipeline import plugin


class IntegrateFtrackNote(plugin.FtrackPublishInstancePlugin):
    """Create comments in ftrack."""

    # Must be after IntegrateAsset plugin in ayon_core
    order = pyblish.api.IntegratorOrder + 0.4999
    label = "Integrate ftrack note"
    families = ["ftrack"]

    # Can be set in presets:
    # - Allows only `comment` keys
    note_template = None
    # - note label must exist in ftrack
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

        # if intent label is set then format comment
        # - it is possible that intent_label is equal to "" (empty string)
        user = session.query(
            f"User where username is \"{session.api_user}\""
        ).first()
        if not user:
            self.log.warning(
                f"Was not able to query current User {session.api_user}"
            )

        labels = []
        if self.note_labels:
            all_labels = session.query("select id, name from NoteLabel").all()
            labels_by_low_name = {
                lab["name"].lower(): lab
                for lab in all_labels
            }
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
            "source": instance.data.get("source", "")
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
            note_text = StringTemplate.format_template(template, format_data)
            if not note_text.solved:
                self.log.debug(
                    "Note template require more keys then can be provided."
                    f"\nTemplate: {template}"
                    f"\nMissing values for keys:{note_text.missing_keys}"
                    f"\nData: {format_data}"
                )
                continue

            if not note_text:
                av_id = asset_version["id"]
                self.log.debug(
                    f"Note for AssetVersion {av_id} would be empty. Skipping."
                    f"\nTemplate: {template}\nData: {format_data}"
                )
                continue
            asset_version.create_note(note_text, author=user, labels=labels)

            try:
                session.commit()
                self.log.debug(
                    f"Note added to AssetVersion \"{asset_version}\""
                )
            except Exception as exc:
                session.rollback()
                session._configure_locations()
                raise exc

