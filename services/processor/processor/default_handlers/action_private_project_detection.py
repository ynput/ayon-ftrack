from ftrack_common.event_handlers import ServerAction


class PrivateProjectDetectionAction(ServerAction):
    """Action helps to identify if does not have access to project."""

    identifier = "ayon.server.missing.perm.private.project"
    label = "Missing permissions (AYON)"
    description = (
        "Main ftrack event server does not have access to this project."
    )

    def _discover(self, event):
        """Show action only if there is a selection in event data."""
        entities = self._translate_event(event)
        if entities:
            return None

        selection = event["data"].get("selection")
        if not selection:
            return None

        return {
            "items": [{
                "label": self.label,
                "variant": self.variant,
                "description": self.description,
                "actionIdentifier": self.discover_identifier,
                "icon": self.icon,
            }]
        }

    def _launch(self, event):
        # Ignore if there are values in event data
        # - somebody clicked on submit button
        values = event["data"].get("values")
        if values:
            return None

        title = "# Private project (missing permissions) #"
        msg = (
            "User ({}) or API Key used on ftrack event server"
            " does not have permissions to access this private project."
        ).format(self.session.api_user)
        return {
            "type": "form",
            "title": "Missing permissions",
            "items": [
                {"type": "label", "value": title},
                {"type": "label", "value": msg},
                # Add hidden to be able detect if was clicked on submit
                {"type": "hidden", "value": "1", "name": "hidden"}
            ],
            "submit_button_label": "Got it"
        }
