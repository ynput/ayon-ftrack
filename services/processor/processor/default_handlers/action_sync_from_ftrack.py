import ftrack_api

from ftrack_common import (
    ServerAction,
    get_service_ftrack_icon_url,
    CUST_ATTR_AUTO_SYNC,
)
from processor.lib import SyncFromFtrack


class SyncFromFtrackAction(ServerAction):
    """Prepare project attributes in Anatomy."""

    identifier = "sync.from.ftrack.server"
    label = "AYON Admin"
    variant = "- Sync to AYON"
    description = "Synchronize project hierarchy based on ftrack"
    icon = get_service_ftrack_icon_url("AYONAdmin.svg")

    role_list = ["Pypeclub", "Administrator", "Project Manager"]

    settings_key = "sync_from_ftrack"

    def discover(self, session, entities, event):
        """Show only on project."""
        if (
            len(entities) != 1
            or entities[0].entity_type.lower() != "project"
        ):
            return False
        return self.valid_roles(session, entities, event)

    def launch(self, session, entities, event):
        self.log.info("Synchronization begins")
        project = self.get_project_from_entity(entities[0])
        project_name = project["full_name"]
        syncer = SyncFromFtrack(session, project_name, self.log)
        syncer.sync_to_server()
        report_items = syncer.report_items
        if report_items:
            self.show_interface(
                report_items,
                title="Sync to AYON report",
                event=event
            )
        self.log.info("Synchronization finished")
        return True

    def register(self):
        super().register()

        # Listen to leecher start event
        self.session.event_hub.subscribe(
            "topic=ayon.ftrack.leecher.started",
            self._on_leecher_start,
            priority=self.priority
        )

    def _on_leecher_start(self, event):
        """Trigger Sync to AYON action when leecher starts.

        The action is triggered for all project that have enabled auto-sync.
        """

        session = self.session
        if session is None:
            self.log.warning(
                "Session is not set. Can't trigger Sync to AYON action.")
            return True

        projects = session.query("Project").all()
        if not projects:
            return True

        selections = []
        for project in projects:
            if project["status"] != "active":
                continue

            auto_sync = project["custom_attributes"].get(CUST_ATTR_AUTO_SYNC)
            if not auto_sync:
                continue

            selections.append({
                "entityId": project["id"],
                "entityType": "show"
            })

        if not selections:
            return

        user = session.query(
            "User where username is \"{}\"".format(session.api_user)
        ).one()
        user_data = {
            "username": user["username"],
            "id": user["id"]
        }

        for selection in selections:
            event_data = {
                "actionIdentifier": self.launch_identifier,
                "selection": [selection]
            }
            session.event_hub.publish(
                ftrack_api.event.base.Event(
                    topic="ftrack.action.launch",
                    data=event_data,
                    source=dict(user=user_data)
                ),
                on_error="ignore"
            )


def register(session):
    SyncFromFtrackAction(session).register()
