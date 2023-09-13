from ftrack_common import (
    ServerAction,
    get_service_ftrack_icon_url,
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


def register(session):
    SyncFromFtrackAction(session).register()
