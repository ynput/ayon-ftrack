from ftrack_common.event_handlers import ServerAction
from processor.lib import get_icon_url
from processor.lib.sync_from_ftrack import SyncFromFtrack


class SyncFromFtrackAction(ServerAction):
    """Prepare project attributes in Anatomy."""

    identifier = "sync.from.ftrack.server"
    label = "OpenPype Admin"
    variant = "- Sync from ftrack"
    description = "Synchronize project hierarchy based on ftrack"
    icon = get_icon_url("OpenPypeAdmin.svg")

    role_list = ["Pypeclub", "Administrator", "Project Manager"]

    settings_key = "sync_from_ftrack"

    item_splitter = {"type": "label", "value": "---"}

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
        self.log.info("Synchronization finished")
        return True


def register(session):
    SyncFromFtrackAction(session).register()
