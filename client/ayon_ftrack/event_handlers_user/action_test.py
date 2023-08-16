from ayon_ftrack.common import LocalAction
from ayon_ftrack.lib import get_ftrack_icon_url


class TestAction(LocalAction):
    """Action for testing purpose or as base for new actions."""

    enabled = False

    identifier = "test.action"
    label = "Test action"
    description = "Test action"
    priority = 10000
    icon = get_ftrack_icon_url("TestAction.svg")

    def discover(self, session, entities, event):
        return True

    def launch(self, session, entities, event):
        self.log.info(event)

        return True


def register(session):
    TestAction(session).register()
