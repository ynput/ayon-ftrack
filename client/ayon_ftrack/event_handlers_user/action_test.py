from ftrack_common import BaseAction
from ayon_ftrack.lib import statics_icon


class TestAction(BaseAction):
    """Action for testing purpose or as base for new actions."""

    ignore_me = True

    identifier = "test.action"
    label = "Test action"
    description = "Test action"
    priority = 10000
    icon = statics_icon("ftrack", "action_icons", "TestAction.svg")

    def discover(self, session, entities, event):
        return True

    def launch(self, session, entities, event):
        self.log.info(event)

        return True


def register(session):
    TestAction(session).register()
