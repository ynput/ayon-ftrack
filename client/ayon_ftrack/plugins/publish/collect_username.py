"""Loads publishing context from json and continues in publish process.

Should run before 'CollectAnatomyContextData' so the user on context is
changed before it's stored to context anatomy data or instance anatomy data.

Requires:
    anatomy -> context["anatomy"] *(pyblish.api.CollectorOrder - 0.11)

Provides:
    context, instances -> All data from previous publishing process.
"""

import os

import ftrack_api
import pyblish.api


class CollectUsernameForWebpublish(pyblish.api.ContextPlugin):
    """
        Translates user email to Ftrack username.

        Emails in Ftrack are same as company's Slack, username is needed to
        load data to Ftrack.

        Expects "pype.club" user created on Ftrack and FTRACK_BOT_API_KEY env
        var set up.

        Resets `context.data["user"] to correctly populate `version.author` and
        `representation.context.username`

    """

    settings_category = "ftrack"

    order = pyblish.api.CollectorOrder + 0.0015
    label = "Collect ftrack username"
    hosts = ["webpublisher", "photoshop"]
    targets = ["remotepublish", "filespublish", "tvpaint_worker"]

    def process(self, context):
        self.log.info("{}".format(self.__class__.__name__))
        os.environ["FTRACK_API_USER"] = os.environ["FTRACK_BOT_API_USER"]
        os.environ["FTRACK_API_KEY"] = os.environ["FTRACK_BOT_API_KEY"]

        # for publishes with studio processing
        user_email = os.environ.get("USER_EMAIL")
        self.log.debug("Email from env:: {}".format(user_email))
        if not user_email:
            # for basic webpublishes
            for instance in context:
                user_email = instance.data.get("user_email")
                self.log.debug("Email from instance:: {}".format(user_email))
                break

        if not user_email:
            self.log.info("No email found")
            return

        session = ftrack_api.Session(auto_connect_event_hub=False)
        user = session.query(
            "User where email like '{}'".format(user_email)
        ).first()

        if not user:
            raise ValueError(
                "Couldn't find user with {} email".format(user_email))

        username = user.get("username")
        self.log.debug("Resolved ftrack username:: {}".format(username))
        os.environ["FTRACK_API_USER"] = username

        burnin_name = username
        if '@' in burnin_name:
            burnin_name = burnin_name[:burnin_name.index('@')]
        context.data["user"] = burnin_name
