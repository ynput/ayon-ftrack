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
        service_api_key, service_username = self._get_username_key()
        os.environ["FTRACK_API_USER"] = service_username
        os.environ["FTRACK_API_KEY"] = service_api_key

        user_email = self._get_user_email(context)

        if not user_email:
            self.log.warning("No email found")
            return

        username = self._get_ftrack_username(user_email)
        os.environ["FTRACK_API_USER"] = username

        burnin_name = username
        if '@' in burnin_name:
            burnin_name = burnin_name[:burnin_name.index('@')]
        context.data["user"] = burnin_name

    def _get_ftrack_username(self, user_email):
        """Queries Ftrack api for user with 'user_email'.

        Raises:
            ValueError: if user not found
        """
        session = ftrack_api.Session(auto_connect_event_hub=False)
        user = session.query(
            "User where email like '{}'".format(user_email)
        ).first()
        if not user:
            raise ValueError(
                "Couldn't find user with {} email".format(user_email))
        username = user.get("username")
        self.log.debug("Resolved ftrack username:: {}".format(username))
        return username

    def _get_user_email(self, context):
        """Collect uploader's email address to lookup user in Ftrack"""
        # for publishes with studio processing
        user_email = os.environ.get("USER_EMAIL")
        self.log.debug("Email from env:: {}".format(user_email))
        if not user_email:
            # for basic webpublishes
            for instance in context:
                user_email = instance.data.get("user_email")
                self.log.debug("Email from instance:: {}".format(user_email))
                break
        return user_email

    def _get_username_key(self):
        """Query settings for ftrack credentials."""
        ftrack_settings = ayon_api.get_service_addon_settings()
        service_settings = ftrack_settings["service_settings"]

        api_key = service_settings["api_key"]
        username = service_settings["username"]

        secrets_by_name = {
            secret["name"]: secret["value"]
            for secret in ayon_api.get_secrets()
        }
        if api_key in secrets_by_name:
            api_key = secrets_by_name[api_key]
        if username in secrets_by_name:
            username = secrets_by_name[username]
        return api_key, username
