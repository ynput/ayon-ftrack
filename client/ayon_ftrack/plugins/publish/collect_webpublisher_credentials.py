"""Translates uploader's user email to ftrack username.

Should run before 'CollectAnatomyContextData' so the user on context is
changed before it's stored to context anatomy data or instance anatomy data.

Requires:
    instance.data.get("user_email") or os.environ.get("USER_EMAIL")

Provides:
   os.environ["FTRACK_API_USER"]
   context.data["user"]
"""

import os

import ftrack_api
import pyblish.api
import ayon_api

from ayon_core.pipeline import KnownPublishError

from ayon_ftrack.pipeline import plugin


class CollectWebpublisherCredentials(plugin.FtrackPublishContextPlugin):
    """
        Translates uploader's user email to ftrack username.

    It expects that user email in ftrack is same as user email in Ayon server,
    ftrack username is needed to load data to ftrack.

    Resets `context.data["user"] to correctly populate `version.author` and
    `representation.context.username`

    """

    order = pyblish.api.CollectorOrder + 0.0015
    label = "Collect Ftrack credentials for Webpublisher"
    hosts = ["webpublisher", "photoshop"]
    targets = ["webpublish"]

    def process(self, context):
        service_api_key, service_username = self._get_username_key(context)
        os.environ["FTRACK_API_USER"] = service_username
        os.environ["FTRACK_API_KEY"] = service_api_key

        user_email = self._get_user_email(context)

        if not user_email:
            self.log.warning("No email found")
            return

        username = self._get_ftrack_username(user_email)
        os.environ["FTRACK_API_USER"] = username

    def _get_ftrack_username(self, user_email):
        """Queries ftrack api for user with 'user_email'.

        Raises:
            ValueError: if user not found
        """
        session = ftrack_api.Session(auto_connect_event_hub=False)
        user = session.query(
            "User where email like '{}'".format(user_email)
        ).first()
        if not user:
            raise ValueError(
                "Couldn't find user with '{}' email".format(user_email))
        username = user.get("username")
        self.log.debug("Resolved ftrack username:: '{}'".format(username))
        return username

    def _get_user_email(self, context):
        """Collect uploader's email address to lookup user in ftrack"""
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

    def _get_username_key(self, context):
        """Query settings for ftrack credentials."""
        project_settings = context.data["project_settings"]
        service_settings = project_settings["ftrack"]["service_settings"]

        api_key_secret = service_settings["api_key"]
        username_secret = service_settings["username"]

        con = ayon_api.get_server_api_connection()
        with con.as_username(None):
            secrets_by_name = {
                secret["name"]: secret["value"]
                for secret in ayon_api.get_secrets()
            }
        api_key = secrets_by_name.get(api_key_secret)
        username = secrets_by_name.get(username_secret)
        if not api_key or not username:
            raise KnownPublishError(
                "Missing ftrack credentials in settings. "
                "Please let admin fill in "
                "'ayon+settings://ftrack/service_settings'"
            )
        return api_key, username
