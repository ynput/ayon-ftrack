# -*- coding: utf-8 -*-
"""Collect default Deadline server."""
import os
import pyblish.api


class CollectLocalFtrackCreds(pyblish.api.ContextPlugin):
    """Collect default Royal Render path."""

    order = pyblish.api.CollectorOrder + 0.01
    label = "Collect local ftrack credentials"
    targets = ["rr_control"]

    settings_category = "ftrack"

    def process(self, context):
        if (
            os.getenv("FTRACK_API_USER")
            and os.getenv("FTRACK_API_KEY")
            and os.getenv("FTRACK_SERVER")
        ):
            return
        addon = context.data["openPypeModules"]["ftrack"]
        if addon.enabled:
            creds = addon.get_credentials()
            username, api_key = creds
            os.environ["FTRACK_API_USER"] = username
            os.environ["FTRACK_API_KEY"] = api_key
            os.environ["FTRACK_SERVER"] = addon.ftrack_url
