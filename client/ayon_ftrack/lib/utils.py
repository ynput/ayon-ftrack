import os
from ayon_ftrack.version import __version__
from ayon_ftrack.common import get_ftrack_icon_url as _get_ftrack_icon_url


def statics_icon(*icon_statics_file_parts):
    statics_server = os.environ.get("OPENPYPE_STATICS_SERVER")
    if not statics_server:
        return None
    return "/".join((statics_server, *icon_statics_file_parts))


def get_ftrack_icon_url(icon_name):
    return _get_ftrack_icon_url(icon_name, addon_version=__version__)
