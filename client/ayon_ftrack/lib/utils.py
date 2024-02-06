from ayon_ftrack.version import __version__
from ayon_ftrack.common import get_ftrack_icon_url as _get_ftrack_icon_url


def get_ftrack_icon_url(icon_name):
    return _get_ftrack_icon_url(icon_name, addon_version=__version__)
