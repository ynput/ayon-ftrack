import os

RESOURCES_DIR = os.path.dirname(os.path.abspath(__file__))


def get_resource(*args):
    """ Serves to simple resources access

    Args:
        *args: should contain *subfolder* names and *filename* of
                  resource from resources folder
    Returns:
        str: Path to resource.

    """
    return os.path.normpath(os.path.join(RESOURCES_DIR, *args))
