import os


def statics_icon(*icon_statics_file_parts):
    statics_server = os.environ.get("OPENPYPE_STATICS_SERVER")
    if not statics_server:
        return None
    return "/".join((statics_server, *icon_statics_file_parts))
