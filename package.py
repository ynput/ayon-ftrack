name = "ftrack"
version = "1.2.4"
title = "ftrack"
client_dir = "ayon_ftrack"

services = {
    "leecher": {"image": f"ynput/ayon-ftrack-leecher:{version}"},
    "processor": {"image": f"ynput/ayon-ftrack-processor:{version}"}
}

plugin_for = ["ayon_server"]

ayon_required_addons = {
    "core": ">=0.4.3",
}
ayon_compatible_addons = {
    "applications": ">=0.2.4",
}
