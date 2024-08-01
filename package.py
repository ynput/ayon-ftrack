name = "ftrack"
version = "1.1.9-dev.1"
title = "Ftrack"
client_dir = "ayon_ftrack"

services = {
    "leecher": {"image": f"ynput/ayon-ftrack-leecher:{version}"},
    "processor": {"image": f"ynput/ayon-ftrack-processor:{version}"}
}

plugin_for = ["ayon_server"]

ayon_required_addons = {
    "core": ">=0.3.0",
}
ayon_compatible_addons = {
    "applications": ">=0.2.4",
}
