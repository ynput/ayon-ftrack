name = "ftrack"
version = "1.1.0-dev.1"
title = "Ftrack"
client_dir = "ayon_ftrack"

services = {
    "leecher": {"image": f"ynput/ayon-ftrack-leecher:{version}"},
    "processor": {"image": f"ynput/ayon-ftrack-processor:{version}"}
}

plugin_for = ["ayon_server"]
