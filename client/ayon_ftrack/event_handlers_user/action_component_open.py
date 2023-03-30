import os
import subprocess
import platform

from ftrack_common import BaseAction
from ayon_ftrack.lib import statics_icon
from openpype.lib import run_detached_process


class ComponentOpen(BaseAction):
    identifier = "component.open"
    label = "Open File"
    icon = statics_icon("ftrack", "action_icons", "ComponentOpen.svg")

    def discover(self, session, entities, event):
        if len(entities) != 1:
            return False

        return entities[0].entity_type == "FileComponent"

    def launch(self, session, entities, event):
        entity = entities[0]

        # Return error if component is on ftrack server
        location_name = entity["component_locations"][0]["location"]["name"]
        if location_name == "ftrack.server":
            return {
                "success": False,
                "message": "This component is stored on ftrack server!"
            }

        # Get component filepath
        # TODO with locations it will be different???
        fpath = entity["component_locations"][0]["resource_identifier"]
        fpath = os.path.normpath(os.path.dirname(fpath))

        if not os.path.isdir(fpath):
            return {
                "success": False,
                "message": f"Didn't found file: {fpath}"
            }
        platform_name = platform.system().lower()
        if platform_name == "windows":
            run_detached_process(["explorer", fpath])
        elif platform_name == "darwin":
            run_detached_process(["open", fpath])
        else:
            try:
                proc = subprocess.Popen(["xdg-open", "--help"])
                proc.wait()
                returncode = proc.returncode
            except OSError:
                # This happens when 'xdg-open' is not available
                returncode = -1

            if returncode != 0:
                return {
                    "success": False,
                    "message": (
                        "Unknown way to open folder explorer on your system"
                    )
                }
            run_detached_process(["xdg-open", fpath])

        return {
            "success": True,
            "message": "Component folder Opened"
        }


def register(session):
    ComponentOpen(session).register()
