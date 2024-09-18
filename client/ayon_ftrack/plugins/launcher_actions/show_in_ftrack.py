"""
Example Ftrack URL:

https://pype.ftrackapp.com/#slideEntityId=38c5fec4-0aed-11ea-a454-3e41ec9bc0d6&slideEntityType=show&view=tasks&itemId=projects&entityId=38c5fec4-0aed-11ea-a454-3e41ec9bc0d6&entityType=show

# This is required otherwise is url invalid view=tasks&itemId=projects&entityId=38c5fec4-0aed-11ea-a454-3e41ec9bc0d6&entityType=show

- "itemId=projects" the top category (overview / projects/ reports / ...)
    must be 'projects'
- "view=tasks" view category 'tasks' is best
- "entityId=38c5fec4-0aed-11ea-a454-3e41ec9bc0d6" id of entity which is in focus (project id is easiest)
- "entityType=show" entity type of 'entityId'

Entity detail in slide (on right side) can't be used on it's own: slideEntityId=38c5fec4-0aed-11ea-a454-3e41ec9bc0d6&slideEntityType=show
- "slideEntityId=38c5fec4-0aed-11ea-a454-3e41ec9bc0d6" entity id which is showed in detail
- "slideEntityType=show" type of 'slideEntityId' entity

Warning: entityType is not entity_type!
    entityType "show" == entity_type "Project"
    entityType "task" == entity_type "Task", "Shot", "Library", "Folder", ...

"""
import webbrowser

from ayon_core.pipeline import LauncherAction
from ayon_core.addon import AddonsManager
from ayon_api import get_project, get_folder_by_path


class ShowInFtrack(LauncherAction):
    name = "showinftrack"
    label = "Show in Ftrack"
    icon = "external-link-square"
    color = "#e0e1e1"
    order = 10

    @staticmethod
    def get_ftrack_addon():
        return AddonsManager().get("ftrack")

    def is_compatible(self, selection):
        if not selection.is_project_selected:
            return False
        return True

    def process(self, selection, **kwargs):
        ftrack_addon = self.get_ftrack_addon()
        ftrack_url = ftrack_addon.ftrack_url

        # Context inputs
        project_name = selection.project_name
        folder_path = selection.folder_path

        fields = ["data.ftrackId"]
        project_entity = get_project(project_name=project_name,
                                     fields=fields)
        if not project_entity:
            raise RuntimeError(f"Project {project_name} not found.")

        project_ftrack_id = project_entity["data"].get("ftrackId")
        if not project_ftrack_id:
            raise RuntimeError(
                f"Project {project_name} has no connected ftrack id.")

        entity_ftrack_id = None
        if folder_path:
            folder_entity = get_folder_by_path(project_name,
                                               folder_path=folder_path,
                                               fields=fields)
            entity_ftrack_id = folder_entity["data"].get("ftrackId")

        # TODO: implement task entity support?

        # Construct the ftrack URL
        # Required
        data = {
            "itemId": "projects",
            "view": "tasks",
            "entityId": project_ftrack_id,
            "entityType": "show"
        }

        # Optional slide
        if entity_ftrack_id:
            data.update({
                "slideEntityId": entity_ftrack_id,
                "slideEntityType": "task"
            })

        sub_url = "&".join("{}={}".format(key, value) for
                           key, value in data.items())
        url = f"{ftrack_url}/#{sub_url}"

        # Open URL in webbrowser
        self.log.info(f"Opening URL: {url}")
        webbrowser.open_new_tab(url)
