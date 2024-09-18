import re

import ayon_api

from ayon_core.pipeline.project_folders import (
    get_project_basic_paths,
    create_project_folders,
)
from ayon_ftrack.common import LocalAction
from ayon_ftrack.lib import get_ftrack_icon_url


class CreateProjectFolders(LocalAction):
    """Action create folder structure and may create hierarchy in Ftrack.

    Creation of folder structure and hierarchy in Ftrack is based on settings.

    Example of content:
    ```json
    {
        "__project_root__": {
            "prod" : {},
            "resources" : {
              "footage": {
                "plates": {},
                "offline": {}
              },
              "audio": {},
              "art_dept": {}
            },
            "editorial" : {},
            "assets[ftrack.Library]": {
              "characters[ftrack]": {},
              "locations[ftrack]": {}
            },
            "shots[ftrack.Sequence]": {
              "scripts": {},
              "editorial[ftrack.Folder]": {}
            }
        }
    }
    ```
    Key "__project_root__" indicates root folder (or entity). Each key in
    dictionary represents folder name. Value may contain another dictionary
    with subfolders.

    Identifier `[ftrack]` in name says that this should be also created in
    Ftrack hierarchy. It is possible to specify entity type of item with "." .
    If key is `assets[ftrack.Library]` then in ftrack will be created entity
    with name "assets" and entity type "Library". It is expected Library entity
    type exist in Ftrack.
    """

    identifier = "ayon.create.project.structure"
    label = "Create Project Structure"
    description = "Creates folder structure"
    role_list = ["Administrator", "Project Manager"]
    icon = get_ftrack_icon_url("CreateProjectFolders.svg")

    pattern_array = re.compile(r"\[.*\]")
    pattern_ftrack = re.compile(r".*\[[.]*ftrack[.]*")
    pattern_ent_ftrack = re.compile(r"ftrack\.[^.,\],\s,]*")
    pattern_template = re.compile(r"\{.*\}")
    project_root_key = "__project_root__"

    def discover(self, session, entities, event):
        if len(entities) != 1:
            return False

        if entities[0].entity_type.lower() != "project":
            return False

        return True

    def launch(self, session, entities, event):
        # Get project entity
        project_entity = self.get_project_from_entity(entities[0])
        project_name = project_entity["full_name"]
        ayon_project = ayon_api.get_project(project_name)
        if not ayon_project:
            return {
                "success": False,
                "message": f"Project '{project_name}' was not found in AYON.",
            }

        try:
            # Get paths based on presets
            basic_paths = get_project_basic_paths(project_name)
            if not basic_paths:
                return {
                    "success": False,
                    "message": "Project structure is not set."
                }

            # Invoking AYON API to create the project folders
            create_project_folders(project_name, basic_paths)
            self.create_ftrack_entities(basic_paths, project_entity)

            self.trigger_event(
                "ayon.project.structure.created",
                {"project_name": project_name}
            )

        except Exception as exc:
            self.log.warning("Creating of structure crashed.", exc_info=True)
            session.rollback()
            return {
                "success": False,
                "message": str(exc)
            }

        return True

    def get_ftrack_paths(self, paths_items):
        all_ftrack_paths = []
        for path_items in paths_items:
            if not path_items:
                continue

            ftrack_path_items = []
            is_ftrack = False
            for item in reversed(path_items):
                # QUESTION Why this not validated only on first item?
                if (
                    item == self.project_root_key
                    # Fix to skip any formatting items (I don't like it!)
                    # - '{root[work]}' and '{project[name]}'
                    or self.pattern_template.match(item)
                ):
                    continue

                if is_ftrack:
                    ftrack_path_items.append(item)
                elif self.pattern_ftrack.match(item):
                    ftrack_path_items.append(item)
                    is_ftrack = True

            ftrack_path_items = list(reversed(ftrack_path_items))
            if ftrack_path_items:
                all_ftrack_paths.append(ftrack_path_items)
        return all_ftrack_paths

    def compute_ftrack_items(self, in_list, keys):
        if len(keys) == 0:
            return in_list
        key = keys[0]
        exist = None
        for index, subdict in enumerate(in_list):
            if key in subdict:
                exist = index
                break
        if exist is not None:
            in_list[exist][key] = self.compute_ftrack_items(
                in_list[exist][key], keys[1:]
            )
        else:
            in_list.append({key: self.compute_ftrack_items([], keys[1:])})
        return in_list

    def translate_ftrack_items(self, paths_items):
        main = []
        for path_items in paths_items:
            main = self.compute_ftrack_items(main, path_items)
        return main

    def create_ftrack_entities(self, basic_paths, project_ent):
        only_ftrack_items = self.get_ftrack_paths(basic_paths)
        ftrack_paths = self.translate_ftrack_items(only_ftrack_items)

        for separation in ftrack_paths:
            parent = project_ent
            self.trigger_creation(separation, parent)

    def trigger_creation(self, separation, parent):
        for item, subvalues in separation.items():
            matches = self.pattern_array.findall(item)
            ent_type = "Folder"
            if len(matches) == 0:
                name = item
            else:
                match = matches[0]
                name = item.replace(match, "")
                ent_type_match = self.pattern_ent_ftrack.findall(match)
                if len(ent_type_match) > 0:
                    ent_type_split = ent_type_match[0].split(".")
                    if len(ent_type_split) == 2:
                        ent_type = ent_type_split[1]
            new_parent = self.create_ftrack_entity(name, ent_type, parent)
            if subvalues:
                for subvalue in subvalues:
                    self.trigger_creation(subvalue, new_parent)

    def create_ftrack_entity(self, name, ent_type, parent):
        for children in parent["children"]:
            if children["name"] == name:
                return children
        data = {
            "name": name,
            "parent_id": parent["id"]
        }
        if parent.entity_type.lower() == "project":
            data["project_id"] = parent["id"]
        else:
            data["project_id"] = parent["project"]["id"]

        existing_entity = self.session.query((
            "TypedContext where name is \"{}\" and "
            "parent_id is \"{}\" and project_id is \"{}\""
        ).format(name, data["parent_id"], data["project_id"])).first()
        if existing_entity:
            return existing_entity

        new_ent = self.session.create(ent_type, data)
        self.session.commit()
        return new_ent
