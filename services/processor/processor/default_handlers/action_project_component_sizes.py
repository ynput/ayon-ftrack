import os
import sys
import tempfile
import csv
import datetime
import json
import collections

from ftrack_common import (
    ServerAction,
    BaseEventHandler,
    get_service_ftrack_icon_url,
    create_chunks,
)


def format_file_size(file_size, suffix=None):
    """Returns formatted string with size in appropriate unit.

    Args:
        file_size (int): Size of file in bytes.
        suffix (str): Suffix for formatted size. Default is 'B' (as bytes).

    Returns:
        str: Formatted size using proper unit and passed suffix (e.g. 7 MiB).
    """

    if suffix is None:
        suffix = "B"

    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(file_size) < 1024.0:
            return "%3.1f%s%s" % (file_size, unit, suffix)
        file_size /= 1024.0
    return "%.1f%s%s" % (file_size, "Yi", suffix)


class ProjectComponentsSizesCalculator(BaseEventHandler):
    def register(self):
        self.session.event_hub.subscribe(
            "topic=ayon.calculate.project.component.size",
            self._launch,
            priority=self.priority
        )

    def launch(self, session, event):
        project_id = event["data"]["project_id"]
        job_id = event["data"]["job_id"]
        job_entity = session.query(
            f"select id, data from Job where id is '{job_id}'"
        ).first()
        # Skip if job or project cannot be found
        if job_entity is None or job_entity["status"] != "running":
            return

        job_data = json.loads(job_entity["data"])
        try:
            job_entity = self._calculate_project_size(
                session, project_id, job_entity, job_data
            )
        except Exception:
            self.log.warning("Project calculation failed", exc_info=True)
            return

        if self._is_job_done(job_entity):
            self._finish_job(session, event, job_entity)

    def _get_project_entities(self, session, project_id):
        entity_ids = set()
        hierarchy_queue = collections.deque()
        hierarchy_queue.append([project_id])
        while hierarchy_queue:
            parent_ids = hierarchy_queue.popleft()
            new_parent_ids = []
            for _parent_ids in create_chunks(parent_ids):
                if not _parent_ids:
                    continue
                entities = session.query(
                    "select id from TypedContext where"
                    f" project_id is '{project_id}'"
                    f" and parent_id in ({self.join_query_keys(_parent_ids)})"
                ).all()
                for entity in entities:
                    if entity.entity_type.lower() == "task":
                        continue
                    entity_id = entity["id"]
                    entity_ids.add(entity_id)
                    new_parent_ids.append(entity_id)
            if new_parent_ids:
                hierarchy_queue.append(new_parent_ids)
        return entity_ids

    def _get_asset_ids(self, session, entity_ids):
        assets_ids = set()
        for _entity_ids in create_chunks(entity_ids):
            assets = session.query(
                "select id from Asset"
                f" where context_id in ({self.join_query_keys(_entity_ids)})"
            ).all()
            assets_ids |= {
                asset["id"]
                for asset in assets
            }
        return assets_ids

    def _get_asset_version_ids(self, session, asset_ids):
        asset_version_ids = set()
        for entity_ids in create_chunks(asset_ids):
            asset_versions = session.query(
                "select id from AssetVersion"
                f" where asset_id in ({self.join_query_keys(entity_ids)})"
            ).all()
            asset_version_ids |= {
                asset_version["id"]
                for asset_version in asset_versions
            }
        return asset_version_ids

    def _get_components_size(
        self, session, asset_version_ids, location_names
    ):
        size = 0
        for entity_ids in create_chunks(asset_version_ids):
            components = session.query(
                "select id, size from Component"
                f" where version_id in ({self.join_query_keys(entity_ids)})"
                " and component_locations.location.name"
                f" in ({self.join_query_keys(location_names)})"
            ).all()
            size += sum([component["size"] for component in components])
        return size

    def _set_progress_description(self, job_data):
        description_template = job_data["desc_template"]
        finished_projects = sum((
            1
            for project_info in job_data["project_data"].values()
            if project_info["done"]
        ))
        job_data["description"] = (
            description_template.format(finished_projects)
        )

    def _calculate_project_size(
        self, session, project_id, job_entity, job_data
    ):
        location_names = ["ftrack.server", "ftrack.review"]

        project = session.query(
            f"select id, full_name from Project where id is '{project_id}'"
        ).first()

        project_info = job_data["project_data"][project_id]
        # If more than 3 attemps already happened, mark as done
        if not project or project_info["attempts"] > 2:
            project_info["done"] = True
            self._set_progress_description(job_data)
            job_entity["data"] = json.dumps(job_data)
            session.commit()
            return job_entity

        # Set attempts to higher number
        project_info["attempts"] += 1
        job_entity["data"] = json.dumps(job_data)
        session.commit()

        project_name = project["full_name"]

        self.log.debug(f"Calculating size of project '{project_name}'")

        entity_ids = self._get_project_entities(session, project_id)
        asset_ids = self._get_asset_ids(session, entity_ids)
        asset_version_ids = self._get_asset_version_ids(session, asset_ids)
        size = self._get_components_size(
            session, asset_version_ids, location_names)

        job_entity = session.query(
            f"Job where id is \"{job_entity['id']}\"").first()
        self.log.debug((
            f"Project '{project_name}' size is {format_file_size(size)}"
        ))
        job_data = json.loads(job_entity["data"])
        project_data = job_data.get("project_data")
        if project_data is None:
            return job_entity
        project_info = project_data[project_id]
        if project_info["size"] != -1:
            return job_entity
        project_info["size"] = size
        project_info["done"] = True
        self._set_progress_description(job_data)
        job_entity["data"] = json.dumps(job_data)
        session.commit()
        return job_entity

    def _add_output_to_job(self, session, job_entity, output, component_name):
        # Sort by name
        sorted_output = sorted(output.items(), key=lambda i: i[0])
        # Sort by size in reverse
        sorted_output.sort(key=lambda i: i[1], reverse=True)

        # Create temp file where output will be stored
        temp_obj = tempfile.NamedTemporaryFile(
            mode="w", prefix="ayon_ftrack_", suffix=".csv", delete=False
        )
        temp_obj.close()
        temp_filepath = temp_obj.name
        # Store the output
        with open(temp_filepath, "w") as stream:
            writer = csv.writer(stream)
            for row in sorted_output:
                project_name, size = row
                writer.writerow([project_name, format_file_size(size), size])

        self.add_file_component_to_job(
            job_entity, session, temp_filepath, component_name
        )

        os.remove(temp_filepath)

    def _is_job_done(self, job_entity):
        job_data = json.loads(job_entity["data"])
        project_data = job_data.get("project_data")
        if not project_data:
            return False
        for project_info in project_data.values():
            if not project_info["done"]:
                return False
        return True

    def _set_job_description(self, session, job_entity, description):
        job_entity["data"] = json.dumps({"description": description})
        session.commit()

    def _finish_job(self, session, event, job_entity):
        data = json.loads(job_entity["data"])
        component_name = data["component_name"]

        sizes_per_project = {
            project_info["name"]: project_info["size"]
            for project_info in data["project_data"].values()
        }

        self.log.debug((
            "Finished."
            f" Uploading result to job component '{component_name}'"))

        self._set_job_description(session, job_entity, "Uploading file")

        self._add_output_to_job(
            session,
            job_entity,
            sizes_per_project,
            component_name
        )

        job_entity["status"] = "done"
        self._set_job_description(
            session, job_entity, "Finished. Click to download"
        )

        self.show_message(
            event,
            "Size calculation finished. You can download csv from job.",
            True
        )


class ProjectComponentsSizes(ServerAction):
    """Calculate project components sizes.

    Action can calculate sizes of all available project or single project.
    """

    identifier = "project.components.sizes"
    label = "AYON Admin"
    variant = "- Calculate project component sizes"
    description = "Calculate component sizes of all versions on ftrack server"
    icon = get_service_ftrack_icon_url("AYONAdmin.svg")
    settings_key = "project_components_sizes"

    def discover(self, session, entities, event):
        """Check if action is available for user role."""
        ftrack_settings = self.get_ftrack_settings(session, event, entities)
        settings = (
            ftrack_settings[self.settings_frack_subkey][self.settings_key]
        )
        if settings["enabled"]:
            return self.valid_roles(session, entities, event)
        return False

    def interface(self, session, entities, event):
        self.log.info(json.dumps(dict(event.items()), indent=4, default=str))
        if event["data"].get("values"):
            return

        title = "Confirm your workflow"
        enum_items = [
            {
                "label": "All projects",
                "value": "all_projects"
            }
        ]
        items = [
            {
                "name": "workflow",
                "label": "Workflow",
                "type": "enumerator",
                "value": "all_projects",
                "data": enum_items
            }
        ]

        project = None
        for entity in entities:
            project = self.get_project_from_entity(entity, session)
            if project:
                break
        label = "Couldn't find a project in your selection."
        if project:
            enum_items.append({
                "label": "Selected project",
                "value": "selected_project"
            })
            label = f"Selected project is '{project['full_name']}'."

        items.append({
            "type": "label",
            "value": f"<b>NOTE:</b> {label}"
        })

        return {
            "items": items,
            "title": title,
            "submit_button_label": "Confirm"
        }

    def launch(self, session, entities, event):
        if "values" not in event["data"]:
            return

        values = event["data"]["values"]
        workflow = values["workflow"]
        current_date = datetime.datetime.now().strftime("%y-%m-%d-%H%M")
        self.log.debug(f"User selected '{workflow}' workflow")
        if workflow == "selected_project":
            project = None
            for entity in entities:
                project = self.get_project_from_entity(entity, session)
                if project:
                    break

            if not project:
                return {
                    "type": "message",
                    "success": False,
                    "message": (
                        "Had issue to find a project in your selection."
                    )
                }
            project_name = project["full_name"]
            component_base_name = f"{project_name}_size"
            project_entities = [project]
        else:
            project_entities = session.query(
                "select id, full_name from Project").all()
            component_base_name = "AllProjects_size"

        if not project_entities:
            self.log.info("There are no projects to calculate size on.")
            return {
                "type": "message",
                "success": False,
                "message": (
                    "Had issue to find a project in your selection."
                )
            }

        component_name = f"{component_base_name}_{current_date}"

        user_entity = session.query(
            "User where id is {}".format(event["source"]["user"]["id"])
        ).one()
        username = user_entity.get("username")
        if not username:
            username = (
                f"{user_entity['first_name']} {user_entity['last_name']}"
            )

        job_entity = session.create(
            "Job",
            {
                "user": user_entity,
                "status": "running",
                "data": json.dumps({
                    "description": "Size calculation started"
                })
            }
        )
        session.commit()

        try:
            output = self._create_calculate_jobs(
                session, project_entities, job_entity, event, component_name
            )
            self.log.debug(
                f"Created job for '{username}'. Calculation started.")

        except Exception as exc:
            # Get exc info before changes in logging to be able to upload it
            #   to the job.
            exc_info = sys.exc_info()
            self.log.warning(
                "Calculation of project size failed.", exc_info=exc)
            session.rollback()

            description = "Size calculation Failed (Download traceback)"
            self.add_traceback_to_job(
                job_entity, session, exc_info, description
            )
            output = {
                "type": "message",
                "success": False,
                "message": (
                    "Failed to calculate sizes."
                    " Error details can be found in a job."
                )
            }

        return output

    def _create_calculate_jobs(
        self, session, projects, job_entity, event, component_name
    ):
        description_template = (
            f"Size calculation ({{}}/{len(projects)})")
        job_data = json.loads(job_entity["data"])
        job_data.update({
            "desc_template": description_template,
            "component_name": component_name,
            "project_data": {
                project["id"]: {
                    "size": -1,
                    "attempts": 0,
                    "done": False,
                    "name": project["full_name"],
                }
                for project in projects
            }
        })
        job_entity["data"] = json.dumps(job_data)
        session.commit()

        for project in projects:
            project_id = project["id"]

            self.trigger_event(
                "ayon.calculate.project.component.size",
                event_data={
                    "project_id": project_id,
                    "job_id": job_entity["id"]
                },
                session=session,
                source=event["source"],
                event=event,
                on_error="ignore"
            )

        return {
            "type": "message",
            "success": True,
            "message": (
                "This may take some time. Look into jobs to check progress."
            )
        }
