import os
import urllib
import json

from platformdirs import user_downloads_dir

from ayon_ftrack.common import (
    LocalAction,
    create_chunks,
)


class DownloadReviewMedia(LocalAction):
    label = "Download Review Media"
    identifier = "download.reviewables"
    description = "Download review media from selected versions"

    def discover(self, _session, entities, _event):
        # Only show this for Versions when a selection has been made
        if not entities:
            return False

        for entity in entities:
            if entity.entity_type.lower() in {
                "assetversion", "reviewsession"
            }:
                return True
        return False

    def launch(self, session, entities, event):
        asset_versions = self._extract_asset_versions(session, entities)
        if not asset_versions:
            return {
                "message": "No review media to download in your selection...",
                "success": False,
            }

        user_id = event["source"].get("user", {}).get("id")
        if user_id:
            self.show_message(
                event, "Preparing information to download...", True
            )

        asset_ids = {
            version["asset_id"]
            for version in asset_versions
        }
        joined_asset_ids = self.join_filter_values(asset_ids)
        assets_by_id = {
            asset["id"]: asset
            for asset in session.query(
                f"select id, name from Asset"
                f" where id in ({joined_asset_ids})"
            ).all()
        }

        asset_versions_by_id = {
            version["id"]: version
            for version in asset_versions
        }
        components = []
        for chunk_ids in create_chunks(asset_versions_by_id.keys()):
            joined_version_ids = self.join_filter_values(chunk_ids)
            components.extend(session.query(
                "select id, name, version_id from Component"
                " where name in ('ftrackreview-mp4', 'ftrackreview-image')"
                f" and version_id in ({joined_version_ids})"
            ).all())

        if not components:
            return {
                "message": (
                    "Selected entities don't have available"
                    " review media to download..."
                ),
                "success": False,
            }

        job = None
        if user_id:
            job = session.create("Job", {
                "user_id": user_id,
                "status": "running",
                "data": json.dumps({
                    "description": "Download review media"
                })
            })
            session.commit()

        success = False
        try:
            self._download_components(
                session,
                components,
                asset_versions_by_id,
                assets_by_id,
                job,
            )
            success = True
            return {
                "message": "Review media downloaded successfully...",
                "success": True,
            }

        except Exception:
            return {
                "message": "Failed to download review media...",
                "success": False
            }

        finally:
            session.recorded_operations.clear()
            if job["status"] == "running":
                job["status"] = "done" if success else "failed"
                session.commit()

    def _download_components(
        self, session, components, asset_versions_by_id, assets_by_id, job
    ):
        download_dir = user_downloads_dir()
        total_count = len(components)
        for idx, component in enumerate(components):
            job["data"] = json.dumps({
                "description": f"Download review media {idx}/{total_count}"
            })
            session.commit()

            url_item = component["component_locations"][0].get("url")
            if url_item is None:
                continue

            download_url = url_item["value"]

            ext = component["file_type"].lstrip(".")
            asset_version_id = component["version_id"]
            asset_version = asset_versions_by_id[asset_version_id]
            asset_id = asset_version["asset_id"]
            asset = assets_by_id[asset_id]

            version = asset_version["version"]
            asset_name = asset["name"]
            basename = f"{asset_name}_{version:0>3}"

            # Calculate the full download path and URL to pull from
            download_path = os.path.join(
                download_dir, f"{basename}.{ext}"
            )
            if os.path.exists(download_path):
                idx = 1
                while True:
                    filename = f"{basename} ({idx}).{ext}"
                    _download_path = os.path.join(download_dir, filename)
                    if not os.path.exists(_download_path):
                        download_path = _download_path
                        break
                    idx += 1

            urllib.request.urlretrieve(download_url, download_path)

        job["data"] = json.dumps({
            "description": "Download review media finished"
        })
        job["status"] = "done"
        session.commit()

    def _extract_asset_versions(self, session, entities):
        asset_version_ids = set()
        review_session_ids = set()
        for entity in entities:
            entity_type_low = entity.entity_type.lower()
            if entity_type_low == "assetversion":
                asset_version_ids.add(entity["id"])
            elif entity_type_low == "reviewsession":
                review_session_ids.add(entity["id"])

        for version_id in self._get_asset_version_ids_from_review_sessions(
            session, review_session_ids
        ):
            asset_version_ids.add(version_id)

        asset_versions = session.query((
            "select id, version, asset_id from AssetVersion where id in ({})"
        ).format(self.join_query_keys(asset_version_ids))).all()

        return asset_versions

    def _get_asset_version_ids_from_review_sessions(
        self, session, review_session_ids
    ):
        if not review_session_ids:
            return set()
        review_session_objects = session.query((
            "select version_id from ReviewSessionObject"
            " where review_session_id in ({})"
        ).format(self.join_query_keys(review_session_ids))).all()

        return {
            review_session_object["version_id"]
            for review_session_object in review_session_objects
        }
