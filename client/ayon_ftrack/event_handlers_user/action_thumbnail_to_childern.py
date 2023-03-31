import json
from ayon_ftrack.common import BaseAction
from ayon_ftrack.lib import statics_icon


class ThumbToChildren(BaseAction):
    identifier = "thumb.to.children"
    label = "Thumbnail"
    variant = " to Children"
    icon = statics_icon("ftrack", "action_icons", "Thumbnail.svg")

    def discover(self, session, entities, event):
        """Show only on project."""
        if entities and entities[0].entity_type != "Project":
            return True
        return False

    def launch(self, session, entities, event):
        user_id = event["source"]["user"]["id"]
        user = session.query(f"User where id is {user_id}").one()

        job = session.create("Job", {
            "user": user,
            "status": "running",
            "data": json.dumps({
                "description": "Push thumbnails to children"
            })
        })
        session.commit()
        try:
            for entity in entities:
                thumbnail_id = entity["thumbnail_id"]
                if thumbnail_id:
                    for child in entity["children"]:
                        child["thumbnail_id"] = thumbnail_id

            # inform the user that the job is done
            job["status"] = "done"
        except Exception as exc:
            session.rollback()
            # fail the job if something goes wrong
            job["status"] = "failed"
            raise exc
        finally:
            session.commit()

        return {
            "success": True,
            "message": "Created job for updating thumbnails!"
        }


def register(session):
    ThumbToChildren(session).register()
