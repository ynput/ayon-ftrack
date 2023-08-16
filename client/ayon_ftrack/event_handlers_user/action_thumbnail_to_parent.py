import json
from ayon_ftrack.common import LocalAction
from ayon_ftrack.lib import get_ftrack_icon_url


class ThumbToParent(LocalAction):
    identifier = "thumb.to.parent"
    label = "Thumbnail"
    variant = " to Parent"
    icon = get_ftrack_icon_url("Thumbnail.svg")

    def discover(self, session, entities, event):
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
                "description": "Push thumbnails to parents"
            })
        })
        session.commit()
        try:
            # TODO we should probably crash nicely and don't crash on first
            #   issue but create a report of all entities with issues
            for entity in entities:
                if entity.entity_type.lower() == "assetversion":
                    parent = entity["task"]
                    if parent is None:
                        par_ent = entity["link"][-2]
                        parent = session.query(
                            "select thumbnail_id from TypedContext"
                            f" where id is \"{par_ent['id']}\""
                        ).first()
                else:
                    try:
                        parent = entity["parent"]
                    except Exception as exc:
                        msg = (
                            "During Action 'Thumbnail to Parent'"
                            " went something wrong"
                        )
                        self.log.error(msg)
                        raise exc

                thumbnail_id = entity["thumbnail_id"]
                if not parent or not thumbnail_id:
                    raise Exception(
                        "Parent or thumbnail id not found. Parent: {}. "
                        "Thumbnail id: {}".format(parent, thumbnail_id)
                    )
                parent["thumbnail_id"] = thumbnail_id

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
    ThumbToParent(session).register()
