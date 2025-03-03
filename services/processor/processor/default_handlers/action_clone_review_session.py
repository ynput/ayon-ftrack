import json

from ftrack_common.event_handlers import ServerAction


def clone_review_session(session, entity):
    # Create a client review with timestamp.
    name = entity["name"]
    review_session = session.create(
        "ReviewSession",
        {
            "name": f"Clone of {name}",
            "project": entity["project"]
        }
    )

    # Add all invitees.
    for invitee in entity["review_session_invitees"]:
        # Make sure email is not None but string
        email = invitee["email"] or ""
        session.create(
            "ReviewSessionInvitee",
            {
                "name": invitee["name"],
                "email": email,
                "review_session": review_session
            }
        )

    # Add all objects to new review session.
    for obj in entity["review_session_objects"]:
        session.create(
            "ReviewSessionObject",
            {
                "name": obj["name"],
                "version": obj["version"],
                "review_session": review_session,
                "asset_version": obj["asset_version"]
            }
        )

    session.commit()


class CloneReviewSession(ServerAction):
    """Generate Client Review action."""
    label = "Clone Review Session (AYON)"
    variant = None
    identifier = "ayon.clone-review-session"
    description = None
    settings_key = "clone_review_session"

    def discover(self, session, entities, event):
        is_valid = (
            len(entities) == 1
            and entities[0].entity_type == "ReviewSession"
        )
        if is_valid:
            is_valid = self.valid_roles(session, entities, event)
        return is_valid

    def launch(self, session, entities, event):
        userId = event['source']['user']['id']
        user = session.query('User where id is ' + userId).one()
        job = session.create(
            'Job',
            {
                'user': user,
                'status': 'running',
                'data': json.dumps({
                    'description': 'Cloning Review Session.'
                })
            }
        )
        session.commit()

        try:
            clone_review_session(session, entities[0])

            job['status'] = 'done'
            session.commit()
        except Exception:
            session.rollback()
            job["status"] = "failed"
            session.commit()
            self.log.error(
                "Cloning review session failed ({})", exc_info=True
            )

        return {
            'success': True,
            'message': 'Action completed successfully'
        }
