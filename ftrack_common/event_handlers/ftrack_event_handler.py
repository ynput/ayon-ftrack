import functools
from .ftrack_base_handler import BaseHandler


class BaseEventHandler(BaseHandler):
    """Event handler listening to topics.

    Output of callback is not handled and handler is not designed for actions.

    By default is listening to "ftrack.update". To change it override
    'register' method of change 'subscription_topic' attribute.
    """

    subscription_topic = "ftrack.update"
    handler_type = "Event"

    def get_project_name_from_event(self, session, event, project_id):
        """Load or query and fill project entity from/to event data.

        Project data are stored by ftrack id because in most cases it is
        easier to access project id than project name.

        Args:
            session (ftrack_api.Session): Current session.
            event (ftrack_api.Event): Processed event by session.
            project_id (str): Ftrack project id.

        Returns:
            Union[str, None]: Project name based on entities or None if project
                cannot be defined.
        """

        if not project_id:
            raise ValueError(
                "Entered `project_id` is not valid. {} ({})".format(
                    str(project_id), str(type(project_id))
                )
            )
        # Try to get project entity from event
        project_data = event["data"].get("project_data")
        if not project_data:
            project_data = {}
            event["data"]["project_data"] = project_data

        project_name = project_data.get(project_id)
        if not project_name:
            # Get project entity from task and store to event
            project_entity = session.query((
                "select full_name from Project where id is \"{}\""
            ).format(project_id)).first()
            if project_entity:
                project_name = project_entity["full_name"]
            event["data"]["project_data"][project_id] = project_name
        return project_name

    def _process(self, event):
        return self._launch(event)

    def _launch(self, event):
        """Callback kept for backwards compatibility.

        Will be removed when default
        """

        self.session.rollback()
        self.session._local_cache.clear()

        try:
            self.process(event)

        except Exception as exc:
            self.session.rollback()
            self.session._configure_locations()
            self.log.error(
                "Event \"{}\" Failed: {}".format(
                    self.__class__.__name__, str(exc)
                ),
                exc_info=True
            )

    def register(self):
        """Register to subscription topic."""

        self.session.event_hub.subscribe(
            "topic={}".format(self.subscription_topic),
            self._process,
            priority=self.priority
        )

    def _translate_event(self, event, session=None):
        """Receive entity objects based on event.

        Args:
            event (ftrack_api.Event): Event to process.
            session (ftrack_api.Session): Connected ftrack session.

        Returns:
            List[ftrack_api.Entity]: Queried entities based on event data.
        """

        return self._get_entities(
            event,
            session,
            ignore=["socialfeed", "socialnotification", "team"]
        )

    def process(self, event):
        """Callback triggered on event with matching topic.

        Args:
            session (ftrack_api.Session): Ftrack session which triggered
                the event.
            event (ftrack_api.Event): Ftrack event to process.
        """

        return self.launch(self.session, event)


    def launch(self, session, event):
        """Deprecated method used for backwards compatibility.

        Override 'process' method rather then 'launch'. Method name 'launch'
        is derived from action event handler which does not make sense in terms
        of not action based processing.

        Args:
            session (ftrack_api.Session): Ftrack session which triggered
                the event.
            event (ftrack_api.Event): Ftrack event to process.
        """

        raise NotImplementedError()