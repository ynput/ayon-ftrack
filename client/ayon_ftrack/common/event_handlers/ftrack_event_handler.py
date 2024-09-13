from .ftrack_base_handler import BaseHandler


class BaseEventHandler(BaseHandler):
    """Event handler listening to topics.

    Output of callback is not handled and handler is not designed for actions.

    By default is listening to "ftrack.update". To change it override
    'register' method of change 'subscription_topic' attribute.
    """
    __ignore_handler_class = True

    subscription_topic = "ftrack.update"
    handler_type = "Event"

    def register(self):
        """Register to subscription topic."""

        self.session.event_hub.subscribe(
            "topic={}".format(self.subscription_topic),
            self._process,
            priority=self.priority
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
            self.log.error(
                "Event \"{}\" Failed: {}".format(
                    self.__class__.__name__, str(exc)
                ),
                exc_info=True
            )
            self.session.rollback()
            self.session._configure_locations()

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
