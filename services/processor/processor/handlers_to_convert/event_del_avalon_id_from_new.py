from ftrack_common import BaseEventHandler, CUST_ATTR_KEY_SERVER_ID

from ayon_ftrack.event_handlers_server.event_sync_to_avalon import (
    SyncToAvalonEvent
)


class DelAvalonIdFromNew(BaseEventHandler):
    '''
    This event removes AvalonId from custom attributes of new entities
    Result:
    - 'Copy->Pasted' entities won't have same AvalonID as source entity

    Priority of this event must be less than SyncToAvalon event
    '''
    priority = SyncToAvalonEvent.priority - 1
    ignore_me = True

    def launch(self, session, event):
        created = []
        entities = event['data']['entities']
        for entity in entities:
            try:
                entity_id = entity['entityId']

                if entity.get('action', None) == 'add':
                    id_dict = entity['changes']['id']

                    if id_dict['new'] is not None and id_dict['old'] is None:
                        created.append(id_dict['new'])

                elif (
                    entity.get('action', None) == 'update' and
                    CUST_ATTR_KEY_SERVER_ID in entity['keys'] and
                    entity_id in created
                ):
                    ftrack_entity = session.get(
                        self._get_entity_type(entity),
                        entity_id
                    )

                    cust_attrs = ftrack_entity["custom_attributes"]
                    if cust_attrs[CUST_ATTR_KEY_SERVER_ID]:
                        cust_attrs[CUST_ATTR_KEY_SERVER_ID] = ""
                        session.commit()

            except Exception:
                session.rollback()
                continue
