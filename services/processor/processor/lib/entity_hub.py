import copy
import collections
from abc import ABCMeta, abstractmethod, abstractproperty

import six
from openpype_api import get_server_api_connection
from openpype_api.graphql import GraphQlQueryFailed
from openpype_api.utils import create_entity_id

from nxtools import slugify

UNKNOWN_VALUE = object()
PROJECT_PARENT_ID = object()
_NOT_SET = object()


def slugify_name(name, separator="_"):
    """Prepare name ready for server regex.

    This function should probably be part of python api.

    Function should expect possible characters as that may change over time on
    server. The separator should be received from server.
    """

    return slugify(name, separator)


class EntityHub:
    def __init__(
        self, project_name, connection=None, allow_data_changes=False
    ):
        if not connection:
            connection = get_server_api_connection()
        self._connection = connection

        self._project_name = project_name
        self._entities_by_id = {}
        self._entities_by_parent_id = collections.defaultdict(list)
        self._project_entity = UNKNOWN_VALUE

        self._allow_data_changes = allow_data_changes

    @property
    def allow_data_changes(self):
        return self._allow_data_changes

    @property
    def project_name(self):
        return self._project_name

    @property
    def project_entity(self):
        if self._project_entity is UNKNOWN_VALUE:
            self.fill_project_from_server()
        return self._project_entity

    def get_attributes_for_type(self, entity_type):
        return self._connection.get_attributes_for_type(entity_type)

    def get_entity_by_id(self, entity_id):
        """Receive entity by its id without entity type.

        The entity must be already existing in cached objects.

        Args:
            entity_id (str): Id of entity.

        Returns:
            Union[BaseEntity, None]: Entity object or None.
        """

        return self._entities_by_id.get(entity_id)

    def get_folder_by_id(self, entity_id, allow_query=True):
        if allow_query:
            return self.get_or_query_entity_by_id(entity_id, ["folder"])
        return self._entities_by_id.get(entity_id)

    def get_task_by_id(self, entity_id, allow_query=True):
        if allow_query:
            return self.get_or_query_entity_by_id(entity_id, ["task"])
        return self._entities_by_id.get(entity_id)

    def get_or_query_entity_by_id(self, entity_id, entity_types):
        existing_entity = self._entities_by_id.get(entity_id)
        if existing_entity is None:
            return existing_entity

        if not entity_types:
            return None

        entity_data = None
        for entity_type in entity_types:
            if entity_type == "folder":
                entity_data = self._connection.get_folder_by_id(
                    self.project_name,
                    entity_id,
                    fields=self._get_folder_fields()
                )
            elif entity_type == "task":
                entity_data = self._connection.get_task_by_id(
                    self.project_name, entity_id
                )
            else:
                raise ValueError(
                    "Unknonwn entity type \"{}\"".format(entity_type)
                )

            if entity_data:
                break

        if not entity_data:
            return None

        if entity_type == "folder":
            return self.add_folder(entity_data)
        elif entity_type == "task":
            return self.add_task(entity_data)

        return None

    @property
    def entities(self):
        for entity in self._entities_by_id.values():
            yield entity

    def add_new_folder(self, *args, created=True, **kwargs):
        """Create folder object and add it to entity hub.

        Args:
            parent (Union[ProjectEntity, FolderEntity]): Parent of added
                folder.

        Returns:
            FolderEntity: Added folder entity.
        """

        folder_entity = FolderEntity(
            *args, **kwargs, created=created, entity_hub=self
        )
        self.add_entity(folder_entity)
        return folder_entity

    def add_new_task(self, *args, created=True, **kwargs):
        task_entity = TaskEntity(
            *args, **kwargs, created=created, entity_hub=self
        )
        self.add_entity(task_entity)
        return task_entity

    def add_folder(self, folder):
        """Create folder object and add it to entity hub.

        Args:
            parent (Union[ProjectEntity, FolderEntity]): Parent of added
                folder.

        Returns:
            FolderEntity: Added folder entity.
        """

        folder_entity = FolderEntity.from_entity_data(folder, entity_hub=self)
        self.add_entity(folder_entity)
        return folder_entity

    def add_task(self, task):
        task_entity = TaskEntity.from_entity_data(task, entity_hub=self)
        self.add_entity(task_entity)
        return task_entity

    def add_entity(self, entity):
        self._entities_by_id[entity.id] = entity
        parent_children = self._entities_by_parent_id[entity.parent_id]
        if entity not in parent_children:
            parent_children.append(entity)

        if entity.parent_id is PROJECT_PARENT_ID:
            return

        parent = self._entities_by_id.get(entity.parent_id)
        if parent is not None:
            parent.add_child(entity.id)

    def unset_entity_parent(self, entity_id, parent_id):
        entity = self._entities_by_id.get(entity_id)
        parent = self._entities_by_id.get(parent_id)
        children_ids = UNKNOWN_VALUE
        if parent is not None:
            children_ids = parent.get_children_ids(False)

        has_set_parent = False
        if entity is not None:
            has_set_parent = entity.parent_id == parent_id

        new_parent_id = None
        if has_set_parent:
            entity.parent_id = new_parent_id

        if children_ids is not UNKNOWN_VALUE and entity_id in children_ids:
            parent.remove_child(entity_id)

        if entity is None or not has_set_parent:
            self.reset_immutable_for_hierarchy_cache(parent_id)
            return

        orig_parent_children = self._entities_by_parent_id[parent_id]
        if entity in orig_parent_children:
            orig_parent_children.remove(entity)

        new_parent_children = self._entities_by_parent_id[new_parent_id]
        if entity not in new_parent_children:
            new_parent_children.append(entity)
        self.reset_immutable_for_hierarchy_cache(parent_id)

    def set_entity_parent(self, entity_id, parent_id, orig_parent_id=_NOT_SET):
        parent = self._entities_by_id.get(parent_id)
        entity = self._entities_by_id.get(entity_id)
        if entity is None:
            if parent is not None:
                children_ids = parent.get_children_ids(False)
                if (
                    children_ids is not UNKNOWN_VALUE
                    and entity_id in children_ids
                ):
                    parent.remove_child(entity_id)
                self.reset_immutable_for_hierarchy_cache(parent.id)
            return

        if orig_parent_id is _NOT_SET:
            orig_parent_id = entity.parent_id
            if orig_parent_id == parent_id:
                return

        orig_parent_children = self._entities_by_parent_id[orig_parent_id]
        if entity in orig_parent_children:
            orig_parent_children.remove(entity)
        self.reset_immutable_for_hierarchy_cache(orig_parent_id)

        orig_parent = self._entities_by_id.get(orig_parent_id)
        if orig_parent is not None:
            orig_parent.remove_child(entity_id)

        parent_children = self._entities_by_parent_id[parent_id]
        if entity not in parent_children:
            parent_children.append(entity)

        entity.parent_id = parent_id
        if parent is None or parent.get_children_ids(False) is UNKNOWN_VALUE:
            return

        parent.add_child(entity_id)
        self.reset_immutable_for_hierarchy_cache(parent_id)

    def _query_entity_children(self, entity):
        folder_fields = self._get_folder_fields()
        tasks = []
        folders = []
        if entity.entity_type == "project":
            folders = list(self._connection.get_folders(
                entity["name"],
                parent_ids=[entity.id],
                fields=folder_fields
            ))

        elif entity.entity_type == "folder":
            folders = list(self._connection.get_folders(
                self._project_entity["name"],
                parent_ids=[entity.id],
                fields=folder_fields
            ))
            tasks = list(self._connection.get_tasks(
                self._project_entity["name"],
                parent_ids=[entity.id]
            ))

        children_ids = {
            child.id
            for child in self._entities_by_parent_id[entity.id]
        }
        for folder in folders:
            folder_entity = self._entities_by_id.get(folder["id"])
            if folder_entity is not None:
                if folder_entity.parent_id == entity.id:
                    children_ids.add(folder_entity.id)
                continue

            folder_entity = self.add_folder(folder)
            children_ids.add(folder_entity.id)

        for task in tasks:
            task_entity = self._entities_by_id.get(task["id"])
            if task_entity is not None:
                if task_entity.parent_id == entity.id:
                    children_ids.add(task_entity.id)
                continue

            task_entity = self.add_task(task)
            children_ids.add(task_entity.id)

        entity.fill_children_ids(children_ids)

    def get_entity_children(self, entity, allow_query=True):
        children_ids = entity.get_children_ids(allow_query=False)
        if children_ids is not UNKNOWN_VALUE:
            return entity.get_children()

        if children_ids is UNKNOWN_VALUE and not allow_query:
            return UNKNOWN_VALUE

        self._query_entity_children(entity)

        return entity.get_children()

    def delete_entity(self, entity):
        parent_id = entity.parent_id
        if parent_id is None:
            return

        parent = self._entities_by_parent_id.get(parent_id)
        if parent is not None:
            parent.remove_child(entity.id)

    def reset_immutable_for_hierarchy_cache(
        self, entity_id, bottom_to_top=True
    ):
        if bottom_to_top is None or entity_id is None:
            return

        reset_queue = collections.deque()
        reset_queue.append(entity_id)
        if bottom_to_top:
            while reset_queue:
                entity_id = reset_queue.popleft()
                entity = self.get_entity_by_id(entity_id)
                if entity is None:
                    continue
                entity.reset_immutable_for_hierarchy_cache(None)
                reset_queue.append(entity.parent_id)
        else:
            while reset_queue:
                entity_id = reset_queue.popleft()
                entity = self.get_entity_by_id(entity_id)
                if entity is None:
                    continue
                entity.reset_immutable_for_hierarchy_cache(None)
                for child in self._entities_by_parent_id[entity.id]:
                    reset_queue.append(child.id)

    def fill_project_from_server(self):
        project_name = self.project_name
        project = self._connection.get_project(
            project_name,
            own_attributes=True
        )
        if not project:
            raise ValueError(
                "Project \"{}\" was not found.".format(project_name)
            )

        self._project_entity = ProjectEntity(
            project["code"],
            parent_id=PROJECT_PARENT_ID,
            entity_id=project["name"],
            library=project["library"],
            folder_types=project["folderTypes"],
            task_types=project["taskTypes"],
            name=project["name"],
            attribs=project["ownAttrib"],
            data=project["data"],
            active=project["active"],
            entity_hub=self
        )
        self.add_entity(self._project_entity)
        return self._project_entity

    def _get_folder_fields(self):
        folder_fields = set(
            self._connection.get_default_fields_for_type("folder")
        )
        folder_fields.add("hasSubsets")
        if self._allow_data_changes:
            folder_fields.add("data")
        return folder_fields

    def query_entities_from_server(self):
        project_entity = self.fill_project_from_server()

        folder_fields = self._get_folder_fields()

        folders = self._connection.get_folders(
            project_entity.name,
            fields=folder_fields,
            own_attributes=True
        )
        tasks = self._connection.get_tasks(
            project_entity.name,
            own_attributes=True
        )
        folders_by_parent_id = collections.defaultdict(list)
        try:
            for folder in folders:
                parent_id = folder["parentId"]
                folders_by_parent_id[parent_id].append(folder)
        except GraphQlQueryFailed as exc:
            print(exc.query)
            raise

        tasks_by_parent_id = collections.defaultdict(list)
        for task in tasks:
            parent_id = task["folderId"]
            tasks_by_parent_id[parent_id].append(task)

        hierarchy_queue = collections.deque()
        hierarchy_queue.append((None, project_entity))
        while hierarchy_queue:
            item = hierarchy_queue.popleft()
            parent_id, parent_entity = item

            children_ids = set()
            for folder in folders_by_parent_id[parent_id]:
                folder_entity = self.add_folder(folder)
                children_ids.add(folder_entity.id)
                folder_entity.has_published_content = folder["hasSubsets"]
                hierarchy_queue.append((folder_entity.id, folder_entity))

            for task in tasks_by_parent_id[parent_id]:
                task_entity = self.add_task(task)
                children_ids.add(task_entity.id)

            parent_entity.fill_children_ids(children_ids)
        self.lock()

    def lock(self):
        if self._project_entity is None:
            return

        for entity in self._entities_by_id.values():
            entity.lock()

    def _get_top_entities(self):
        all_ids = set(self._entities_by_id.keys())
        return [
            entity
            for entity in self._entities_by_id.values()
            if entity.parent_id not in all_ids
        ]

    def _split_entities(self):
        top_entities = self._get_top_entities()
        entities_queue = collections.deque(top_entities)
        removed_entity_ids = []
        created_entity_ids = []
        other_entity_ids = []
        while entities_queue:
            entity = entities_queue.popleft()
            removed = entity.removed
            if removed:
                removed_entity_ids.append(entity.id)
            elif entity.created:
                created_entity_ids.append(entity.id)
            else:
                other_entity_ids.append(entity.id)

            for child in tuple(self._entities_by_parent_id[entity.id]):
                if removed:
                    self.unset_entity_parent(child.id, entity.id)
                entities_queue.append(child)
        return created_entity_ids, other_entity_ids, removed_entity_ids

    def _get_update_body(self, entity, changes=None):
        if changes is None:
            changes = entity.changes

        if not changes:
            return None
        return {
            "type": "update",
            "entityType": entity.entity_type,
            "entityId": entity.id,
            "data": changes
        }

    def _get_create_body(self, entity):
        return {
            "type": "create",
            "entityType": entity.entity_type,
            "entityId": entity.id,
            "data": entity.to_create_body_data()
        }

    def _get_delete_body(self, entity):
        return {
            "type": "delete",
            "entityType": entity.entity_type,
            "entityId": entity.id
        }

    def commit_changes(self):
        # TODO use Operations Session instead of known operations body
        # TODO have option to commit changes out of hierarchy
        operations_body = []

        created_entity_ids, other_entity_ids, removed_entity_ids = (
            self._split_entities()
        )
        processed_ids = set()
        for entity_id in other_entity_ids:
            if entity_id in processed_ids:
                continue

            entity = self._entities_by_id[entity_id]
            changes = entity.changes
            processed_ids.add(entity_id)
            if not changes:
                continue

            if entity.entity_type == "project":
                response = self._connection.patch(
                    "projects/{}".format(self.project_name),
                    **changes
                )
                if response.status_code != 204:
                    raise ValueError("Failed to update project")
                continue

            bodies = [self._get_update_body(entity, changes)]
            # Parent was created and was not yet added to operations body
            parent_queue = collections.deque()
            parent_queue.append(entity.parent_id)
            while parent_queue:
                # Make sure entity's parents are created
                parent_id = parent_queue.popleft()
                if (
                    parent_id is UNKNOWN_VALUE
                    or parent_id in processed_ids
                    or parent_id not in created_entity_ids
                ):
                    continue

                parent = self._entities_by_id.get(parent_id)
                processed_ids.add(parent.id)
                bodies.append(self._get_create_body(parent))
                parent_queue.append(parent.id)

            operations_body.extend(reversed(bodies))

        for entity_id in created_entity_ids:
            if entity_id in processed_ids:
                continue
            entity = self._entities_by_id[entity_id]
            operations_body.append(self._get_create_body(entity))

        for entity_id in reversed(removed_entity_ids):
            if entity_id in processed_ids:
                continue

            parent_children = self._entities_by_parent_id[entity.parent_id]
            if entity in parent_children:
                parent_children.remove(entity)

            entity = self._entities_by_id.pop(entity_id)
            if not entity.created:
                operations_body.append(self._get_delete_body(entity))

        self._connection.send_batch_operations(
            self.project_name, operations_body
        )

        self.lock()


class AttributeValue(object):
    def __init__(self, value):
        self._value = value
        self._origin_value = copy.deepcopy(value)

    def get_value(self):
        return self._value

    def set_value(self, value):
        self._value = value

    value = property(get_value, set_value)

    @property
    def changed(self):
        return self._value != self._origin_value

    def lock(self):
        self._origin_value = copy.deepcopy(self._value)


class Attributes(object):
    """Object representing attribs of entity.

    Todos:
        This could be enhanced to know attribute schema and validate values
        based on the schema.

    Args:
        attrib_keys (Iterable[str]): Keys that are available in attribs of the
            entity.
        values (Union[None, Dict[str, Any]]): Values of attributes.
    """

    def __init__(self, attrib_keys, values=UNKNOWN_VALUE):
        if values in (UNKNOWN_VALUE, None):
            values = {}
        self._attributes = {
            key: AttributeValue(values.get(key))
            for key in attrib_keys
        }

    def __contains__(self, key):
        return key in self._attributes

    def __getitem__(self, key):
        return self._attributes[key].value

    def __setitem__(self, key, value):
        self._attributes[key].set_value(value)

    def __iter__(self):
        for key in self._attributes:
            yield key

    def keys(self):
        return self._attributes.keys()

    def values(self):
        for attribute in self._attributes.values():
            yield attribute.value

    def items(self):
        for key, attribute in self._attributes.items():
            yield key, attribute.value

    def get(self, key, default=None):
        """Get value of attribute.

        Args:
            key (str): Attribute name.
            default (Any): Default value to return when attribute was not
                found.
        """

        attribute = self._attributes.get(key)
        if attribute is None:
            return default
        return attribute.value

    def set(self, key, value):
        """Change value of attribute.

        Args:
            key (str): Attribute name.
            value (Any): New value of the attribute.
        """

        self[key] = value

    def get_attribute(self, key):
        """Access to attribute object.

        Args:
            key (str): Name of attribute.

        Returns:
            AttributeValue: Object of attribute value.

        Raises:
            KeyError: When attribute is not available.
        """

        return self._attributes[key]

    def lock(self):
        for attribute in self._attributes.values():
            attribute.lock()

    @property
    def changes(self):
        """Attribute value changes.

        Returns:
            Dict[str, Any]: Key mapping with new values.
        """

        return {
            attr_key: attribute.value
            for attr_key, attribute in self._attributes.items()
            if attribute.changed
        }

    def to_dict(self, ignore_none=True):
        output = {}
        for key, value in self.items():
            if (
                value is UNKNOWN_VALUE
                or (ignore_none and value is None)
            ):
                continue

            output[key] = value
        return output


@six.add_metaclass(ABCMeta)
class BaseEntity(object):
    """Object representation of entity from server which is capturing changes.

    All data on created object are expected as "current data" on server entity
    unless the entity has set 'created' to 'True'. So if new data should be
    stored to server entity then fill entity with server data first and
    then change them.

    Calling 'lock' method will mark entity as "saved" and all changes made on
    entity are set as "current data" on server.

    Args:
        name (str): Name of entity.
        attribs (Dict[str, Any]): Attribute values.
        data (Dict[str, Any]): Entity data (custom data).
        parent_id (Union[str, None]): Id of parent entity.
        entity_id (Union[str, None]): Id of the entity. New id is created if
            not passed.
        thumbnail_id (Union[str, None]): Id of entity's thumbnail.
        active (bool): Is entity active.
        entity_hub (EntityHub): Object of entity hub which created object of
            the entity.
        created (Union[bool, None]): Entity is new. When 'None' is passed the
            value is defined based on value of 'entity_id'.
    """

    def __init__(
        self,
        entity_id,
        parent_id=UNKNOWN_VALUE,
        name=UNKNOWN_VALUE,
        attribs=UNKNOWN_VALUE,
        data=UNKNOWN_VALUE,
        thumbnail_id=UNKNOWN_VALUE,
        active=UNKNOWN_VALUE,
        entity_hub=None,
        created=None
    ):
        if entity_hub is None:
            raise ValueError("Missing required kwarg 'entity_hub'")

        self._entity_hub = entity_hub

        if created is None:
            created = entity_id is None

        if entity_id is None:
            entity_id = create_entity_id()

        if data is None:
            data = {}

        children_ids = UNKNOWN_VALUE
        if created:
            children_ids = set()

        if not created and parent_id is UNKNOWN_VALUE:
            raise ValueError("Existing entity is missing parent id.")

        # These are public without any validation at this moment
        #   may change in future (e.g. name will have regex validation)
        self._entity_id = entity_id

        self._parent_id = parent_id
        self._name = name
        self.active = active
        self._created = created
        self._thumbnail_id = thumbnail_id
        self._attribs = Attributes(
            self._get_attributes_for_type(self.entity_type),
            attribs
        )
        self._data = data
        self._children_ids = children_ids

        self._orig_parent_id = parent_id
        self._orig_name = name
        self._orig_data = copy.deepcopy(data)
        self._orig_thumbnail_id = thumbnail_id
        self._orig_active = active

        self._immutable_for_hierarchy_cache = None

    def __repr__(self):
        return "<{} - {}>".format(self.__class__.__name__, self.id)

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item, value):
        return setattr(self, item, value)

    @property
    def id(self):
        """Access to entity id under which is entity available on server.

        Returns:
            str: Entity id.
        """

        return self._entity_id

    @property
    def removed(self):
        return self._parent_id is None

    @property
    def orig_parent_id(self):
        return self._orig_parent_id

    @property
    def attribs(self):
        """Entity attributes based on server configuration.

        Returns:
            Attributes: Attributes object handling changes and values of
                attributes on entity.
        """

        return self._attribs

    @property
    def data(self):
        """Entity custom data that are not stored by any deterministic model.

        Be aware that 'data' can't be queried using GraphQl and cannot be
            updated partially.

        Returns:
            Dict[str, Any]: Custom data on entity.
        """

        return self._data

    @property
    def project_name(self):
        """Quick access to project from entity hub.

        Returns:
            str: Name of project under which entity lives.
        """

        return self._entity_hub.project_name

    @abstractproperty
    def entity_type(self):
        """Entity type coresponding to server.

        Returns:
            Literal[project, folder, task]: Entity type.
        """

        pass

    @abstractproperty
    def parent_entity_types(self):
        """Entity type coresponding to server.

        Returns:
            Iterable[str]: Possible entity types of parent.
        """

        pass

    @abstractproperty
    def changes(self):
        """Receive entity changes.

        Returns:
            Union[Dict[str, Any], None]: All values that have changed on
                entity. New entity must return None.
        """

        pass

    @classmethod
    @abstractmethod
    def from_entity_data(cls, entity_data, entity_hub):
        """Create entity based on queried data from server.

        Args:
            entity_data (Dict[str, Any]): Entity data from server.
            entity_hub (EntityHub): Hub which handle the entity.

        Returns:
            BaseEntity: Object of the class.
        """

        pass

    @abstractmethod
    def to_create_body_data(self):
        """Convert object of entity to data for server on creation.

        Returns:
            Dict[str, Any]: Entity data.
        """

        pass

    @property
    def immutable_for_hierarchy(self):
        """Entity is immutable for hierarchy changes.

        Hierarchy changes can be considered as change of name or parents.

        Returns:
            bool: Entity is immutable for hierarchy changes.
        """

        if self._immutable_for_hierarchy_cache is not None:
            return self._immutable_for_hierarchy_cache

        immutable_for_hierarchy = self._immutable_for_hierarchy
        if immutable_for_hierarchy is not None:
            self._immutable_for_hierarchy_cache = immutable_for_hierarchy
            return self._immutable_for_hierarchy_cache

        for child in self._entity_hub.get_entity_children(self):
            if child.immutable_for_hierarchy:
                self._immutable_for_hierarchy_cache = True
                return self._immutable_for_hierarchy_cache

        self._immutable_for_hierarchy_cache = False
        return self._immutable_for_hierarchy_cache

    @property
    def _immutable_for_hierarchy(self):
        """Override this method to define if entity object is immutable.

        This property was added to define immutable state of Folder entities
        which is used in property 'immutable_for_hierarchy'.

        Returns:
            Union[bool, None]: Bool to explicitly telling if is immutable or
                not otherwise None.
        """

        return None

    @property
    def has_cached_immutable_hierarchy(self):
        return self._immutable_for_hierarchy_cache is not None

    def reset_immutable_for_hierarchy_cache(self, bottom_to_top=True):
        """Clear cache of immutable hierarchy property.

        This is used when entity changed parent or a child was added.

        Args:
            bottom_to_top (bool): Reset cache from top hierarchy to bottom or
                from bottom hierarchy to top.
        """

        self._immutable_for_hierarchy_cache = None
        self._entity_hub.reset_immutable_for_hierarchy_cache(
            self.id, bottom_to_top
        )

    def _get_default_changes(self):
        """Collect changes of common data on entity.

        Returns:
            Dict[str, Any]: Changes on entity. Key and it's new value.
        """

        changes = {}
        if self._orig_name != self._name:
            changes["name"] = self._name

        if self._entity_hub.allow_data_changes:
            if self._orig_data != self._data:
                changes["data"] = self._data

        if self._orig_thumbnail_id != self._thumbnail_id:
            changes["thumbnailId"] = self._thumbnail_id

        if self._orig_active != self.active:
            changes["active"] = self.active

        attrib_changes = self.attribs.changes
        for name, value in attrib_changes.items():
            key = "attrib.{}".format(name)
            changes[key] = value
        return changes

    def _get_attributes_for_type(self, entity_type):
        return self._entity_hub.get_attributes_for_type(entity_type)

    def lock(self):
        """Lock entity as 'saved' so all changes are discarded."""

        self._orig_parent_id = self._parent_id
        self._orig_name = self._name
        self._orig_data = copy.deepcopy(self._data)
        self._orig_thumbnail_id = self.thumbnail_id
        self._attribs.lock()

        self._immutable_for_hierarchy_cache = None

    def _get_entity_by_id(self, entity_id):
        return self._entity_hub.get_entity_by_id(entity_id)

    def get_name(self):
        return self._name

    def set_name(self, name):
        self._name = name

    name = property(get_name, set_name)

    def get_parent_id(self):
        """Parent entity id.

        Returns:
            Union[str, None]: Id of parent entity or none if is not set.
        """

        return self._parent_id

    def set_parent_id(self, parent_id):
        """Change parent by id.

        Args:
            parent_id (Union[str, None]): Id of new parent for entity.

        Raises:
            ValueError: If parent was not found by id.
            TypeError: If validation of parent does not pass.
        """

        if parent_id != self._parent_id:
            orig_parent_id = self._parent_id
            self._parent_id = parent_id
            self._entity_hub.set_entity_parent(
                self.id, parent_id, orig_parent_id
            )

    parent_id = property(get_parent_id, set_parent_id)

    def get_parent(self, allow_query=True):
        """Parent entity.

        Returns:
            Union[BaseEntity, None]: Parent object.
        """

        parent = self._entity_hub.get_entity_by_id(self._parent_id)
        if parent is not None:
            return parent

        if not allow_query:
            return self._parent_id

        if self._parent_id is UNKNOWN_VALUE:
            return self._parent_id

        return self._entity_hub.get_or_query_entity_by_id(
            self._parent_id, self.parent_entity_types
        )

    def set_parent(self, parent):
        """Change parent object.

        Args:
            parent (BaseEntity): New parent for entity.

        Raises:
            TypeError: If validation of parent does not pass.
        """

        parent_id = None
        if parent is not None:
            parent_id = parent.id
        self._entity_hub.set_entity_parent(self.id, parent_id)

    parent = property(get_parent, set_parent)

    def get_children_ids(self, allow_query=True):
        """Access to children objects.

        Todos:
            Children should be maybe handled by EntityHub instead of entities
                themselves. That would simplify 'set_entity_parent',
                'unset_entity_parent' and other logic related to changing
                hierarchy.

        Returns:
            Union[List[str], Type[UNKNOWN_VALUE]]: Children iterator.
        """

        if self._children_ids is UNKNOWN_VALUE:
            if not allow_query:
                return self._children_ids
            self._entity_hub.get_entity_children(self, True)
        return set(self._children_ids)

    children_ids = property(get_children_ids)

    def get_children(self, allow_query=True):
        """Access to children objects.

        Returns:
            Union[List[BaseEntity], Type[UNKNOWN_VALUE]]: Children iterator.
        """

        if self._children_ids is UNKNOWN_VALUE:
            if not allow_query:
                return self._children_ids
            return self._entity_hub.get_entity_children(self, True)

        return [
            self._entity_hub.get_entity_by_id(children_id)
            for children_id in self._children_ids
        ]

    children = property(get_children)

    def add_child(self, child):
        """Add child entity.

        Args:
            child (BaseEntity): Child object to add.

        Raises:
            TypeError: When child object has invalid type to be children.
        """

        child_id = child
        if isinstance(child_id, BaseEntity):
            child_id = child.id

        if self._children_ids is not UNKNOWN_VALUE:
            self._children_ids.add(child_id)

        self._entity_hub.set_entity_parent(child_id, self.id)

    def remove_child(self, child):
        """Remove child entity.

        Is ignored if child is not in children.

        Args:
            child (Union[str, BaseEntity]): Child object or child id to remove.
        """

        child_id = child
        if isinstance(child_id, BaseEntity):
            child_id = child.id

        if self._children_ids is not UNKNOWN_VALUE:
            self._children_ids.discard(child_id)
        self._entity_hub.unset_entity_parent(child_id, self.id)

    def get_thumbnail_id(self):
        """Thumbnail id of entity.

        Returns:
            Union[str, None]: Id of parent entity or none if is not set.
        """

        return self._thumbnail_id

    def set_thumbnail_id(self, thumbnail_id):
        """Change thumbnail id.

        Args:
            thumbnail_id (Union[str, None]): Id of thumbnail for entity.
        """

        self._thumbnail_id = thumbnail_id

    thumbnail_id = property(get_thumbnail_id, set_thumbnail_id)

    @property
    def created(self):
        """Entity is new.

        Returns:
            bool: Entity is newly created.
        """

        return self._created

    def fill_children_ids(self, children_ids):
        """Fill children ids on entity.

        Warning:
            This is not an api call but is called from entity hub.
        """

        self._children_ids = set(children_ids)


class ProjectEntity(BaseEntity):
    entity_type = "project"
    parent_entity_types = []
    # TODO These are hardcoded but maybe should be used from server???
    default_folder_type_icon = "folder"
    default_task_type_icon = "task_alt"

    def __init__(
        self, project_code, library, folder_types, task_types, *args, **kwargs
    ):
        super(ProjectEntity, self).__init__(*args, **kwargs)

        self._project_code = project_code
        self._library_project = library
        self._folder_types = folder_types
        self._task_types = task_types

        self._orig_project_code = project_code
        self._orig_library_project = library
        self._orig_folder_types = copy.deepcopy(folder_types)
        self._orig_task_types = copy.deepcopy(task_types)

    def get_parent(self, *args, **kwargs):
        return None

    def set_parent(self, parent):
        raise ValueError(
            "Parent of project cannot be set to {}".format(parent)
        )

    parent = property(get_parent, set_parent)

    def get_folder_types(self):
        return copy.deepcopy(self._folder_types)

    def set_folder_types(self, folder_types):
        new_folder_types = []
        for folder_type in folder_types:
            if "icon" not in folder_type:
                folder_type["icon"] = self.default_folder_type_icon
            new_folder_types.append(folder_type)
        self._folder_types = new_folder_types

    def get_task_types(self):
        return copy.deepcopy(self._task_types)

    def set_task_types(self, task_types):
        new_task_types = []
        for task_type in task_types:
            if "icon" not in task_type:
                task_type["icon"] = self.default_task_type_icon
            new_task_types.append(task_type)
        self._task_types = new_task_types

    folder_types = property(get_folder_types, set_folder_types)
    task_types = property(get_task_types, set_task_types)

    @property
    def changes(self):
        changes = self._get_default_changes()
        if self._orig_folder_types != self._folder_types:
            changes["folderTypes"] = self.get_folder_types()

        if self._orig_task_types != self._task_types:
            changes["taskTypes"] = self.get_task_types()

        return changes

    @classmethod
    def from_entity_data(cls, project, entity_hub):
        return cls(
            project["code"],
            parent_id=PROJECT_PARENT_ID,
            entity_id=project["name"],
            library=project["library"],
            folder_types=project["folderTypes"],
            task_types=project["taskTypes"],
            name=project["name"],
            attribs=project["ownAttrib"],
            data=project["data"],
            active=project["active"],
            entity_hub=entity_hub
        )

    def to_create_body_data(self):
        raise NotImplementedError(
            "ProjectEntity does not support conversion to entity data"
        )


class FolderEntity(BaseEntity):
    entity_type = "folder"
    parent_entity_types = ["folder", "project"]

    def __init__(self, folder_type, *args, label=None, **kwargs):
        super(FolderEntity, self).__init__(*args, **kwargs)

        self._folder_type = folder_type
        self._label = label

        self._orig_folder_type = folder_type
        self._orig_label = label
        # Know if folder has any subsets
        # - is used to know if folder allows hierarchy changes
        self._has_published_content = False

    def get_label(self):
        return self._label

    def set_label(self, label):
        self._label = label

    label = property(get_label, set_label)
    def lock(self):
        super(FolderEntity, self).lock()
        self._orig_folder_type = self._folder_type

    @property
    def changes(self):
        changes = self._get_default_changes()

        if self._orig_parent_id != self._parent_id:
            parent_id = self._parent_id
            if parent_id == self.project_name:
                parent_id = None
            changes["parentId"] = parent_id

        if self._orig_folder_type != self._folder_type:
            changes["folderType"] = self._folder_type

        # label = self._label
        # if self._name == label:
        #     label = None
        #
        # if label != self._orig_label:
        #     changes["label"] = label

        return changes

    @classmethod
    def from_entity_data(cls, folder, entity_hub):
        parent_id = folder["parentId"]
        if parent_id is None:
            parent_id = entity_hub.project_entity.id
        return cls(
            folder["folderType"],
            # label=folder["label"],
            entity_id=folder["id"],
            parent_id=parent_id,
            name=folder["name"],
            data=folder.get("data"),
            attribs=folder["ownAttrib"],
            active=folder["active"],
            thumbnail_id=folder["thumbnailId"],
            created=False,
            entity_hub=entity_hub
        )

    def to_create_body_data(self):
        parent_id = self._parent_id
        if parent_id is UNKNOWN_VALUE:
            raise ValueError("Folder does not have set 'parent_id'")

        if parent_id == self.project_name:
            parent_id = None

        if not self.name or self.name is UNKNOWN_VALUE:
            raise ValueError("Folder does not have set 'name'")

        output = {
            "name": self.name,
            "folderType": self.folder_type,
            "parentId": parent_id,
        }
        attrib = self.attribs.to_dict()
        if attrib:
            output["attrib"] = attrib

        if self.active is not UNKNOWN_VALUE:
            output["active"] = self.active

        if self.thumbnail_id is not UNKNOWN_VALUE:
            output["thumbnailId"] = self.thumbnail_id

        if self._entity_hub.allow_data_changes:
            output["data"] = self._data
        return output

    def get_folder_type(self):
        return self._folder_type

    def set_folder_type(self, folder_type):
        self._folder_type = folder_type

    folder_type = property(get_folder_type, set_folder_type)

    def get_has_published_content(self):
        return self._has_published_content

    def set_has_published_content(self, has_published_content):
        if self._has_published_content is has_published_content:
            return

        self._has_published_content = has_published_content
        # Reset immutable cache of parents
        self._entity_hub.reset_immutable_for_hierarchy_cache(self.id)

    has_published_content = property(
        get_has_published_content, set_has_published_content
    )

    @property
    def _immutable_for_hierarchy(self):
        if self.has_published_content:
            return True
        return None


class TaskEntity(BaseEntity):
    entity_type = "task"
    parent_entity_types = ["folder"]

    def __init__(self, task_type, *args, label=None, **kwargs):
        super(TaskEntity, self).__init__(*args, **kwargs)

        self._task_type = task_type
        self._label = label

        self._orig_task_type = task_type
        self._orig_label = label

        self._children_ids = set()

    def lock(self):
        super(TaskEntity, self).lock()
        self._orig_task_type = self._task_type

    def get_task_type(self):
        return self._task_type

    def set_task_type(self, task_type):
        self._task_type = task_type

    task_type = property(get_task_type, set_task_type)

    def get_label(self):
        return self._label

    def set_label(self, label):
        self._label = label

    label = property(get_label, set_label)

    def add_child(self, child):
        raise ValueError("Task does not support to add children")

    @property
    def changes(self):
        changes = self._get_default_changes()

        if self._orig_parent_id != self._parent_id:
            changes["folderId"] = self._parent_id

        if self._orig_task_type != self._task_type:
            changes["taskType"] = self._task_type

        # label = self._label
        # if self._name == label:
        #     label = None
        #
        # if label != self._orig_label:
        #     changes["label"] = label

        return changes

    @classmethod
    def from_entity_data(cls, task, entity_hub):
        return cls(
            task["taskType"],
            entity_id=task["id"],
            # label=task["label"],
            parent_id=task["folderId"],
            name=task["name"],
            data=task.get("data"),
            attribs=task["ownAttrib"],
            active=task["active"],
            created=False,
            entity_hub=entity_hub
        )

    def to_create_body_data(self):
        if self.parent_id is UNKNOWN_VALUE:
            raise ValueError("Task does not have set 'parent_id'")

        output = {
            "name": self.name,
            "taskType": self.task_type,
            "folderId": self.parent_id,
            "attrib": self.attribs.to_dict(),
        }
        attrib = self.attribs.to_dict()
        if attrib:
            output["attrib"] = attrib

        if self.active is not UNKNOWN_VALUE:
            output["active"] = self.active

        if (
            self._entity_hub.allow_data_changes
            and self._data is not UNKNOWN_VALUE
        ):
            output["data"] = self._data
        return output
