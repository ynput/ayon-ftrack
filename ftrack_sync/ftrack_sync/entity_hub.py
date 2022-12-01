import json
import copy
import collections
from abc import ABCMeta, abstractmethod, abstractproperty

import six
from openpype_api import get_server_api_connection
from openpype_api.graphql import GraphQlQueryFailed
from openpype_api.utils import create_entity_id


class EntityHub:
    def __init__(
        self, project_name, connection=None, allow_data_changes=False
    ):
        if not connection:
            connection = get_server_api_connection()
        self._connection = connection

        self._project_name = project_name
        self._entities_by_id = {}
        self._project_entity = None

        self._allow_data_changes = allow_data_changes

    @property
    def allow_data_changes(self):
        return self._allow_data_changes

    @property
    def project_name(self):
        return self._project_name

    @property
    def project_entity(self):
        return self._project_entity

    def get_attributes_for_type(self, entity_type):
        return self._connection.get_attributes_for_type(entity_type)

    def get_entity_by_id(self, entity_id):
        return self._entities_by_id.get(entity_id)

    @property
    def entities(self):
        for entity in self._entities_by_id.values():
            yield entity

    @property
    def entitites_by_id(self):
        return dict(self._entities_by_id)

    def add_project(self, *args, **kwargs):
        project_entity = ProjectEntity(*args, **kwargs, entity_hub=self)
        self.add_entity(project_entity)
        return project_entity

    def add_folder(self, parent, *args, **kwargs):
        """Create folder object and add it to entity hub.

        Args:
            parent (Union[ProjectEntity, FolderEntity]): Parent of added
                folder.

        Returns:
            FolderEntity: Added folder entity.
        """

        folder_entity = FolderEntity(*args, **kwargs, entity_hub=self)
        self.add_entity(folder_entity, parent)
        return folder_entity

    def add_task(self, parent, *args, **kwargs):
        task_entity = TaskEntity(*args, **kwargs, entity_hub=self)
        self.add_entity(task_entity, parent)
        return task_entity

    def add_entity(self, entity, parent=None):
        entity_id = entity.id
        if entity_id not in self._entities_by_id:
            self._entities_by_id[entity_id] = entity

        self.set_entity_parent(entity, parent)

        if isinstance(entity, ProjectEntity):
            if self._project_entity is None:
                self._project_entity = entity

            elif self._project_entity is not entity:
                raise ValueError("Got more then one project entity")

    def set_entity_parent(self, entity, parent):
        old_parent = entity.parent
        if old_parent is parent:
            return

        if old_parent is not None:
            old_parent.remove_child(entity, use_hub=False)
            old_parent.reset_immutable_for_hierarchy_cache()

        entity.set_parent(parent, use_hub=False)
        if parent is not None:
            parent.add_child(entity, use_hub=False)
            parent.reset_immutable_for_hierarchy_cache()

    def remove_entity(self, entity):
        parent = entity.parent
        if parent is None:
            return

        parent.remove_child(entity, use_hub=False)
        parent.reset_immutable_for_hierarchy_cache()

    def query_project_from_server(self):
        project_name = self.project_name
        project = self._connection.get_project(
            project_name,
            own_attributes=True
        )
        if not project:
            raise ValueError(
                "Project \"{}\" was not found.".format(project_name)
            )

        return self.add_project(
            project["code"],
            library=project["library"],
            folder_types=project["folderTypes"],
            task_types=project["taskTypes"],
            name=project["name"],
            attribs=project["ownAttrib"],
            data=project["data"],
            entity_id=project["name"],
            active=project["active"]
        )

    def query_entities_from_server(self):
        project_entity = self.query_project_from_server()

        folder_fields = set(
            self._connection.get_default_fields_for_type("folder")
        )
        folder_fields.add("hasSubsets")
        if self._allow_data_changes:
            folder_fields.add("data")

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

            for folder in folders_by_parent_id[parent_id]:
                folder_entity = parent_entity.add_folder(
                    folder["folderType"],
                    name=folder["name"],
                    entity_id=folder["id"],
                    data=folder.get("data"),
                    attribs=folder["ownAttrib"],
                    active=folder["active"],
                    thumbnail_id=folder["thumbnailId"],
                    is_new=False
                )
                folder_entity.has_published_content = folder["hasSubsets"]
                hierarchy_queue.append((folder_entity.id, folder_entity))

            for task in tasks_by_parent_id[parent_id]:
                parent_entity.add_task(
                    task["taskType"],
                    name=task["name"],
                    entity_id=task["id"],
                    data=task.get("data"),
                    attribs=task["ownAttrib"],
                    active=task["active"],
                    is_new=False
                )

        self.lock()

    def lock(self):
        entities_by_id = {}
        if self._project_entity is None:
            self._entities_by_id = entities_by_id
            return

        entity_queue = collections.deque()
        entity_queue.append(self._project_entity)
        while entity_queue:
            entity = entity_queue.popleft()
            entity.lock()
            entities_by_id[entity.id] = entity
            for child in entity:
                entity_queue.append(child)

        self._entities_by_id = entities_by_id

    def commit_changes(self):
        # TODO use Operations Session instead of known operations body
        # TODO have option to commit changes out of hierarchy
        hier_queue = collections.deque()
        hier_queue.append(self.project_entity)
        all_entity_ids = set(self._entities_by_id.keys())
        operations_body = []
        while hier_queue:
            entity = hier_queue.popleft()
            for child in entity:
                hier_queue.append(child)

            all_entity_ids.discard(entity.id)

            # Project cannot be updated using operations
            if entity.entity_type.lower() == "project":
                changes = entity.changes
                response = self._connection.patch(
                    "projects/{}".format(self.project_name),
                    **changes
                )
                if response.status_code != 204:
                    raise ValueError("Failed to update project")
                continue

            if entity.is_new:
                operations_body.append({
                    "type": "create",
                    "entityType": entity.entity_type,
                    "entityId": entity.id,
                    "data": entity.to_entity_data()
                })
                continue

            changes = entity.changes
            if changes:
                operations_body.append({
                    "type": "update",
                    "entityType": entity.entity_type,
                    "entityId": entity.id,
                    "data": changes
                })

        for entity_id in all_entity_ids:
            entity = self._entities_by_id[entity_id]
            operations_body.append({
                "type": "delete",
                "entityType": entity.entity_type,
                "entityId": entity_id
            })

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

    def __init__(self, attrib_keys, values=None):
        values = values or {}
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
        return {
            key: value
            for key, value in self.items()
            if not ignore_none or value is not None
        }


@six.add_metaclass(ABCMeta)
class BaseEntity(object):
    """Object representation of entity from server which is capturing changes.

    All data on created object are expected as "current data" on server entity
    unless the entity has set 'is_new' to 'True'. So if new data should be
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
        is_new (Union[bool, None]): Entity is new. When 'None' is passed the
            value is defined based on value of 'entity_id'.
    """

    def __init__(
        self,
        name,
        attribs=None,
        data=None,
        parent_id=None,
        entity_id=None,
        thumbnail_id=None,
        active=True,
        entity_hub=None,
        is_new=None
    ):
        if entity_hub is None:
            raise ValueError("Missing required kwarg 'entity_hub'")

        if is_new is None:
            is_new = entity_id is None

        if entity_id is None:
            entity_id = create_entity_id()

        if data is None:
            data = {}

        # These are public without any validation at this moment
        #   may change in future (e.g. name will have regex validation)
        self._name = name
        self.active = active
        self._is_new = is_new
        self._thumbnail_id = thumbnail_id
        self._entity_hub = entity_hub
        self._parent_id = parent_id
        self._attribs = Attributes(
            self._get_attributes_for_type(self.entity_type),
            attribs
        )
        self._data = data
        self._entity_id = entity_id
        self._children = []

        self._orig_parent_id = parent_id
        self._orig_name = name
        self._orig_data = copy.deepcopy(data)
        self._orig_thumbnail_id = thumbnail_id
        self._orig_active = active

        self._immutable_for_hierarchy_cache = None

    def __iter__(self):
        """Iterate over entity children.

        Returns:
            Iterable[BaseEntity]: Iteration over children.
        """

        for child in self._children:
            yield child

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
    def has_valid_parent(self):
        """Object has set valid parent for creation/update.

        Similar to 'validate_parent' but can be also used for cases when parent
            is not set on entity.

        Returns:
            bool: Parent is valid.
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

    @abstractmethod
    def validate_parent(self, parent):
        """Validate if parent can be used as parent for entity.

        Args:
            parent (Union[BaseEntity, None]): Object of parent to validate.

        Raises:
            TypeError: When parent is invalid type of parent for the entity.
        """

        pass

    @abstractmethod
    def validate_child(self, child):
        """Validate if child can be added under the object.

        Args:
            child (BaseEntity): Object of child to validate.
        """

        pass

    @abstractmethod
    def to_entity_data(self):
        """Convert object of entity to data for server.

        Can be used when entity is created on server.

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

        for child in self:
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

    def reset_immutable_for_hierarchy_cache(self, bottom_to_top=True):
        """Clear cache of immutable hierarchy property.

        This is used when entity changed parent or a child was added.

        Args:
            bottom_to_top (bool): Reset cache from top hierarchy to bottom or
                from bottom hierarchy to top.
        """

        self._immutable_for_hierarchy_cache = None
        if bottom_to_top:
            parent = self.parent
            if parent is not None:
                parent.reset_immutable_for_hierarchy_cache(bottom_to_top)

        else:
            for child in self:
                child.reset_immutable_for_hierarchy_cache(bottom_to_top)

    def _get_default_changes(self):
        """Collect changes of common data on entity.

        Returns:
            Dict[str, Any]: Changes on entity. Key and it's new value.
        """

        changes = {}
        if self._orig_name != self.name:
            changes["name"] = self._orig_name

        if self._entity_hub.allow_data_changes:
            if self._orig_data != self._data:
                changes["data"] = self._data

        if self._orig_thumbnail_id != self.thumbnail_id:
            changes["thumbnailId"] = self.thumbnail_id

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

        parent = self._get_entity_by_id(parent_id)
        if not parent and parent_id:
            raise ValueError(
                "Entity with id \"{}\" was not found.".format(parent_id)
            )
        self.set_parent(self._get_entity_by_id(parent_id))

    def get_parent(self):
        """Parent entity.

        Returns:
            Union[BaseEntity, None]: Parent object.
        """

        return self._get_entity_by_id(self._parent_id)

    def set_parent(self, parent, use_hub=True):
        """Change parent object.

        Args:
            parent (BaseEntity): New parent for entity.

        Raises:
            TypeError: If validation of parent does not pass.
        """

        if use_hub:
            self._entity_hub.set_entity_parent(self, parent)
            return

        parent_id = None
        if parent is not None:
            parent_id = parent.id
        if parent_id != self._parent_id:
            self.validate_parent(parent)
            self._parent_id = parent_id

    parent_id = property(get_parent_id, set_parent_id)
    parent = property(get_parent, set_parent)

    @property
    def children(self):
        """Access to children objects.

        Returns:
            List[BaseEntity]: Children iterator.
        """

        return list(self._children)

    def add_child(self, child, use_hub=True):
        """Add child entity.

        Args:
            child (BaseEntity): Child object to add.

        Raises:
            TypeError: When child object has invalid type to be children.
        """

        if use_hub:
            self._event_hub.set_entity_parent(child, self)
            return

        if child not in self._children:
            self.validate_child(child)
            self._children.append(child)

    def remove_child(self, child, use_hub=True):
        """Remove child entity.

        Is ignored if child is not in children.

        Args:
            child (BaseEntity): Child object to remove.
        """

        if use_hub:
            self._entity_hub.set_entity_parent(child, None)
            return

        if child in self._children:
            self._children.remove(child)

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
    def is_new(self):
        """Entity is new.

        Returns:
            bool: Entity is newly created.
        """

        return self._is_new


class ProjectEntity(BaseEntity):
    entity_type = "project"
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
    def has_valid_parent(self):
        return self._parent is None

    @property
    def changes(self):
        changes = self._get_default_changes()
        if self._orig_folder_types != self._folder_types:
            changes["folderTypes"] = self.get_folder_types()

        if self._orig_task_types != self._task_types:
            changes["taskTypes"] = self.get_task_types()

        return changes

    def to_entity_data(self):
        raise NotImplementedError(
            "ProjectEntity does not support conversion to entity data"
        )

    def add_folder(self, *args, **kwargs):
        """Create folder object and add it to entity hub.

        Args:
            folder_type (str): Folder type of folder.
            name (str): Name of folder.
            attribs (Union[Dict[str, Any], None]): Attribute values.
            data (Union[Dict[str, Any], None]): Custom entity data.
            entity_id (Union[str, None]): Entity id.
            thumbnail_id (Union[str, None]): Thumbnail id.
            active (bool): Entity is active.

        Returns:
            FolderEntity: Added folder entity.
        """

        return self._entity_hub.add_folder(self, *args, **kwargs)

    def validate_parent(self, parent):
        if parent is not None:
            raise TypeError("Project cannot have set parent. Got {}".format(
                str(type(parent))
            ))

    def validate_child(self, child):
        if isinstance(child, FolderEntity):
            return

        raise TypeError(
            "Got invalid child \"{}\". Expected 'FolderEntity'".format(
                str(type(child))
            )
        )


class FolderEntity(BaseEntity):
    entity_type = "folder"

    def __init__(self, folder_type, *args, **kwargs):
        super(FolderEntity, self).__init__(*args, **kwargs)

        self._folder_type = folder_type
        self._orig_folder_type = folder_type
        # Know if folder has any subsets
        # - is used to know if folder allows hierarchy changes
        self._has_published_content = False

    def add_folder(self, *args, **kwargs):
        return self._entity_hub.add_folder(self, *args, **kwargs)

    def add_task(self, *args, **kwargs):
        return self._entity_hub.add_task(self, *args, **kwargs)

    def lock(self):
        super(FolderEntity, self).lock()
        self._orig_folder_type = self._folder_type

    @property
    def has_valid_parent(self):
        return isinstance(self._parent, (ProjectEntity, FolderEntity))

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

        return changes

    def to_entity_data(self):
        parent_id = self._parent_id
        if parent_id == self.project_name:
            parent_id = None
        output = {
            "name": self.name,
            "folderType": self.folder_type,
            "parentId": parent_id,
            "thumbnailId": self.thumbnail_id,
            "attrib": self.attribs.to_dict(),
            "active": self.active,
        }
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
        self.reset_immutable_for_hierarchy_cache()

    has_published_content = property(
        get_has_published_content, set_has_published_content
    )

    @property
    def _immutable_for_hierarchy(self):
        if self.has_published_content:
            return True
        return None

    def validate_parent(self, parent):
        if parent is None or isinstance(parent, (ProjectEntity, FolderEntity)):
            return

        raise TypeError((
            "Invalid type of parent. Got {}."
            " Expected 'None', 'ProjectEntity' or 'FolderEntity'"
        ).format(str(type(parent))))

    def validate_child(self, child):
        if isinstance(child, (FolderEntity, TaskEntity)):
            return

        raise TypeError((
            "Got invalid child \"{}\". Expected 'FolderEntity' or 'TaskEntity'"
        ).format(str(type(child))))


class TaskEntity(BaseEntity):
    entity_type = "task"

    def __init__(self, task_type, *args, **kwargs):
        super(TaskEntity, self).__init__(*args, **kwargs)

        self._task_type = task_type
        self._orig_task_type = task_type

    @property
    def has_valid_parent(self):
        return isinstance(self._parent, FolderEntity)

    def lock(self):
        super(TaskEntity, self).lock()
        self._orig_task_type = self._task_type

    def get_task_type(self):
        return self._task_type

    def set_task_type(self, task_type):
        self._task_type = task_type

    task_type = property(get_task_type, set_task_type)

    def validate_parent(self, parent):
        if parent is None:
            return

        if not isinstance(parent, FolderEntity):
            raise TypeError((
                "Invalid type of parent. Got {}."
                " Expected 'None' or 'FolderEntity'"
            ).format(str(type(parent))))

    def validate_child(self, child):
        raise ValueError("{} does not support children assignment".format(
            self.__class__.__name__))

    @property
    def changes(self):
        changes = self._get_default_changes()

        if self._orig_parent_id != self._parent_id:
            changes["folderId"] = self._parent_id

        if self._orig_task_type != self._task_type:
            changes["taskType"] = self._task_type

        return changes

    def to_entity_data(self):
        output = {
            "name": self.name,
            "taskType": self.task_type,
            "folderId": self.parent_id,
            "attrib": self.attribs.to_dict(),
            "active": self.active,
        }

        if self._entity_hub.allow_data_changes:
            output["data"] = self._data
        return output
