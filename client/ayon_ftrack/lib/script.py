import collections
import ftrack_api


project_name = "Name"
hierarchical_cust_attrs = []
joined_hierarchical_attrs = ",".join(
    [f'"{attr}"'for attr in hierarchical_cust_attrs]
)

session = ftrack_api.Session()

project_entity = session.query(
    f"Project where full_name is {project_name}"
).one()
project_id = project_entity["id"]
task_entities = session.query(
    f"select id from TypedContext where project_id is {project_id}"
).all()
task_entity_ids = [entity["id"] for entity in task_entities]
cust_attr_confs = session.qeury((
    "select id, key, is_hierarchical from CustomAttributeConfiguration"
    f" where key in {joined_hierarchical_attrs}"
)).all()

cust_attr_keys_by_id = {
    attr_conf["id"]: attr_conf["key"]
    for attr_conf in cust_attr_confs
    if attr_conf["is_hierarchical"]
}

joined_attr_conf_ids = ",".join(cust_attr_keys_by_id.keys())
joined_task_ids = ",".join([entity["id"] for entity in task_entities])

values = session.query((
    "select value, entity_id, configuration_id from CustomAttributeValue"
    " where entity_id in ({}) and configuration_id in ({})"
).format(joined_task_ids, joined_attr_conf_ids))


values_by_entity_id = collections.defaultdict(list)
for item in values:
    value = item["value"]
    if value is None:
        continue

    attr_id = item["configuration_id"]
    entity_id = item["entity_id"]
    values_by_entity_id[entity_id].append(attr_id)


for entity_id, conf_ids in values_by_entity_id.items():
    for conf_id in conf_ids:
        key = cust_attr_keys_by_id[conf_id]
        if key == "width":
            value = 800
        elif key == "height":
            value = 600
        else:
            continue
        update_data = collections.OrderedDict([
            ("configuration_id", conf_id),
            ("entity_id", entity_id)
        ])

        session.recorded_operations.push(
            ftrack_api.operation.UpdateEntityOperation(
                "ContextCustomAttributeValue",
                update_data,
                "value",
                ftrack_api.symbol.NOT_SET,
                value
            )
        )
