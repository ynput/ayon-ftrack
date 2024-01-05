import pyblish.api
from ayon_ftrack.pipeline import plugin


class IntegrateFtrackComponentOverwrite(plugin.FtrackPublishInstancePlugin):
    """
    Set `component_overwrite` to True on all instances `ftrackComponentsList`
    """

    order = pyblish.api.IntegratorOrder + 0.49
    label = "Overwrite ftrack created versions"
    families = ["clip"]
    optional = True
    active = False

    def process(self, instance):
        component_list = instance.data.get("ftrackComponentsList")
        if not component_list:
            self.log.info("No component to overwrite...")
            return

        for cl in component_list:
            cl["component_overwrite"] = True
            name = cl["component_data"]["name"]
            self.log.debug("Component {} overwriting".format(name))
