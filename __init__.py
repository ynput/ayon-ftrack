from typing import Any, Type

from openpype.addons import BaseServerAddon

from .settings import FtrackSettings, DEFAULT_VALUES


class FtrackAddon(BaseServerAddon):
    name = "ftrack"
    title = "ftrack addon"
    version = "1.0.0"
    settings_model: Type[FtrackSettings] = FtrackSettings
    services = {
        "leecher": {"image": "openpype/ay-ftrack-leecher:1.0.0"},
        "processor": {"image": "openpype/ay-ftrack-processor:1.0.0"}
    }

    async def get_default_settings(self):
        settings_model_cls = self.get_settings_model()
        return settings_model_cls(**DEFAULT_VALUES)
