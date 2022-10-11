from typing import Any, Type

from openpype.addons import BaseServerAddon

from .settings import FtrackSettings


class FtrackAddon(BaseServerAddon):
    name = "ftrack"
    title = "ftrack addon"
    version = "1.0.0"
    settings_model: Type[FtrackSettings] = FtrackSettings
    frontend_scopes: dict[str, Any] = {"project": {"sidebar": "hierarchy"}}
    services = {
        "leecher": {"image": "openpype/ay-ftrack-leecher:1.0.0"}
    }

    def setup(self):
        pass

    async def get_default_settings(self):
        settings_model_cls = self.get_settings_model()
        return settings_model_cls(**{
            "ftrack_server": "",
            "events": {
                "status_update": {
                    "enabled": True,
                    "mapping": [
                        {
                            "name": "In Progress",
                            "value": ["__any__"]
                        }, {
                            "name": "Ready",
                            "value": ["Not Ready"]
                        }, {
                            "name": "__ignore__",
                            "value": [
                                "in progress",
                                "omitted",
                                "on hold"
                            ]
                        }
                    ]
                }
            }
        })
