import os

from pydantic import BaseModel, Field, ValidationError
from nxtools import logging, critical_error


class APIConfig(BaseModel):
    api_key: str = Field(...)
    server_url: str = Field(...)
    addon_name: str = Field(...)
    addon_version: str = Field(...)
    service_name: str = Field(...)


def get_api_config():
    data = {}
    for key, val in os.environ.items():
        key = key.lower()
        if not key.startswith("ay_"):
            continue
        data[key.replace("ay_", "", 1)] = val
    try:
        config = APIConfig(**data)
    except ValidationError as e:
        for error in e.errors():
            logging.error(f"{' '.join(error['loc'])} : {error['msg']} ")
        critical_error("Unable to configure API")
    return config


config = get_api_config()
