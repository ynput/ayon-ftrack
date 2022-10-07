import requests

from nxtools import logging, critical_error
from pydantic import BaseModel

from ayclient.config import config


class User(BaseModel):
    name: str


class API:
    def __init__(self):
        self.user = {}
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "X-Api-Key": config.api_key,
            }
        )

        response = self.get("users/me")
        if not response:
            print(response.text)
            critical_error("Unable to login")
        self.user = User(**response.json())
        logging.info(f"Logged in as {self.user.name}")

    def url_for(self, endpoint: str) -> str:
        return f"{config.server_url.rstrip('/')}/api/{endpoint.strip('/')}"

    def get(self, endpoint: str, params=None, **kwargs) -> requests.Response:
        return self.session.get(self.url_for(endpoint), params=params)

    def post(self, endpoint: str, data=None, json=None, **kwargs) -> requests.Response:
        return self.session.post(self.url_for(endpoint), data=data, json=json, **kwargs)

    def put(self, endpoint: str, data=None, json=None, **kwargs) -> requests.Response:
        return self.session.put(self.url_for(endpoint), data=data, json=json, **kwargs)

    def patch(self, endpoint: str, data=None, json=None, **kwargs) -> requests.Response:
        return self.session.patch(
            self.url_for(endpoint),
            data=data,
            json=json,
            **kwargs,
        )

    def delete(self, endpoint: str, **kwargs) -> requests.Response:
        return self.session.delete(self.url_for(endpoint), **kwargs)


api = API()
