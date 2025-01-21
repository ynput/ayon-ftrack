import datetime
import urllib.parse
from typing import Optional, Any, Iterable, Union

import httpx

# Maybe add FtrackEntity class with 'entity_type' property
#     to match the ftrack_api.entity.base.Entity class
FtrackEntityType = dict[str, Any]


class ServerError(Exception):
    def __init__(self, message, response=None, error_code=None):
        super().__init__(message)
        self.response = response
        self.error_code = error_code

    @classmethod
    def from_call_error(cls, response):
        response_data = response.json()
        exception = response_data["exception"]
        content = response_data["content"]
        error_code = response_data.get("error_code")
        used_cls = cls
        if error_code == "api_credentials_invalid":
            return InvalidCredentials(
                content,
                response,
                error_code,
            )

        return used_cls(
            f"Server reported error: {exception} ({content})",
            response,
            error_code,
        )


class InvalidCredentials(ServerError):
    pass


class QueryResult:
    def __init__(self, session: "FtrackSession", query: str, limit=500):
        self._session = session
        self._query = query
        self._limit = limit
        self._offset = 0
        self._done = False
        self._fetched_data = None
        self._first = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._fetched_data:
            if not self._done:
                self._fetched_data = await self._fetch_more()
            if not self._fetched_data:
                self._done = True
                raise StopAsyncIteration()

        output = self._fetched_data.pop(0)
        if self._first is None:
            self._first = output
        return output

    async def first(self):
        if self._first is not None:
            return self._first

        if self._fetched_data is not None:
            raise ValueError("Query already started, cannot return first.")
        self._limit = 1
        async for item in self:
            self._done = True
            return item
        return None

    async def all(self) -> list[FtrackEntityType]:
        item: FtrackEntityType
        return [item async for item in self]

    async def _fetch_more(self):
        if self._done:
            return None

        query_parts = [self._query, f"limit {self._limit}"]
        if self._offset:
            query_parts.append(f"offset {self._offset}")

        result = await self._session.call({
            "action": "query",
            "expression": " ".join(query_parts),
        })
        self._offset += self._limit
        if len(result["data"]) < self._limit:
            self._done = True
        return result["data"]


class FtrackSession:
    def __init__(self, server_url: str, api_key: str, username: str):
        server_url = server_url.rstrip("/")
        self._server_url = server_url
        self._api_key = api_key
        self._username = username
        self._api_url = server_url + "/api"
        self._client = httpx.AsyncClient(
            headers={
                "content-type": "application/json",
                "accept": "application/json",
                "ftrack-api-key": api_key,
                "ftrack-user": username,
            }
        )

    async def call(self, data):
        single_item = isinstance(data, dict)
        if single_item:
            data = [data]
        try:
            response = await self._client.post(self._api_url, json=data)
        except httpx.ConnectError as exc:
            raise ServerError(
                f"Failed to connect to server {exc}",
                None,
                "connection_error",
            )

        if response.status_code != 200:
            raise ServerError(response, response.text)
        response_data = response.json()
        if "exception" in response_data:
            raise ServerError.from_call_error(response)

        if single_item:
            return response_data[0]
        return response_data

    async def validate(self):
        await self.get_server_information()

    async def get_server_information(self):
        return await self.call({"action": "query_server_information"})

    def query(self, query: str, limit=500):
        return QueryResult(self, query, limit)

    async def get_projects(
        self, fields: Optional[set[str]] = None
    ):
        if not fields:
            fields = {"id", "full_name", "name", "status"}
        fields_str = ", ".join(fields)
        return self.query(f"select {fields_str} from Project")

    def get_url(self, resource_identifier: str) -> str:
        query = urllib.parse.urlencode((
            ("id", resource_identifier),
            ("username", self._username),
            ("apiKey", self._api_key),
        ))
        return f"{self._server_url}/component/get?{query}"


def join_filter_values(values: Iterable[str]) -> str:
    return ",".join(f'"{value}"' for value in values)


def create_chunks(
    iterable: set[str],
    chunk_size: int = 200,
):
    if not iterable:
        return

    if chunk_size < 1:
        chunk_size = 1

    iterable_size = len(iterable)
    tupled_iterable = tuple(iterable)
    for idx in range(0, iterable_size, chunk_size):
        yield tupled_iterable[idx:idx + chunk_size]


def convert_ftrack_date_obj(
    date: Union[dict[str, Any], str, None]
) -> Optional[datetime.datetime]:
    if date is None:
        return None
    if isinstance(date, dict):
        date = date["value"]
    date_obj = datetime.datetime.fromisoformat(date)
    date_obj += datetime.timedelta(hours=24 - date_obj.hour)
    return date_obj


def convert_ftrack_date(
    date: Union[dict[str, Any], str, None]
) -> Optional[str]:
    """Convert ftrack date to "standard" date.

    Dates in ftrack are stored with slight offset. This function
        adds 24 hours to the date to get the next day 00:00:00.

    Args:
        date (Union[dict[str, Any], str, None]): ftrack date.

    Returns:
        Optional[str]: Standard date.

    """
    date_obj = convert_ftrack_date_obj(date)
    if date_obj is not None:
        return date_obj.isoformat()
    return None
