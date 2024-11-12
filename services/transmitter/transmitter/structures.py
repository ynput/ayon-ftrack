from typing import TypedDict, Literal


EventStatus = Literal[
    "pending", "in_progress", "finished", "failed", "aborted", "restarted"
]


class JobEventType(TypedDict):
    id: str
    dependsOn: str
    hash: str
    status: EventStatus
