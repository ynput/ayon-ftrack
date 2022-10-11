import time
import ayclient
from nxtools import logging


def get_job():
    payload = {
        "sourceTopic": "ftrack.leech",
        "targetTopic": "ftrack.proc",
        "sender": ayclient.config.service_name,
        "description": "Event processing",
        "sequential": True,
    }
    response = ayclient.api.post("enroll", json=payload)
    if not response:
        if response.status_code == 404:
            logging.info("Nothing to do")
        else:
            logging.error("Something's wrong")
        return None
    return response.json()


def process(event_id: str, source_id: str):

    source_event = ayclient.api.get(f"events/{source_id}").json()
    print("Source event:", source_event)

    description = f"Processed {source_event['description']}"

    req_data = {
        "sender": ayclient.config.service_name,
        "description": description,
        "payload": {"value": "whatevr"},
        "status": "finished",
    }

    res = ayclient.api.patch(f"events/{event_id}", json=req_data)
    assert res


def main():
    while True:
        job = get_job()
        if job is None:
            break
        process(job["id"], job["dependsOn"])
        time.sleep(0.01)


if __name__ == "__main__":
    main()
