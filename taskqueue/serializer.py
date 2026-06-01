"""Task serializer utilities."""

import json
import uuid


def serialize_task(task_name, args, kwargs, priority):
    task_data = {
        "task_id": str(uuid.uuid4()),
        "task_name": task_name,
        "args": list(args),
        "kwargs": dict(kwargs),
        "priority": priority,
        "retries": 0,
        "status": "PENDING",
    }
    return json.dumps(task_data), task_data["task_id"]


def deserialize_task(task_json):
    return json.loads(task_json)
