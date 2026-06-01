"""Task producer to enqueue tasks."""

from taskqueue.broker import redis_client
from taskqueue.serializer import serialize_task


def enqueue_task(task_name, args=None, kwargs=None, priority=1):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    task_json, task_id = serialize_task(task_name, args, kwargs, priority)

    redis_client.lpush("task_queue", task_json)
    print(f"Enqueued task: {task_name} with ID {task_id}")
    return task_id
