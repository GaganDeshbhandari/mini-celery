"""Task producer to enqueue tasks."""

from taskqueue.broker import redis_client
from taskqueue.serializer import serialize_task
from taskqueue.task_status import set_task_status
from taskqueue.status import TaskStatus

def enqueue_task(task_name, args=None, kwargs=None, priority=1):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    task_json, task_id = serialize_task(task_name, args, kwargs, priority)

    if(priority == 4):
        redis_client.lpush("critical_queue", task_json)
    elif(priority == 3):
        redis_client.lpush("high_queue", task_json)
    elif(priority == 2):
        redis_client.lpush("medium_queue", task_json)
    else:
        redis_client.lpush("low_queue", task_json)
    
    set_task_status(task_id,TaskStatus.PENDING)
    print(f"Enqueued task: {task_name} with ID {task_id}")
    return task_id
