"""Task worker to consume and execute tasks."""

from examples.test_producer import send_email
from examples.test_failed import fail_task
from examples.test_retry import flaky_task
from taskqueue.broker import redis_client
from taskqueue.dlq import push_to_dlq
from taskqueue.serializer import deserialize_task, serialize_task
from taskqueue.task import task_registry
from taskqueue.task_status import set_task_status
from taskqueue.status import TaskStatus

import json
import time
from datetime import datetime,timedelta


MAX_RETRIES = 3

def get_retry_delay(retry, base_delay):
    return base_delay**retry

def process_task(task_json):
    task_data = deserialize_task(task_json)
    task_name = task_data.get("task_name")
    args = task_data.get("args", [])
    kwargs = task_data.get("kwargs", {})
    task_id = task_data.get("task_id")
    retries = task_data.get("retries")

    func = task_registry.get(task_name)


    if func is None:
        print(f"Unknown task: {task_name}")
        set_task_status(task_id, TaskStatus.REJECTED)
        return

    set_task_status(task_id, TaskStatus.RUNNING)

    try:
        func(*args, **kwargs)
        set_task_status(task_id,TaskStatus.SUCCESS)
        print(f"Task completed: {task_name} [{task_id}]")

    except Exception as e:
        if(retries >= MAX_RETRIES):

            task_data["failed_at"] = datetime.now().isoformat()
            push_to_dlq(json.dumps(task_data))
            set_task_status(task_id,TaskStatus.FAILED)
            print(f"Task error: {task_name} [{task_id}] {e}")
            print(f"Task pushed to DLQ: {task_name} [{task_id}]")
        else:
            task_data["retries"] += 1
            wait_time = get_retry_delay(task_data["retries"],5)

            print(f"Retrying task {task_id} in {wait_time} seconds (attempt {task_data["retries"]})")

            # time.sleep(wait_time)
            scheduled_at = datetime.now() + timedelta(seconds=wait_time)
            task_data["scheduled_at"] = scheduled_at.isoformat()

            set_task_status(task_id,TaskStatus.PENDING)
            redis_client.lpush("retry_queue",json.dumps(task_data))


def run_worker():
    print("Worker started. Waiting for tasks...")
    print(task_registry.keys())
    while True:
        critical_result = redis_client.rpop("critical_queue")

        if critical_result:
            process_task(critical_result)
            continue

        high_result = redis_client.rpop("high_queue")

        if high_result:
            process_task(high_result)
            continue

        medium_result = redis_client.rpop("medium_queue")

        if medium_result:
            process_task(medium_result)
            continue

        low_result = redis_client.rpop("low_queue")

        if low_result:
            process_task(low_result)
            continue

        print("No tasks. Waiting...")
        time.sleep(1)


if __name__ == "__main__":
    run_worker()
