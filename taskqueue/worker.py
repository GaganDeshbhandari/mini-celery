"""Task worker to consume and execute tasks."""

from examples.test_producer import send_email
from examples.test_failed import fail_task
from examples.test_retry import flaky_task
from examples.test_priority_queue import *
from examples.test_priority_retry import *
from taskqueue.broker import redis_client
from taskqueue.dlq import push_to_dlq
from taskqueue.serializer import deserialize_task
from taskqueue.task import task_registry
from taskqueue.task_status import set_task_status
from taskqueue.status import TaskStatus

import json
import time
from datetime import datetime,timedelta


MAX_RETRIES = 3
BASE_RETRY_DELAY = 5

def get_retry_delay(retry, base_delay):
    return base_delay**retry

def prepare_task(task_json):
    task_data = deserialize_task(task_json)

    task_data["processing_started_at"] = (
        datetime.now().isoformat()
    )

    return json.dumps(task_data)

def process_task(task_json, processing_queue):
    task_data = deserialize_task(task_json)
    task_name = task_data.get("task_name")
    args = task_data.get("args", [])
    kwargs = task_data.get("kwargs", {})
    task_id = task_data.get("task_id")
    retries = task_data.get("retries")

    # Will add this later for the recovery_scheduler
    # task_data["processing_started_at"] = (
    #         datetime.now().isoformat()
    #     )

    func = task_registry.get(task_name)


    if func is None:
        print(f"Unknown task: {task_name}")

        redis_client.lrem(
            processing_queue,
            1,
            task_json
        )
        set_task_status(task_id, TaskStatus.REJECTED)

        return

    set_task_status(task_id, TaskStatus.RUNNING)

    try:
        func(*args, **kwargs)

        redis_client.lrem(
            processing_queue,
            1,
            task_json
        )
        set_task_status(task_id,TaskStatus.SUCCESS)

        print(f"Task completed: {task_name} [{task_id}]")

    except Exception as e:
        if(retries >= MAX_RETRIES):

            task_data["failed_at"] = datetime.now().isoformat()
            set_task_status(task_id,TaskStatus.FAILED)

            redis_client.lrem(
                processing_queue,
                1,
                task_json
            )
            push_to_dlq(json.dumps(task_data))

            print(f"Task error: {task_name} [{task_id}] {e}")
            print(f"Task pushed to DLQ: {task_name} [{task_id}]")

        else:

            task_data["retries"] += 1
            wait_time = get_retry_delay(task_data["retries"],BASE_RETRY_DELAY)

            print(f"Retrying task {task_id} in {wait_time} seconds (attempt {task_data["retries"]})")

            scheduled_at = datetime.now() + timedelta(seconds=wait_time)
            task_data["scheduled_at"] = scheduled_at.isoformat()

            redis_client.lrem(
                processing_queue,
                1,
                task_json
            )
            set_task_status(task_id,TaskStatus.PENDING)

            execution_time = scheduled_at.timestamp()
            redis_client.zadd(
                "retry_queue",
                {
                    json.dumps(task_data): execution_time
                }
            )


def run_worker():
    print("Worker started. Waiting for tasks...")
    print(task_registry.keys())

#     QUEUE_MAP = {
#     "critical_queue": "critical_processing",
#     "high_queue": "high_processing",
#     "medium_queue": "medium_processing",
#     "low_queue": "low_processing",
# }

    while True:

        critical_result = redis_client.blmove(
            "critical_queue",
            "critical_processing",
            "RIGHT",
            "LEFT",
            timeout=1
        )

        if critical_result:
            critical_result = prepare_task(critical_result)
            process_task(critical_result,"critical_processing")
            continue

        high_result = redis_client.blmove(
            "high_queue",
            "high_processing",
            "RIGHT",
            "LEFT",
            timeout=1
        )

        if high_result:
            high_result = prepare_task(high_result)
            process_task(high_result,"high_processing")
            continue

        medium_result = redis_client.blmove(
                "medium_queue",
                "medium_processing",
                "RIGHT",
                "LEFT",
                timeout=1
            )

        if medium_result:
            medium_result = prepare_task(medium_result)
            process_task(medium_result,"medium_processing")
            continue

        low_result = redis_client.blmove(
                "low_queue",
                "low_processing",
                "RIGHT",
                "LEFT",
                timeout=1
                )

        if low_result:
            low_result = prepare_task(low_result)
            process_task(low_result,"low_processing")
            continue

        print("No tasks. Waiting...")



if __name__ == "__main__":
    run_worker()
