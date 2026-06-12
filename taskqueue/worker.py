"""Task worker to consume and execute tasks."""

from examples.test_producer import send_email
from examples.test_failed import fail_task
from examples.test_retry import flaky_task
from examples.test_priority_queue import *
from examples.test_priority_retry import *
from examples.test_heartbeat import *
from taskqueue.broker import redis_client
from taskqueue.dlq import push_to_dlq
from taskqueue.serializer import deserialize_task
from taskqueue.task import task_registry
from taskqueue.task_status import set_task_status
from taskqueue.status import TaskStatus

import json
import time
from datetime import datetime,timedelta

import os
import socket

from threading import Thread

WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"

MAX_RETRIES = 3
BASE_RETRY_DELAY = 5
HEARTBEAT_INTERVAL = 5

def send_heartbeats():
    while True:

        redis_client.hset(
            f"worker:{WORKER_ID}",
            mapping={
                "status":"ONLINE",
                "last_heartbeat" : datetime.now().isoformat()
            }
        )

        time.sleep(HEARTBEAT_INTERVAL)

def get_retry_delay(retry, base_delay):
    return base_delay**retry


def process_task(task_json, processing_queue):
    task_data = deserialize_task(task_json)
    task_name = task_data.get("task_name")
    args = task_data.get("args", [])
    kwargs = task_data.get("kwargs", {})
    task_id = task_data.get("task_id")
    retries = task_data.get("retries")


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

    redis_client.hset(
        f"processing:{task_id}",
        mapping={
            "worker_id": WORKER_ID,
            "started_at": datetime.now().isoformat(),
            "processing_queue": processing_queue
        }
    )
    set_task_status(task_id, TaskStatus.RUNNING)

    try:
        func(*args, **kwargs)

        redis_client.lrem(
            processing_queue,
            1,
            task_json
        )
        redis_client.delete(
                f"processing:{task_id}"
            )
        set_task_status(task_id,TaskStatus.SUCCESS)

        print(f"[{WORKER_ID}] "f"Task completed: {task_name} [{task_id}]")

    except Exception as e:
        if(retries >= MAX_RETRIES):

            task_data["failed_at"] = datetime.now().isoformat()
            redis_client.delete(
                f"processing:{task_id}"
            )
            set_task_status(task_id,TaskStatus.FAILED)

            redis_client.lrem(
                processing_queue,
                1,
                task_json
            )
            push_to_dlq(json.dumps(task_data))


            print(f"[{WORKER_ID}] "f"Task error: {task_name} [{task_id}]")
            print(f"Task pushed to DLQ: {task_name} [{task_id}]")

        else:

            task_data["retries"] += 1
            wait_time = get_retry_delay(task_data["retries"],BASE_RETRY_DELAY)

            redis_client.delete(
                f"processing:{task_id}"
            )

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
    print(task_registry.keys())

    redis_client.hset(
        "workers",
        WORKER_ID,
        "ONLINE"
    )

    heartbeat_thread = Thread(
        target=send_heartbeats,
        daemon=True
    )

    heartbeat_thread.start()

    print(f"Worker {WORKER_ID} started")

    while True:

        critical_result = redis_client.blmove(
            "critical_queue",
            "critical_processing",
            1,
            src="RIGHT",
            dest="LEFT"
        )

        if critical_result:


            process_task(critical_result,"critical_processing")
            continue

        high_result = redis_client.blmove(
            "high_queue",
            "high_processing",
            1,
            src="RIGHT",
            dest="LEFT"
        )

        if high_result:
            process_task(high_result,"high_processing")
            continue

        medium_result = redis_client.blmove(
            "medium_queue",
            "medium_processing",
            1,
            src="RIGHT",
            dest="LEFT"
        )

        if medium_result:
            process_task(medium_result,"medium_processing")
            continue

        low_result = redis_client.blmove(
            "low_queue",
            "low_processing",
            1,
            src="RIGHT",
            dest="LEFT"
        )

        if low_result:
            process_task(low_result,"low_processing")
            continue

        print("No tasks. Waiting...")



if __name__ == "__main__":
    # run_worker()
    try:
        run_worker()
    finally:
        redis_client.hdel(
            "workers",
            WORKER_ID
        )
        redis_client.delete(
            f"worker:{WORKER_ID}"
        )