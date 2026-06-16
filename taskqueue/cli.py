from examples.test_producer import *
from examples.test_failed import *
from examples.test_retry import *
from examples.test_priority_queue import *
from examples.test_priority_retry import *
from examples.test_heartbeat import *
from taskqueue.task import task_registry


import argparse
import json
from datetime import datetime

from taskqueue.broker import redis_client
from taskqueue.task_status import get_task_status

def show_workers():
    workers = redis_client.hgetall(
        "workers"
    )

    if not workers:
        print("No workers found")
        return

    for worker_id, status in workers.items():
        print(f"{worker_id} : {status}")

def show_dead_workers():
    workers = redis_client.hgetall(
        "workers"
    )

    dead_found = False

    for worker_id, status in workers.items():

        if status == "DEAD":
            print(worker_id)
            dead_found = True

    if not dead_found:
        print("No dead workers found")

def show_worker_tasks(worker_id):

    tasks = redis_client.smembers(
        f"worker_tasks:{worker_id}"
    )

    if not tasks:
        print("No tasks found")
        return

    print(f"Tasks owned by {worker_id}:")

    for task_id in tasks:
        print(task_id)

def show_queue_stats():

    queues = {
        "critical_queue": redis_client.llen,
        "high_queue": redis_client.llen,
        "medium_queue": redis_client.llen,
        "low_queue": redis_client.llen,
        "critical_processing": redis_client.llen,
        "high_processing": redis_client.llen,
        "medium_processing": redis_client.llen,
        "low_processing": redis_client.llen,
        "dead_letter_queue": redis_client.llen,
        "retry_queue": redis_client.zcard,
    }

    print("\nQueue Statistics")
    print("-" * 40)

    for queue_name, operation in queues.items():
        size = operation(queue_name)
        print(f"{queue_name:<20} {size}")


def show_processing_queues():

    processing_queues = [
        "critical_processing",
        "high_processing",
        "medium_processing",
        "low_processing"
    ]

    print("\nProcessing Queues")
    print("-" * 40)

    for queue in processing_queues:

        tasks = redis_client.lrange(
            queue,
            0,
            -1
        )

        print(f"\n{queue} ({len(tasks)} tasks)")

        for task_json in tasks:
            task_data = json.loads(task_json)

            print(
                f"  {task_data['task_id']} "
                f"({task_data['task_name']})"
            )

def show_retry_queue():

    tasks = redis_client.zrange(
        "retry_queue",
        0,
        -1,
        withscores=True
    )

    if not tasks:
        print("Retry queue is empty")
        return

    print("\nRetry Queue")
    print("-" * 50)

    for task_json, score in tasks:

        task_data = json.loads(task_json)

        scheduled_at = datetime.fromtimestamp(
            score
        )

        print(
            f"{task_data['task_id']} | "
            f"{task_data['task_name']} | "
            f"retry={task_data['retries']} | "
            f"scheduled_at={scheduled_at}"
        )

def purge_dlq():

    count = redis_client.llen(
        "dead_letter_queue"
    )

    redis_client.delete(
        "dead_letter_queue"
    )

    print(
        f"Removed {count} tasks from DLQ"
    )

def show_dlq():

    tasks = redis_client.lrange(
        "dead_letter_queue",
        0,
        -1
    )

    if not tasks:
        print("Dead Letter Queue is empty")
        return

    print("\nDead Letter Queue")
    print("-" * 80)

    for task_json in tasks:

        task_data = json.loads(task_json)

        print(
            f"Task ID: {task_data.get('task_id')}"
        )
        print(
            f"Task Name: {task_data.get('task_name')}"
        )
        print(
            f"Retries: {task_data.get('retries')}"
        )
        print(
            f"Failed At: {task_data.get('failed_at')}"
        )
        print("-" * 80)


def show_task_status(task_id):

    status = get_task_status(task_id)

    if not status:
        print("Task not found")
        return

    print(
        f"Task {task_id}: {status}"
    )

def show_task_info(task_id):

    status = redis_client.get(
        f"task_status:{task_id}"
    )

    processing = redis_client.hgetall(
        f"processing:{task_id}"
    )

    print("\nTask Information")
    print("-" * 40)

    print(f"Task ID: {task_id}")
    print(f"Status: {status}")

    if processing:
        print(
            f"Worker ID: {processing.get('worker_id')}"
        )

        print(
            f"Started At: {processing.get('started_at')}"
        )

        print(
            f"Processing Queue: {processing.get('processing_queue')}"
        )

def show_health():

    workers = redis_client.hgetall(
        "workers"
    )

    online_workers = 0
    dead_workers = 0

    for status in workers.values():

        if status == "ONLINE":
            online_workers += 1

        elif status == "DEAD":
            dead_workers += 1

    processing_tasks = (
        redis_client.llen("critical_processing")
        + redis_client.llen("high_processing")
        + redis_client.llen("medium_processing")
        + redis_client.llen("low_processing")
    )

    print("\nSystem Health")
    print("-" * 40)

    print(f"Online Workers : {online_workers}")
    print(f"Dead Workers   : {dead_workers}")

    print(
        f"Critical Queue : {redis_client.llen('critical_queue')}"
    )

    print(
        f"High Queue     : {redis_client.llen('high_queue')}"
    )

    print(
        f"Medium Queue   : {redis_client.llen('medium_queue')}"
    )

    print(
        f"Low Queue      : {redis_client.llen('low_queue')}"
    )

    print(
        f"Processing     : {processing_tasks}"
    )

    print(
        f"Retry Queue    : {redis_client.zcard('retry_queue')}"
    )

    print(
        f"DLQ            : {redis_client.llen('dead_letter_queue')}"
    )

# def submit_task(task_name):

#     task_func = task_registry.get(
#         task_name
#     )

#     if not task_func:
#         print(
#             f"Unknown task: {task_name}"
#         )
#         return

#     task_id = task_func.delay()

#     print(
#         f"Task submitted successfully"
#     )
#     print(
#         f"Task ID: {task_id}"
#     )

def submit_task(task_name, args):

    task_func = task_registry.get(task_name)

    if not task_func:
        print(f"Unknown task: {task_name}")
        return

    task_id = task_func.delay(*args)

    print(f"Task submitted: {task_id}")

def main():
    parser = argparse.ArgumentParser(
        prog="mini-celery",
        description="Mini Celery CLI"
    )

    subparsers = parser.add_subparsers(
        dest="command"
    )

    subparsers.add_parser(
        "workers",
        help="Show all workers"
    )

    subparsers.add_parser(
        "dead-workers",
        help="Show dead workers"
    )

    worker_tasks_parser = subparsers.add_parser(
    "worker-tasks",
    help="Show tasks owned by a worker"
    )

    worker_tasks_parser.add_argument(
    "worker_id"
    )

    subparsers.add_parser(
        "queue-stats",
        help="Show the stats of all the queue"
    )

    subparsers.add_parser(
        "processing-queues",
        help="Show processing queues"
    )

    subparsers.add_parser(
        "retry-queue",
        help="Show retry queue"
    )

    subparsers.add_parser(
        "dlq",
        help="Show dead letter queue"
    )

    subparsers.add_parser(
        "purge-dlq",
        help="Delete all tasks from DLQ"
    )

    task_status_parser = subparsers.add_parser(
        "task-status",
        help="Show task status"
    )

    task_status_parser.add_argument(
        "task_id"
    )

    task_info_parser = subparsers.add_parser(
        "task-info",
        help="Show task information"
    )

    task_info_parser.add_argument(
        "task_id"
    )

    subparsers.add_parser(
        "health",
        help="Show system health"
    )

    submit_parser = subparsers.add_parser(
        "submit",
        help="Submit a task"
    )

    submit_parser.add_argument(
        "task_name"
    )

    submit_parser.add_argument(
        "--args",
        nargs="*",
        default=[]
    )

    args = parser.parse_args()

    if args.command == "workers":
        show_workers()

    elif args.command == "dead-workers":
        show_dead_workers()

    elif args.command == "worker-tasks":
        show_worker_tasks(
            args.worker_id
        )

    elif args.command == "queue-stats":
        show_queue_stats()

    elif args.command == "processing-queues":
        show_processing_queues()

    elif args.command == "retry-queue":
        show_retry_queue()

    elif args.command == "dlq":
        show_dlq()

    elif args.command == "purge-dlq":
        purge_dlq()

    elif args.command == "task-status":
        show_task_status(
            args.task_id
        )

    elif args.command == "task-info":
        show_task_info(
        args.task_id
    )

    elif args.command == "health":
        show_health()

    # elif args.command == "submit":
    #     submit_task(
    #         args.task_name
    #     )

    elif args.command == "submit":
        submit_task(
            args.task_name,
            args.args
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()