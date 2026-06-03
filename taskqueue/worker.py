"""Task worker to consume and execute tasks."""

from examples.test_producer import send_email
from examples.test_failed import fail_task
from taskqueue.broker import redis_client
from taskqueue.serializer import deserialize_task
from taskqueue.task import task_registry
from taskqueue.task_status import set_task_status
from taskqueue.status import TaskStatus


def process_task(task_json):
    task_data = deserialize_task(task_json)
    task_name = task_data.get("task_name")
    args = task_data.get("args", [])
    kwargs = task_data.get("kwargs", {})
    task_id = task_data.get("task_id")

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
        set_task_status(task_id,TaskStatus.FAILED)
        print(f"Task error: {task_name} [{task_id}] {e}")


def run_worker():
    print("Worker started. Waiting for tasks...")
    print(task_registry.keys())
    while True:
        result = redis_client.brpop("task_queue", timeout=5)
        if result is None:
            print("No tasks. Waiting...")
            continue

        task_json = result[1]
        process_task(task_json)


if __name__ == "__main__":
    run_worker()
