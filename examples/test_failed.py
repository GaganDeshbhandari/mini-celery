import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from taskqueue.task import task


@task
def fail_task():
    raise Exception("Intentional failure")


if __name__ == "__main__":
    task_id = fail_task.delay()

    print("Enqueued Task ID:", task_id)