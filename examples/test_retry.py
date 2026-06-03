import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from taskqueue.task import task

attempts = 0

@task
def flaky_task():
    global attempts

    attempts += 1

    print(f"Attempt {attempts}")

    if attempts < 3:
        raise Exception("Temporary Failure")

    return "Success"

if __name__ == "__main__":
    task_id = flaky_task.delay()

    print("Enqueued Task ID:", task_id)