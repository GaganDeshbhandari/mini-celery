import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from taskqueue.task import task


attempt = 0


@task
def flaky_critical_task():
    global attempt

    attempt += 1

    print(f"Attempt {attempt}")

    if attempt < 3:
        raise Exception("Temporary failure")

    print("TASK RECOVERED")


if __name__ == "__main__":
    task_id = flaky_critical_task.delay(priority=4)
    print("flaky_critical_task_id:", task_id)
