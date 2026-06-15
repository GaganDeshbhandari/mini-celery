import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from taskqueue.task import task, task_registry

import time


@task
def long_running_task(seconds):
    print(f"Starting task for {seconds} seconds...")
    time.sleep(seconds)
    print("Task completed")


if __name__ == "__main__":
      long_running_task.delay(
      120,
      priority=4
    )