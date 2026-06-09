import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from taskqueue.task import task


@task
def critical_task():
    print("CRITICAL TASK EXECUTED")


@task
def high_task():
    print("HIGH TASK EXECUTED")


@task
def medium_task():
    print("MEDIUM TASK EXECUTED")


@task
def low_task():
    print("LOW TASK EXECUTED")


if __name__ == "__main__":
    low_task_id = low_task.delay(priority=1)
    medium_task_id = medium_task.delay(priority=2)
    high_task_id = high_task.delay(priority=3)
    critical_task_id = critical_task.delay(priority=4)

    print("low_task_id:", low_task_id)
    print("medium_task_id:", medium_task_id)
    print("high_task_id:", high_task_id)
    print("critical_task_id:", critical_task_id)
