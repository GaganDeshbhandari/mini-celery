# examples/test_heartbeat.py
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from taskqueue.task import task
import time


@task
def heartbeat_test(seconds):
    print(f"Task started. Sleeping for {seconds} seconds...")
    time.sleep(seconds)
    print("Task finished.")


if __name__ == "__main__":
    heartbeat_test.delay(30, priority=4)