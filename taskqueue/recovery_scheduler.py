from datetime import datetime
import time
import json

from taskqueue.broker import redis_client
from taskqueue.task_status import set_task_status
from taskqueue.status import TaskStatus

PROCESSING_TIMEOUT = 60

PROCESSING_QUEUES = [
    "critical_processing",
    "high_processing",
    "medium_processing",
    "low_processing"
]

QUEUE_MAP = {
    "critical_processing": "critical_queue",
    "high_processing": "high_queue",
    "medium_processing": "medium_queue",
    "low_processing": "low_queue"
}


def run_recovery_scheduler():
  print("Recovery Scheduler Started...")

  while True:
    for processing_queue in PROCESSING_QUEUES:
      tasks = redis_client.lrange(
        processing_queue,
        0,
        -1
      )

      for task_json in tasks:
        task_data = json.loads(task_json)
        task_id = task_data["task_id"]

        metadata = redis_client.hgetall(
          f"processing:{task_id}"
        )

        if not metadata:
          continue


        started_at = datetime.fromisoformat(
          metadata["started_at"]
        )

        age = (datetime.now() - started_at).total_seconds()

        if age <= PROCESSING_TIMEOUT:
          continue

        original_queue = QUEUE_MAP[processing_queue]

        redis_client.lrem(
          processing_queue,
          1,
          task_json
          )

        redis_client.delete(
          f"processing:{task_id}"
        )

        set_task_status(
          task_id,
          TaskStatus.PENDING
        )

        redis_client.lpush(
            original_queue,
            task_json
        )

        print(f"Recovered task {task_id}")

    time.sleep(5)


if __name__ == "__main__":
    run_recovery_scheduler()
