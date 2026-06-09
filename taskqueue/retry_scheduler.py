from datetime import datetime
import json
import time

from taskqueue.broker import redis_client

QUEUE_MAP = {
  4: "critical_queue",
  3: "high_queue",
  2: "medium_queue",
  1: "low_queue"
  }


def move_to_priority_queue(task_data):

    queue_name = QUEUE_MAP.get(task_data["priority"], "low_queue")

    redis_client.lpush(queue_name, json.dumps(task_data))

    return


def run_retry_scheduler():
    print("Retry Scheduler Started...")

    while True:

        result = redis_client.brpop("retry_queue")

        task_json = result[1]

        task_data = json.loads(task_json)

        scheduled_at = task_data.get("scheduled_at")

        if scheduled_at is None:
            continue

        scheduled_time = datetime.fromisoformat(
            scheduled_at
        )

        if datetime.now() >= scheduled_time:
            task_data.pop("scheduled_at", None)
            move_to_priority_queue(task_data)

        else:

            redis_client.lpush(
                "retry_queue",
                task_json
            )

            time.sleep(1)


if __name__ == "__main__":
    run_retry_scheduler()