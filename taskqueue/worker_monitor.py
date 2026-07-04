from taskqueue.broker import redis_client
from datetime import datetime
import time

from taskqueue.recovery_scheduler import recover_task, QUEUE_MAP
from taskqueue.database.repositery import upsert_worker
import json

HEARTBEAT_TIMEOUT = 15
HEARTBEAT_INTERVAL = 5

def run_worker_monitor():
  while True:

    workers = redis_client.hgetall(
      "workers"
    )

    for worker_id, status in workers.items():

      if status == "DEAD":
        continue

      metadata = redis_client.hgetall(
          f"worker:{worker_id}"
      )

      if not metadata:
        continue

      last_heartbeat = datetime.fromisoformat(
        metadata["last_heartbeat"]
      )

      age = (
          datetime.now() - last_heartbeat
      ).total_seconds()

      if age > HEARTBEAT_TIMEOUT:
        redis_client.hset(
          "workers",
          worker_id,
          "DEAD"
        )

        print(f"Dead worker detected: {worker_id}")

        redis_client.hset(
          f"worker:{worker_id}",
          "status",
          "DEAD"
        )
        upsert_worker(
          worker_id,
          "DEAD"
        )

        task_ids = redis_client.smembers(f"worker_tasks:{worker_id}")

        for task_id in task_ids:

          processing_metadata = redis_client.hgetall(
              f"processing:{task_id}"
          )

          if not processing_metadata:
              continue

          processing_queue = processing_metadata[
              "processing_queue"
          ]

          original_queue = QUEUE_MAP[
              processing_queue
          ]

          tasks = redis_client.lrange(
              processing_queue,
              0,
              -1
          )

          for task_json in tasks:

              task_data = json.loads(task_json)

              if task_data["task_id"] == task_id:

                  recover_task(
                      task_json,
                      task_id,
                      processing_queue,
                      original_queue
                  )

                  redis_client.srem(
                      f"worker_tasks:{worker_id}",
                      task_id
                  )

                  print(
                      f"Recovered task {task_id} from dead worker {worker_id}"
                  )

                  break

        redis_client.delete(
            f"worker_tasks:{worker_id}"
        )

    time.sleep(5)


if __name__ == "__main__":
  run_worker_monitor()