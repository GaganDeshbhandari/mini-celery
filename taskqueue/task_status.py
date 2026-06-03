from taskqueue.broker import redis_client


def _build_status_key(task_id):
  return f"task_status:{task_id}"

def set_task_status(task_id, status):
  key = _build_status_key(task_id)
  redis_client.set(key,status)

def get_task_status(task_id):
  key = _build_status_key(task_id)
  status = redis_client.get(key)
  return status


