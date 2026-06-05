"""Dead letter queue helpers."""

from taskqueue.broker import redis_client


DLQ_KEY = "dead_letter_queue"


def push_to_dlq(task_json):
    redis_client.lpush(DLQ_KEY, task_json)


def get_dlq_tasks(start=0, end=-1):
    return redis_client.lrange(DLQ_KEY, start, end)


def get_dlq_length():
    return redis_client.llen(DLQ_KEY)
