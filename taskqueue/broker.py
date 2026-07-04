"""Redis broker client."""

import redis



redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True,
)



"""Checks whether the redis_client is connected or not"""
def ping_broker():
    try:
        return redis_client.ping()
    except redis.ConnectionError:
        return False
