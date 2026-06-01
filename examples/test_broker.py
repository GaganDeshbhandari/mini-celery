import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from taskqueue.broker import ping_broker, redis_client


if __name__ == "__main__":
    print("redis_client import ok:", redis_client is not None)
    print("ping_broker:", ping_broker())
