import os

import redis

BUFFER_QUEUE = "buffer_queue"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
