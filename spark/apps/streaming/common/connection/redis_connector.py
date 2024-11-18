import redis

REDIS_CONFIG = {
    'host': 'redis_traffic',
    'port': 6379,
    'db': 0
}

_redis_client = None


def get_redis_client():
    """
    Returns a singleton Redis client instance
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(**REDIS_CONFIG)
    return _redis_client
