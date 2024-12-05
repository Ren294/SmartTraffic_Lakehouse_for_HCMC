"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
import redis

REDIS_CONFIG = {
    'host': 'redis_traffic',
    'port': 6379,
    'db': 0
}

_redis_client = None


def get_redis_client():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(**REDIS_CONFIG)
    return _redis_client
