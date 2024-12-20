"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from .redis_connector import get_redis_client
from .postgres_connector import get_postgres_connection
__all__ = ['get_redis_client', 'get_postgres_connection']
