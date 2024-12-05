"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from .spark_config import create_spark_session
from .hive_metastore import get_metastore_properties

__all__ = ['create_spark_session', 'get_metastore_properties']
