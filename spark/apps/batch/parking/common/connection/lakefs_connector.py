"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from lakefs.client import Client

LAKEFS_CONFIG = {
    'host': 'http://lakefs:8000',
    'username': 'AKIAJC5AQUW4OXQYCRAQ',
    'password': 'iYf4H8GSjY+HMfN70AMquj0+PRpYcgUl0uN01G7Z'
}

_lakefs_client = None


def get_lakefs_client():

    global _lakefs_client
    if _lakefs_client is None:
        _lakefs_client = Client(**LAKEFS_CONFIG)
    return _lakefs_client


def get_lakefs():
    return LAKEFS_CONFIG
