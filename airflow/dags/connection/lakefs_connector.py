from lakefs.client import Client

LAKEFS_CONFIG = {
    'host': 'http://lakefs:8000',
    'username': 'AKIAJC5AQUW4OXQYCRAQ',
    'password': 'iYf4H8GSjY+HMfN70AMquj0+PRpYcgUl0uN01G7Z'
}

_lakefs_client = None


def get_lakefs_client():
    """
    Returns a singleton LakeFS client instance
    Uses the new High Level SDK client

    Returns:
        Client: Configured lakeFS client instance
    """
    global _lakefs_client
    if _lakefs_client is None:
        _lakefs_client = Client(**LAKEFS_CONFIG)
    return _lakefs_client


def get_lakefs():
    return LAKEFS_CONFIG
