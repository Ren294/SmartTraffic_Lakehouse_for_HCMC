from lakefs.client import Client

LAKEFS_CONFIG = {
    'host': 'http://lakefs:8000',
    'username': 'AKIAJ3ZOMDF3UF24HUGQ',
    'password': 'vYoxDXsQQEfor9/hM41KP3GPWptAXlvL+3eu0MZa'
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
