import aiohttp
import requests_cache
from ..config.settings import HEADERS, REQUEST_TIMEOUT, CACHE_PATH


async def setup_session():
    """Configura sesión HTTP con cache"""
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    return aiohttp.ClientSession(
        headers=HEADERS,
        connector=connector,
        timeout=timeout
    )


def setup_cached_session():
    """Configura sesión con cache para requests sincrónicos"""
    return requests_cache.CachedSession(
        str(CACHE_PATH),
        expire_after=3600
    )