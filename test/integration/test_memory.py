from __future__ import annotations

from aiohttp import ClientSession
from aiohttp_client_cache.backends.base import CacheBackend, DictCache
from test.conftest import httpbin
from test.integration import BaseBackendTest, BaseStorageTest


class TestMemoryBackend(BaseBackendTest):
    """Run tests for CacheBackend base class, which uses in-memory caching by default"""

    backend_class = CacheBackend

    async def test_content_reset(self):
        """Test that cached response content can be read multiple times (without consuming and
        re-reading the same file-like object)
        """
        url = httpbin('get')
        async with ClientSession() as session:
            async with session.get(url) as resp:
                aiohttp_content = await resp.read()
                assert aiohttp_content
        
        async with self.init_session() as session:
            original_response = await session.get(url)
            original_content = await original_response.read()
            assert original_content == aiohttp_content

            cached_response_1 = await session.get(url)
            assert cached_response_1.from_cache
            content_1 = await cached_response_1.read()
            assert content_1
            cached_response_2 = await session.get(url)
            content_2 = await cached_response_2.read()
            assert content_1 == content_2 == original_content

    async def test_without_contextmanager(self):
        """Test that the cache backend can be safely used without the CachedSession contextmanager.
        An "unclosed ClientSession" warning is expected here, however.
        """
        session = await self._init_session()
        await session.get(httpbin('get'))
        del session

    # Serialization tests don't apply to in-memory cache
    async def test_serializer__pickle(self):
        pass

    async def test_serializer__itsdangerous(self):
        pass


class TestMemoryCache(BaseStorageTest):
    storage_class = DictCache
