import pytest
from yapcache.caches.memory import InMemoryCache
from yapcache.caches.redis import RedisCache
from yapcache.caches import MultiLayerCache
from redis.asyncio import Redis
from yapcache.cache_item import NOT_FOUND, CacheItem

memory_cache = InMemoryCache(maxsize=10_000)


@pytest.mark.asyncio
async def test_cache_multilayer_redis_and_memory():
    key = 'test'
    value = 42
    ttl = 60

    redis_client = Redis()
    redis_cache = RedisCache(redis_client)

    cache = MultiLayerCache(caches=[redis_cache, memory_cache])

    await cache.set(key, value, ttl=ttl)

    result = await cache.get(key)

    assert isinstance(result, CacheItem)
    assert result.value == value

    # Test getting a non-existing key
    result = await cache.get("non_existing_key")
    assert result == NOT_FOUND

    await cache.close()
    await redis_client.aclose()
