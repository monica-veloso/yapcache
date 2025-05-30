import asyncio
from functools import wraps

from typing_extensions import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Optional,
    ParamSpec,
    TypeVar,
    Concatenate,
)

from yapcache.cache_item import CacheItem
from yapcache.caches import Cache
from yapcache.distlock import DistLock, NullLock

P = ParamSpec("P")
R = TypeVar("R")


def memoize(
    cache: Cache,
    cache_key: Callable[P, str],
    ttl: float | Callable[Concatenate[R, P], float],
    best_before: Callable[Concatenate[R, P], Optional[float]] = lambda _, *a, **kw: None,
    lock: Callable[[str], DistLock] = lambda *a, **kw: NullLock(),
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    update_tasks: dict[str, asyncio.Task] = {}

    def decorator(fn: Callable[P, Awaitable[R]]):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            key = cache_key(*args, **kwargs)

            async def _call_with_lock():
                async with lock(key + ":lock"):
                    found = await cache.get(key)   # did someone populate the cache while I was waiting for the lock?
                    if isinstance(found, CacheItem) and not found.is_stale:
                        return found.value

                    result = await fn(*args, **kwargs)

                    await cache.set(
                        key,
                        value=result,
                        ttl=ttl(result, *args, **kwargs) if callable(ttl) else ttl,
                        best_before=best_before(result, *args, **kwargs),
                    )

                    return result

            found = await cache.get(key)
            if isinstance(found, CacheItem):
                if found.is_stale and key not in update_tasks:
                    task = asyncio.create_task(_call_with_lock())
                    update_tasks[key] = task  # TODO: acho que tem problema
                    task.add_done_callback(lambda _: update_tasks.pop(key))
                return found.value

            result = await _call_with_lock()

            return result

        return wrapper

    return decorator
