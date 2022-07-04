import functools
import cachetools


def async_ttl_cache(ttl: int, maxsize: int):
    cache: cachetools.TTLCache = cachetools.TTLCache(ttl=ttl, maxsize=maxsize)

    def decorator(fn):
        @functools.wraps(fn)
        async def memoize(*args, **kwargs):
            key = str((args, kwargs))
            try:
                cache[key] = cache.pop(key)
            except KeyError:
                cache[key] = await fn(*args, **kwargs)
            return cache[key]

        return memoize

    return decorator
