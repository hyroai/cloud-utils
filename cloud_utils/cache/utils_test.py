import asyncio
import collections
import functools

import gamla

from cloud_utils.cache.stores import lru_memory


def make_async_store():
    store = collections.OrderedDict()

    async def get_item(key):
        return store[key]

    async def set_item(key, value):
        store[key] = value

    return get_item, set_item


class _CalledTooManyTimes(Exception):
    pass


def assert_max_called(n: int):
    def decorator(f):
        count = 0
        if asyncio.iscoroutinefunction(f):

            @functools.wraps(f)
            async def wrapped(*args, **kwargs):
                nonlocal count
                count += 1
                if count > n:
                    raise _CalledTooManyTimes()
                return await f(*args, **kwargs)

            return wrapped

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            nonlocal count
            count += 1
            if count > n:
                raise _CalledTooManyTimes()
            return f(*args, **kwargs)

        return wrapped

    return decorator


def test_cache():

    get_item, set_item = lru_memory.make_store("some store", 2)

    @gamla.persistent_cache(
        get_item,
        set_item,
        gamla.make_hashed_call_key("test_cache"),
    )
    @assert_max_called(2)
    def f(a: int, b: int, c: int, d: int):
        return a + b + c + d

    assert f(1, 2, c=3, d=4) == 10
    assert f(1, 2, 3, d=4) == 10
    assert f(1, 2, c=3, d=4) == 10


async def test_cache_async():

    get_item, set_item = lru_memory.make_async_store("some other store", 2)

    @gamla.persistent_cache(
        get_item,
        set_item,
        gamla.make_hashed_call_key("test_cache"),
    )
    @assert_max_called(2)
    async def f(a: int, b: int):
        await asyncio.sleep(0.001)
        return a + b

    assert (await f(1, b=2)) == 3
    assert (await f(1, b=2)) == 3
    assert (await f(2, b=2)) == 4
