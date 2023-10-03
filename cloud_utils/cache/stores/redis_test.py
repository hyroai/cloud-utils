import asyncio
import json
from typing import Callable

import gamla
from fakeredis import FakeServer, FakeStrictRedis, aioredis

from cloud_utils.cache import utils
from cloud_utils.cache.stores import redis, redis_sync

_SERVER = FakeServer()


def _make_sync_fake_redis_client():
    return FakeStrictRedis(server=_SERVER)


def _make_async_fake_redis_client():
    return aioredis.FakeRedis(server=_SERVER)


async def test_redis_store_unbounded():
    get_item, set_item = redis.make_store(
        _make_async_fake_redis_client,
        0,
        0,
        "unbound_store",
        json.dumps,
        json.loads,
    )

    await set_item("1", 1)
    await set_item("2", 2)
    await set_item("3", "3")

    assert await get_item("1") == 1
    assert await get_item("2") == 2
    assert await get_item("3") == "3"


_ttl_by_value: Callable[[str], int] = gamla.ternary(
    gamla.equals("1"), gamla.just(1), gamla.just(5)
)


async def test_redis_store_ttl():
    get_item, set_item = redis.make_store_with_custom_ttl(
        _make_async_fake_redis_client,
        5,
        _ttl_by_value,
        "ttl_1",
        json.dumps,
        json.loads,
    )

    await set_item("1", 1)
    await set_item("2", 2)
    await asyncio.sleep(2)
    assert await get_item("2") == 2
    try:
        await get_item("1")
    except KeyError:
        assert (
            utils.cache_key_name("ttl_1", "1")
            not in await _make_async_fake_redis_client().keys()
        )


async def test_redis_store_custom_ttl():
    get_item, set_item = redis.make_store(
        _make_async_fake_redis_client,
        5,
        1,
        "ttl_1",
        json.dumps,
        json.loads,
    )

    await set_item("1", 1)
    await asyncio.sleep(2)
    try:
        await get_item("1")
    except KeyError:
        assert (
            utils.cache_key_name("ttl_1", "1")
            not in await _make_async_fake_redis_client().keys()
        )


def test_sync_redis_store_unbounded():
    get_item, set_item = redis_sync.make_store(
        lambda: FakeStrictRedis(server=_SERVER),
        0,
        "unbound_store",
        json.dumps,
        json.loads,
    )

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", "3")

    assert get_item("1") == 1
    assert get_item("2") == 2
    assert get_item("3") == "3"


def test_redis_sync_store_ttl():
    get_item, set_item = redis_sync.make_store(
        _make_sync_fake_redis_client,
        1,
        "ttl_1",
        json.dumps,
        json.loads,
    )

    set_item("1", 1)
    try:
        get_item("1")
    except KeyError:
        assert (
            utils.cache_key_name("ttl_1", "1")
            not in _make_sync_fake_redis_client().keys()
        )
