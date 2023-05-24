import asyncio

from fakeredis import FakeServer, FakeStrictRedis, aioredis

from cloud_utils.cache import utils
from cloud_utils.cache.stores import redis, redis_sync

_SERVER = FakeServer()


async def test_redis_store_unbounded():
    client = aioredis.FakeRedis(server=_SERVER)
    get_item, set_item = redis.make_store(client, 0, "unbound_store")

    await set_item("1", 1)
    await set_item("2", 2)
    await set_item("3", "3")

    assert await get_item("1") == 1
    assert await get_item("2") == 2
    assert await get_item("3") == "3"


async def test_redis_store_ttl():
    client = aioredis.FakeRedis(server=_SERVER)
    get_item, set_item = redis.make_store(client, 1, "ttl_1")

    await set_item("1", 1)
    await asyncio.sleep(2)
    try:
        await get_item("1")
    except KeyError:
        assert utils.cache_key_name("ttl_1", "1") not in await client.keys()


def test_sync_redis_store_unbounded():
    client = FakeStrictRedis(server=_SERVER)
    get_item, set_item = redis_sync.make_store(client, 0, "unbound_store")

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", "3")

    assert get_item("1") == 1
    assert get_item("2") == 2
    assert get_item("3") == "3"


def test_redis_sync_store_ttl():
    client = FakeStrictRedis(server=_SERVER)
    get_item, set_item = redis_sync.make_store(client, 1, "ttl_1")

    set_item("1", 1)
    try:
        get_item("1")
    except KeyError:
        assert utils.cache_key_name("ttl_1", "1") not in client.keys()
