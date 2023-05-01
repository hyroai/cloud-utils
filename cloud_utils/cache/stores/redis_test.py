from fakeredis import FakeServer, aioredis

from cloud_utils.cache.stores import redis

_SERVER = FakeServer()
_REDIS = aioredis.FakeRedis(server=_SERVER)


async def test_redis_store_unbounded():

    get_item, set_item = redis.make_store(_REDIS, "unbound_store", 0)

    await set_item("1", 1)
    await set_item("2", 2)
    await set_item("3", "3")

    assert await get_item("1") == 1
    assert await get_item("2") == 2
    assert await get_item("3") == "3"
