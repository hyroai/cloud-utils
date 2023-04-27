import redis.asyncio as redis


def make_store(host: str, port):
    r: redis.Redis

    async def connect():
        nonlocal r
        r = await redis.Redis(
            host,
            port,
        )
