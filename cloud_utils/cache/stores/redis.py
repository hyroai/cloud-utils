import functools
import json
import logging
from typing import Callable, Tuple

import redis.asyncio as redis

from cloud_utils.cache import utils


def redis_error_handler(f):
    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        try:
            await f(*args, **kwargs)
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ) as err:  # Could not connect to redis. This could be temporary. Ignore.
            logging.error(f"Got {str(err)} error")

    return wrapper


def make_store(
    redis_client: redis.Redis,
    name: str,
    ttl: int,
) -> Tuple[Callable, Callable]:
    utils.log_initialized_cache("redis", name)

    async def get_item(key: str):
        result = await redis_error_handler(
            redis_client.get(utils.cache_key_name(name, key)),
        )
        if result is None:
            logging.error(f"{key} is not in {name}")
            raise KeyError
        try:
            return json.loads(result)
        except ValueError:  # Key contents are malformed (will force key to update).
            logging.error(f"Malformed key detected: {key} in {name}.")
            raise KeyError

    async def set_item(key: str, value):
        if ttl == 0:
            await redis_error_handler(
                redis_client.set(utils.cache_key_name(name, key), json.dumps(value)),
            )
        else:
            await redis_error_handler(
                redis_client.setex(
                    utils.cache_key_name(name, key),
                    ttl,
                    json.dumps(value),
                ),
            )

    return get_item, set_item
