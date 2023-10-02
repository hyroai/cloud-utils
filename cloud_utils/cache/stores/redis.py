import logging
from typing import Any, Callable, Tuple, Union

import gamla
import redis.asyncio as redis

from cloud_utils.cache import utils


def redis_error_handler(f):
    async def wrapper(*args, **kwargs):
        try:
            result = await f(*args, **kwargs)
            return result
        except (
            redis.ConnectionError,
            redis.TimeoutError,
        ) as err:  # Could not connect to redis. This could be temporary. Ignore.
            logging.error(f"redis: got {str(err)} error")

    return wrapper


def make_store_with_custom_ttl(
    make_redis_client: Callable[[], redis.Redis],
    max_parallelism: int,
    ttl: Callable[[Any], int],
    name: str,
    encoder: Callable[[Any], Any],
    decoder: Callable[[Any], Any],
) -> Tuple[Callable, Callable]:
    redis_client = None
    utils.log_initialized_cache("redis", name)

    def get_redis_client():
        nonlocal redis_client
        if redis_client is None:
            redis_client = make_redis_client()

        return redis_client

    async def get_item(key: str):
        cache_key = utils.cache_key_name(name, key)
        result = await redis_error_handler(get_redis_client().get)(cache_key)
        if result is None:
            logging.debug(f"{key} is not in {name}")
            raise KeyError
        try:
            return decoder(result)
        except ValueError:  # Key contents are malformed (will force key to update).
            logging.error(f"Malformed key detected: {key} in {name}.")
            raise KeyError

    async def set_item(key: str, value):
        ttl_value = ttl(value)
        value = encoder(value)
        if ttl_value == 0:
            await redis_error_handler(
                get_redis_client().set,
            )(utils.cache_key_name(name, key), value)
        else:
            await redis_error_handler(get_redis_client().setex)(
                utils.cache_key_name(name, key),
                ttl_value,
                value,
            )

    # When using a redis async client, we are limited to the amount of connections we can create.
    # Usually the cached function `f` will be throttled, meaning we can exhaust all connections on `get` operations (`get` happens before `f`).
    # In order to allow `set` operations to also occur in parallel we split the get/set operations proportionally to `max_parallelism`.
    if max_parallelism > 0:
        allowed_get_operations = round(0.8 * max_parallelism)
        get_item = gamla.throttle(allowed_get_operations, get_item)
        set_item = gamla.throttle(max_parallelism - allowed_get_operations, set_item)

    return get_item, set_item


def make_store(
    make_redis_client: Callable[[], redis.Redis],
    max_parallelism: int,
    ttl: int,
    name: str,
    encoder: Callable[[Any], Any],
    decoder: Callable[[Any], Any],
) -> Tuple[Callable, Callable]:
    return make_store_with_custom_ttl(make_redis_client, max_parallelism, gamla.just(ttl), name, encoder, decoder)
