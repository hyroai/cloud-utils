import logging
from typing import Any, Callable, Tuple

import redis

from cloud_utils.cache import utils


def _redis_error_handler(f):
    def wrapper(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
            return result
        except (
            redis.ConnectionError,
            redis.TimeoutError,
        ) as err:  # Could not connect to redis. This could be temporary. Ignore.
            logging.error(f"Got {str(err)} error")

    return wrapper


def make_store(
    make_redis_client: Callable[[], redis.Redis],
    ttl: int,
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

    def get_item(key: str):
        cache_key = utils.cache_key_name(name, key)
        result = _redis_error_handler(get_redis_client().get)(cache_key)
        if result is None:
            logging.debug(f"{key} is not in {name}")
            raise KeyError
        try:
            return decoder(result)
        except ValueError:  # Key contents are malformed (will force key to update).
            logging.error(f"Malformed key detected: {key} in {name}.")
            raise KeyError

    def set_item(key: str, value):
        value = encoder(value)
        if ttl == 0:
            _redis_error_handler(get_redis_client().set)(
                utils.cache_key_name(name, key),
                value,
            )
        else:
            _redis_error_handler(get_redis_client().setex)(
                utils.cache_key_name(name, key),
                ttl,
                value,
            )

    return get_item, set_item
