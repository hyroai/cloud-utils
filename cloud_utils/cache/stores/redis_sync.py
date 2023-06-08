import json
import logging
from typing import Callable, Tuple

import redis

from cloud_utils.cache import utils
import zlib
import gamla


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


def _make_store(
    zip_content: bool, redis_client: redis.Redis, ttl: int, name: str, force: bool
) -> Tuple[Callable, Callable]:
    utils.log_initialized_cache("redis", name)

    def get_item(key: str):
        if force:
            raise KeyError
        cache_key = utils.cache_key_name(name, key)
        result = _redis_error_handler(redis_client.get)(cache_key)
        if result is None:
            logging.debug(f"{key} is not in {name}")
            raise KeyError
        try:
            if zip_content:
                result = gamla.pipe(
                    result, zlib.decompress, lambda x: x.decode("utf-8"), json.dumps
                )
            return json.loads(result)
        except ValueError:  # Key contents are malformed (will force key to update).
            logging.error(f"Malformed key detected: {key} in {name}.")
            raise KeyError

    def set_item(key: str, value):
        if ttl == 0:
            _redis_error_handler(
                redis_client.set,
            )(utils.cache_key_name(name, key), json.dumps(value))
        else:
            value = zlib.compress(value) if zip_content else json.dumps(value)
            _redis_error_handler(redis_client.setex)(
                utils.cache_key_name(name, key),
                ttl,
                value,
            )

    return get_item, set_item


def make_store(
    redis_client: redis.Redis, ttl: int, name: str, force: bool = False
) -> Tuple[Callable, Callable]:
    return _make_store(False, redis_client, ttl, name, force)


def make_store_zip(
    redis_client: redis.Redis, ttl: int, name: str, force: bool = False
) -> Tuple[Callable, Callable]:
    return _make_store(True, redis_client, ttl, name, force)
