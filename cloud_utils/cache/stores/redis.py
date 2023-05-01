import json
import logging
from typing import Callable, Tuple

import redis


def _cache_key_name(cache_name: str, key: Tuple) -> str:
    return f"{cache_name}:{json.dumps(key)}"


def redis_error_handler(f):
    def call_f(args):
        try:
            f(args)
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ) as err:  # Could not connect to redis. This could be temporary. Ignore.
            logging.error(f"Got {str(err)} error")

    return call_f


def make_store(
    redis_client: redis.Redis,
    name: str,
    ttl,
) -> Tuple[Callable, Callable]:
    def get_item(key: Tuple):
        result = redis_error_handler(redis_client.get(_cache_key_name(name, key)))
        if result is None:
            logging.error(f"{key} is not in {name}")
            raise KeyError
        try:
            return json.loads(result)
        except ValueError:  # Key contents are malformed (will force key to update).
            logging.error(f"Malformed key detected: {key} in {name}.")
            raise KeyError

    def set_item(key: Tuple, value):
        if ttl == 0:
            redis_error_handler(
                redis_client.set(_cache_key_name(name, key), json.dumps(value)),
            )
        else:
            redis_error_handler(
                redis_client.setex(
                    _cache_key_name(name, key),
                    ttl,
                    json.dumps(value),
                ),
            )

    return get_item, set_item
