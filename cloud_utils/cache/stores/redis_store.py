import json
import logging
from typing import Callable, Tuple, Union

import redis
import redis.asyncio as redis_async


def _cache_key_name(cache_name: str, key: Tuple) -> str:
    return f"{cache_name}:{json.dumps(key)}"


def make_store(
    redis_client: Union[redis.Redis, redis_async.Redis],
    name: str,
    ttl: int = 0,
) -> Tuple[Callable, Callable]:
    def get_item(key: Tuple):
        result = redis_client.get(_cache_key_name(name, key))
        if result is None:
            logging.error(f"{key} is not in {name}")
            raise KeyError
        try:
            return json.loads(result)
        except ValueError:  # Key contents are malformed (will force key to update).
            logging.error(f"Malformed key detected: {key} in {name}.")
            raise KeyError

    def set_item(key: Tuple, value):
        try:
            redis_client.setex(
                _cache_key_name(name, key),
                ttl,
                json.dumps(value),
            )
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ):  # Could not connect to redis. This could be temporary. Ignore.
            pass

    return get_item, set_item
