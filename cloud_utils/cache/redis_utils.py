import json
import logging
from typing import Callable, Text, Tuple

import redis
import toolz


def get_redis_client(host: Text, password: Text) -> redis.Redis:
    return redis.Redis(
        host=host, port=6379, db=0, decode_responses=True, password=password
    )


def _get_redis_cache_key_name(environment: Text, cache_name: Text, key: Tuple) -> Text:
    return f"{environment}:{cache_name}:{json.dumps(key)}"


@toolz.curry
def make_redis_store(
    redis_client: redis.Redis, environment: Text, name: Text, **_
) -> Tuple[Callable, Callable]:
    logging.info(f"initializing redis cache for {name}")

    def get_item(key: Tuple):
        result = redis_client.get(_get_redis_cache_key_name(environment, name, key))
        if result is None:
            raise KeyError
        try:
            return json.loads(result)
        except ValueError:  # Key contents are malformed (will force key to update).
            logging.error(f"malformed key detected: {key}")
            raise KeyError

    def set_item(key: Tuple, value):
        try:
            redis_client.set(
                _get_redis_cache_key_name(environment, name, key), json.dumps(value)
            )
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ):  # Could not connect to redis. This could be temporary. Ignore.
            pass

    return get_item, set_item
