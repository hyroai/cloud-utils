import json
import logging
from typing import Callable, Text, Tuple

import redis
from cloud_utils import config

_REDIS_CLIENT = redis.Redis(
    host=config.EXTERNAL_API_CACHE_REDIS_HOST,
    port=6379,
    db=0,
    decode_responses=True,
    password=config.EXTERNAL_API_CACHE_REDIS_PASSWORD,
)


def _get_redis_cache_key_name(cache_name: Text, key: Tuple) -> Text:
    return f"{config.ENVIRONMENT}:{cache_name}:{json.dumps(key)}"


def make_redis_store(name: Text, **_) -> Tuple[Callable, Callable]:
    logging.info(f"initializing redis cache for {name}")

    def get_item(key: Tuple):
        result = _REDIS_CLIENT.get(_get_redis_cache_key_name(name, key))
        if result is None:
            raise KeyError
        try:
            return json.loads(result)
        except ValueError:  # Key contents are malformed (will force key to update).
            logging.error(f"malformed key detected: {key}")
            raise KeyError

    def set_item(key: Tuple, value):
        try:
            _REDIS_CLIENT.set(_get_redis_cache_key_name(name, key), json.dumps(value))
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ):  # Could not connect to redis. This could be temporary. Ignore.
            pass

    return get_item, set_item
