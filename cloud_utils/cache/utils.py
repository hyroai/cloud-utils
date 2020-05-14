import asyncio
import datetime
import functools
import inspect
import json
import logging
from typing import Callable, Text

import async_lru
import gamla
import redis
import toolz

from cloud_utils.cache import file_store, redis_utils

_HASH_VERSION_KEY = "hash_version"
_LAST_RUN_TIMESTAMP = "last_run_timestamp"


@gamla.curry
def _write_to_versions_file(versions_file, deployment_name: Text, hash_to_load: Text):
    versions = toolz.assoc(
        json.load(versions_file),
        deployment_name,
        {
            _HASH_VERSION_KEY: hash_to_load,
            _LAST_RUN_TIMESTAMP: datetime.datetime.now().isoformat(),
        },
    )
    json.dump(versions, versions_file, indent=2)


def auto_updating_cache(
    factory: Callable,
    update: bool,
    versions_file_path: Text,
    environment: Text,
    bucket_name: Text,
    frame_level: int = 2,
) -> Callable:
    versions = toolz.pipe(versions_file_path, file_store.open_file, json.load)

    # Deployment name is the concatenation of caller's module name and factory's function name.
    deployment_name = (
        f"{inspect.stack()[frame_level].frame.f_code.co_filename}::{factory.__name__}"
    )

    if deployment_name in versions and (
        not update
        or datetime.datetime.now()
        - datetime.datetime.fromisoformat(
            versions[deployment_name][_LAST_RUN_TIMESTAMP]
        )
        <= datetime.timedelta(days=1)
    ):
        return gamla.just(versions[deployment_name][_HASH_VERSION_KEY])

    logging.info(f"Updating version '{deployment_name}'")
    try:
        _save_to_bucket_return_hash = file_store.save_to_bucket_return_hash(
            environment, bucket_name
        )
        if asyncio.iscoroutinefunction(factory):
            hash_to_load = gamla.run_sync(
                gamla.compose_left(factory, _save_to_bucket_return_hash)()
            )
        else:
            hash_to_load = gamla.compose_left(factory, _save_to_bucket_return_hash)()
    except Exception as e:
        if deployment_name in versions:
            hash_to_load = versions[deployment_name][_HASH_VERSION_KEY]
            logging.error(
                f"Unable to update version '{deployment_name}'. Using old hash {hash_to_load} created on {versions[deployment_name][_LAST_RUN_TIMESTAMP]}."
            )
        else:
            raise e

    toolz.pipe(
        versions_file_path,
        file_store.open_file("r+"),
        _write_to_versions_file(deployment_name, hash_to_load),
    )

    return gamla.just(hash_to_load)


_ORDERED_SEQUENCE_TYPES = (list, tuple)


def _get_origin_type(type_hint):
    """Get native type for subscripted type hints, e.g. List[int] -> list, Tuple[float] -> tuple. """
    try:
        return type_hint.__origin__
    except AttributeError:
        return type_hint


def persistent_cache(
    redis_client: redis.Redis,
    name: Text,
    environment: Text,
    is_external: bool,
    num_misses_to_trigger_sync: int,
) -> Callable:

    maxsize = 10_000

    def simple_decorator(func):
        if inspect.iscoroutinefunction(func):
            return async_lru.alru_cache(maxsize=maxsize)(func)
        return functools.lru_cache(maxsize=maxsize)(func)

    if not is_external and environment in ("production", "staging", "development"):
        return simple_decorator

    if environment in ("production", "staging", "development"):
        get_cache_item, set_cache_item = redis_utils.make_redis_store(
            redis_client, environment, name
        )
    else:
        get_cache_item, set_cache_item = file_store.make_file_store(
            name, num_misses_to_trigger_sync
        )

    def decorator(func):
        @functools.wraps(func)
        async def wrapper_async(*args, **kwargs):
            key = gamla.make_call_key(args, kwargs)
            try:
                return get_cache_item(key)
            except KeyError:
                result = await func(*args, **kwargs)
                set_cache_item(key, result)
                return result

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = gamla.make_call_key(args, kwargs)
            try:
                return get_cache_item(key)
            except KeyError:
                result = func(*args, **kwargs)
                set_cache_item(key, result)
                return result

        if inspect.iscoroutinefunction(func):
            return wrapper_async
        return wrapper

    return decorator
