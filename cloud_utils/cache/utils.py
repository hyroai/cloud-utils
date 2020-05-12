import asyncio
import datetime
import functools
import inspect
import json
import logging
import pathlib
from typing import Any, Callable, Text

import async_lru
import gamla
import redis
import toolz
from toolz import curried

from cloud_utils import storage
from cloud_utils.cache import file_store, redis_utils
from cloud_utils.storage import utils as storage_utils

HASH_VERSION = "hash_version"
_LAST_RUN_TIMESTAMP = "last_run_timestamp"


@toolz.curry
def _save_to_blob(item_name: Text, obj: Any, bucket_name: Text):
    storage.upload_blob(bucket_name, storage_utils.hash_to_filename(item_name), obj)


@toolz.curry
def _write_to_versions_file(versions_file, deployment_name: Text, hash_to_load: Text):
    versions = toolz.assoc(
        json.load(versions_file),
        deployment_name,
        {
            HASH_VERSION: hash_to_load,
            _LAST_RUN_TIMESTAMP: datetime.datetime.now().isoformat(),
        },
    )
    json.dump(versions, versions_file, indent=2)


def save_to_bucket_return_hash(
    cache_dir: pathlib.Path, bucket_name: Text, environment: Text
):
    return toolz.compose_left(
        gamla.pair_with(gamla.compute_stable_json_hash),
        curried.do(gamla.star(_save_to_blob(bucket_name))),
        curried.do(gamla.star(file_store.save_local(cache_dir)))
        if environment == "local"
        else toolz.identity,
        toolz.first,
        gamla.log_text("Saved hash {}"),
    )


def auto_updating_cache(
    factory: Callable, update: bool, versions_file_path: Text
) -> Callable:
    versions = toolz.pipe(versions_file_path, file_store.open_file, json.load)

    # Deployment name is the concatenation of caller's module name and factory's function name.
    deployment_name = (
        f"{inspect.stack()[1].frame.f_locals['__name__']}.{factory.__name__}"
    )

    if deployment_name in versions and (
        not update
        or datetime.datetime.now()
        - datetime.datetime.fromisoformat(
            versions[deployment_name][_LAST_RUN_TIMESTAMP]
        )
        <= datetime.timedelta(days=1)
    ):
        return gamla.just(versions[deployment_name][HASH_VERSION])

    logging.info(f"Updating version '{deployment_name}'")
    try:
        if asyncio.iscoroutinefunction(factory):
            hash_to_load = gamla.run_sync(
                gamla.compose_left(factory, save_to_bucket_return_hash)()
            )
        else:
            hash_to_load = gamla.compose_left(factory, save_to_bucket_return_hash)()
    except Exception as e:
        if deployment_name in versions:
            hash_to_load = versions[deployment_name][HASH_VERSION]
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
    is_external: bool = False,
    num_misses_to_trigger_sync: int = 100,
    environment: Text = "local",
) -> Callable:

    maxsize = 10000

    def simple_decorator(func):
        if inspect.iscoroutinefunction(func):
            return async_lru.alru_cache(maxsize=maxsize)(func)
        return functools.lru_cache(maxsize=maxsize)(func)

    if not is_external and environment in ("production", "staging", "development"):
        return simple_decorator

    if environment in ("production", "staging", "development"):
        _resolve_cache_store = redis_utils.make_redis_store(redis_client, environment)
    else:
        _resolve_cache_store = file_store.make_file_store

    get_cache_item, set_cache_item = _resolve_cache_store(
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
