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
from toolz import curried
from toolz.curried import operator

from cloud_utils.cache import file_store, redis_utils

_HASH_VERSION_KEY = "hash_version"
_LAST_RUN_TIMESTAMP = "last_run_timestamp"


class VersionNotFound(Exception):
    pass


@gamla.curry
def _write_to_versions_file(deployment_name: Text, hash_to_load: Text, versions_file):
    versions = toolz.pipe(
        {
            _HASH_VERSION_KEY: hash_to_load,
            _LAST_RUN_TIMESTAMP: datetime.datetime.now().isoformat(),
        },
        curried.assoc(json.load(versions_file), deployment_name),
        dict.items,
        curried.sorted,
        dict,
    )

    versions_file.seek(0)
    json.dump(versions, versions_file, indent=2)
    versions_file.truncate()


@gamla.curry
def _write_hash_to_versions_file(
    versions_file_name: Text, deployment_name: Text, hash_to_load: Text,
):
    return toolz.pipe(
        versions_file_name,
        file_store.open_file(mode="r+"),
        _write_to_versions_file(deployment_name, hash_to_load),
    )


def _should_update(deployment_name: Text, update: bool, force_update: bool) -> bool:
    return gamla.anyjuxt(
        gamla.just(force_update),
        gamla.alljuxt(
            gamla.just(update),
            gamla.compose_left(
                curried.get_in([deployment_name, _LAST_RUN_TIMESTAMP]),
                gamla.anyjuxt(
                    operator.eq(None),
                    gamla.compose_left(
                        datetime.datetime.fromisoformat,
                        lambda x: datetime.datetime.now() - x
                        > datetime.timedelta(days=1),
                    ),
                ),
            ),
        ),
    )


def auto_updating_cache(
    factory: Callable,
    update: bool,
    versions_file_path: Text,
    environment: Text,
    bucket_name: Text,
    force_update: bool,
    frame_level: int = 2,
) -> Callable:

    # Deployment name is the concatenation of caller's module name and factory's function name.
    deployment_name = (
        f"{inspect.stack()[frame_level].frame.f_globals['__name__']}.{factory.__name__}"
    )

    logging.info(f"Fetching deployment name '{deployment_name}'")
    return gamla.compose_left(
        gamla.just(versions_file_path),
        file_store.open_file,
        json.load,
        gamla.ternary(
            _should_update(deployment_name, update, force_update),
            gamla.compose_left(
                gamla.ignore_input(factory),
                file_store.save_to_bucket_return_hash(environment, bucket_name),
                curried.do(
                    _write_hash_to_versions_file(versions_file_path, deployment_name),
                ),
                gamla.log_text(f"Version '{deployment_name}' has been updated"),
            ),
            gamla.compose_left(
                gamla.check(
                    gamla.inside(deployment_name), VersionNotFound(deployment_name),
                ),
                curried.get_in([deployment_name, _HASH_VERSION_KEY]),
            ),
        ),
    )


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
            redis_client, environment, name,
        )
    else:
        get_cache_item, set_cache_item = file_store.make_file_store(
            name, num_misses_to_trigger_sync,
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
