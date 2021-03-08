import datetime
import functools
import inspect
import json
import logging
from typing import Callable, Dict, Text

import async_lru
import gamla
import redis

from cloud_utils.cache import file_store, redis_utils

_HASH_VERSION_KEY = "hash_version"
_LAST_RUN_TIMESTAMP = "last_run_timestamp"


class VersionNotFound(Exception):
    pass


@gamla.curry
def _write_to_versions_file(identifier: Text, hash_to_load: Text, versions_file):
    new_versions_dict = gamla.pipe(
        json.load(versions_file),
        gamla.add_key_value(
            identifier,
            {
                _HASH_VERSION_KEY: hash_to_load,
                _LAST_RUN_TIMESTAMP: datetime.datetime.now().isoformat(),
            },
        ),
        dict.items,
        sorted,
        dict,
    )
    versions_file.seek(0)
    json.dump(new_versions_dict, versions_file, indent=2)
    versions_file.truncate()


@gamla.curry
def _write_hash_to_versions_file(
    versions_file_name: Text,
    identifier: Text,
    hash_to_load: Text,
):
    return gamla.pipe(
        versions_file_name,
        file_store.open_file("r+"),
        _write_to_versions_file(identifier, hash_to_load),
    )


def _time_since_last_updated(identifier: Text):
    return gamla.compose_left(
        gamla.get_in_or_none([identifier, _LAST_RUN_TIMESTAMP]),
        gamla.unless(
            gamla.equals(None),
            gamla.compose_left(
                datetime.datetime.fromisoformat,
                lambda last_updated: datetime.datetime.now() - last_updated,
            ),
        ),
    )


def _should_update(
    identifier: Text,
    update: bool,
    force_update: bool,
    ttl_hours: int,
) -> Callable[[Dict], bool]:
    return gamla.anyjuxt(
        gamla.just(force_update),
        gamla.complement(gamla.inside(identifier)),
        gamla.alljuxt(
            gamla.just(update),
            gamla.compose_left(
                _time_since_last_updated(identifier),
                gamla.anyjuxt(
                    gamla.equals(None),
                    gamla.greater_than(datetime.timedelta(hours=ttl_hours)),
                ),
            ),
        ),
    )


_total_hours_since_update = gamla.ternary(
    gamla.equals(None),
    gamla.just(0),
    gamla.compose_left(lambda time_span: time_span.total_seconds() / 3600, round),
)


def auto_updating_cache(
    factory: Callable,
    update: bool,
    versions_file_path: Text,
    environment: Text,
    bucket_name: Text,
    force_update: bool,
    frame_level: int,
    ttl_hours: int,
) -> Callable:

    # Deployment name is the concatenation of caller's module name and factory's function name.
    identifier = f"{inspect.stack()[frame_level+1].frame.f_globals['__name__']}.{factory.__name__}"

    return gamla.compose_left(
        gamla.just(versions_file_path),
        file_store.open_file("r"),
        json.load,
        gamla.side_effect(
            gamla.compose_left(
                _time_since_last_updated(identifier),
                _total_hours_since_update,
                lambda hours_since_last_update: f"Loading cache for [{identifier}]. Last updated {hours_since_last_update} hours ago.",
                logging.info,
            ),
        ),
        gamla.ternary(
            _should_update(identifier, update, force_update, ttl_hours),
            gamla.compose_left(
                gamla.ignore_input(factory),
                file_store.save_to_bucket_return_hash(environment, bucket_name),
                gamla.side_effect(
                    _write_hash_to_versions_file(versions_file_path, identifier),
                ),
                gamla.log_text(f"Finished updating cache for [{identifier}]."),
            ),
            gamla.get_in([identifier, _HASH_VERSION_KEY]),
        ),
    )


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
            redis_client,
            environment,
            name,
        )
    else:
        get_cache_item, set_cache_item = file_store.make_file_store(
            name,
            num_misses_to_trigger_sync,
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
