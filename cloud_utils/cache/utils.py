import datetime
import functools
import inspect
import json
import logging
import os
from typing import Callable, Dict, Optional

import async_lru
import gamla
import redis

from cloud_utils.cache import file_store, redis_utils

RESULT_HASH_KEY = "result_hash"
LAST_RUN_TIMESTAMP = "last_run_timestamp"


class VersionNotFound(Exception):
    pass


@gamla.curry
def _write_to_cache_file(
    cache_file_name: str,
    identifier: str,
    hash_dict: Dict,
):
    cache_file = file_store.open_file("r+")(cache_file_name)
    new_versions_dict = gamla.pipe(
        cache_file,
        json.load,
        gamla.add_key_value(identifier, hash_dict),
        dict.items,
        sorted,
        dict,
    )
    cache_file.seek(0)
    json.dump(new_versions_dict, cache_file, indent=2)
    cache_file.write("\n")
    cache_file.truncate()


def _time_since_last_updated(
    identifier: str,
) -> Callable[[Dict], Optional[datetime.timedelta]]:
    return gamla.compose_left(
        gamla.get_in_or_none([identifier, LAST_RUN_TIMESTAMP]),
        gamla.unless(
            gamla.equals(None),
            gamla.compose_left(
                datetime.datetime.fromisoformat,
                lambda last_updated: datetime.datetime.now() - last_updated,
            ),
        ),
    )


_total_hours_since_update = gamla.ternary(
    gamla.equals(None),
    gamla.just(0),
    gamla.compose_left(lambda time_span: time_span.total_seconds() / 3600, round),
)


@gamla.curry
def default_cache_file_path(cache_file_name, factory):
    return gamla.pipe(
        factory,
        gamla.function_to_directory,
        gamla.pair_right(gamla.just(cache_file_name)),
        gamla.star(os.path.join),
    )


def _create_cache_file(path: str) -> str:
    if not os.path.isfile(path):
        with open(path, "w") as f:
            f.write("{}\n")

    return path


def auto_updating_cache(
    factory: Callable,
    cache_file_path: str,
    save_local: bool,
    bucket_name: str,
    should_update: Callable[[Optional[datetime.timedelta]], bool],
    function_to_identifier: Callable,
    hash_to_dict: Callable[[str], Dict],
):
    cache_file = _create_cache_file(cache_file_path)

    async def inner(*args, **kwargs):
        identifier = function_to_identifier(*args, kwargs)
        return await gamla.pipe(
            cache_file,
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
                gamla.compose_left(_time_since_last_updated(identifier), should_update),
                gamla.compose_left(
                    gamla.just(gamla.compose_left(factory, gamla.to_awaitable)),
                    gamla.apply_async(*args),
                    file_store.save_to_bucket_return_hash(save_local, bucket_name),
                    gamla.side_effect(
                        gamla.compose_left(
                            hash_to_dict,
                            _write_to_cache_file(cache_file, identifier),
                        ),
                    ),
                    gamla.log_text(f"Finished updating cache for [{identifier}]."),
                ),
                gamla.get_in([identifier, RESULT_HASH_KEY]),
            ),
        )

    return inner


def persistent_cache(
    redis_client: redis.Redis,
    name: str,
    environment: str,
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
