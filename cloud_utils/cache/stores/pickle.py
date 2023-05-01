import logging
import os
import pickle
from typing import Any, Callable, Dict, Tuple

import gamla

from cloud_utils.cache import utils


def _local_cache_path(cache_name: str, path: str):
    return os.path.join(path, f"{cache_name}.pickle")


@gamla.timeit
def _load_cache_from_local(cache_name: str, path: str) -> Dict[Tuple, Any]:
    with open(_local_cache_path(cache_name, path), "rb") as local_cache_file:
        return pickle.load(local_cache_file)


def _save_cache_locally(cache_name: str, path: str, cache: Dict[Tuple, Any]):
    if not os.path.exists(path):
        os.makedirs(path)
    local_path = _local_cache_path(cache_name, path)
    with open(local_path, "wb") as local_cache_file:
        pickle.dump(cache, local_cache_file)
    logging.info(f"Saved {len(cache)} cache items locally for {cache_name}.")


def make_store(name: str, cache_path: str) -> Tuple[Callable, Callable]:
    change_count = 0
    utils.log_initialized_cache("pickle", name)
    # Initialize cache.
    try:
        cache = _load_cache_from_local(name, cache_path)
        logging.info(f"Loaded {len(cache):,} cache items from local file for {name}.")
    except (OSError, IOError, EOFError, pickle.UnpicklingError) as err:
        logging.info(
            f"Cache {name} does not exist or is invalid. Initializing an empty cache.",
        )
        logging.error(err)
        cache = {}

    def get_item(key: Tuple):
        return cache[key]

    def set_item(key: Tuple, value):
        nonlocal change_count

        change_count += 1
        cache[key] = value

        if change_count >= 10:
            logging.info(
                f"More than 10 keys changed in cache {name}. Syncing with local file.",
            )

            try:
                _save_cache_locally(name, cache_path, cache)
                logging.info(
                    f"Synced cache {name} to local file",
                )
                change_count -= 10
            except (OSError, IOError, EOFError) as exception:
                logging.error(
                    f"Could not sync {name} with local file. Error: {exception}.",
                )

    return get_item, set_item


make_async_store = gamla.compose_left(
    make_store,
    gamla.map(gamla.wrap_awaitable),
    tuple,
)
