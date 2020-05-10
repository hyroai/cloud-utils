import logging
import pathlib
import pickle
import timeit
from typing import Any, Callable, Dict, Text, Tuple

import toolz

LOCAL_CACHE_PATH: pathlib.Path = pathlib.Path.home().joinpath(".nlu_cache")


def _get_local_cache_filename(cache_name: Text) -> Text:
    return f"{cache_name}.pickle"


_make_path = toolz.compose(LOCAL_CACHE_PATH.joinpath, _get_local_cache_filename)


def _load_cache_from_local(cache_name: Text) -> Dict[Tuple, Any]:
    with _make_path(cache_name).open("rb") as local_cache_file:
        return pickle.load(local_cache_file)


def _save_cache_locally(cache_name: Text, cache: Dict[Tuple, Any]):
    LOCAL_CACHE_PATH.mkdir(parents=True, exist_ok=True)
    with _make_path(cache_name).open("wb") as local_cache_file:
        pickle.dump(cache, local_cache_file)
    logging.info(f"saved {len(cache)} cache items locally for {cache_name}")


def make_file_store(
    name: Text, num_misses_to_trigger_sync: int
) -> Tuple[Callable, Callable]:
    change_count = 0
    sync_running = False
    sync_start = 0.0

    logging.info(
        f"initializing local file cache for {name} (num_misses_to_trigger_sync={num_misses_to_trigger_sync})"
    )

    # Initialize cache.
    try:
        cache = _load_cache_from_local(name)
        logging.info(f"loaded {len(cache)} cache items from local file for {name}")
    except (OSError, IOError, EOFError, pickle.UnpicklingError) as err:
        logging.info(
            f"cache {name} does not exist or is invalid. initializing an empty cache. error: {err}"
        )
        cache = {}

    def get_item(key: Tuple):
        return cache[key]

    def set_item(key: Tuple, value):
        nonlocal change_count, sync_running, sync_start

        change_count += 1
        cache[key] = value

        if change_count >= num_misses_to_trigger_sync and not sync_running:
            logging.info(
                f"more than {num_misses_to_trigger_sync} keys changed in cache {name}. syncing with local file"
            )

            sync_running = True
            sync_start = timeit.default_timer()

            try:
                _save_cache_locally(name, cache)
                sync_running = False
                logging.info(
                    f"synced cache {name} to local file in {timeit.default_timer() - sync_start}"
                )
                change_count -= num_misses_to_trigger_sync
            except (OSError, IOError, EOFError) as exception:
                logging.error(
                    f"could not sync {name} with local file. error: {exception}"
                )

    return get_item, set_item
