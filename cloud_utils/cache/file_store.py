import json
import logging
import pathlib
import pickle
import timeit
import zipfile
from typing import Any, Callable, Dict, Text, Tuple

import gamla
import toolz
import xmltodict
from toolz import curried

from cloud_utils import storage

_LOCAL_CACHE_PATH: pathlib.Path = pathlib.Path.home().joinpath(".nlu_cache")


def open_file(mode: Text):
    return gamla.compose_left(pathlib.Path, lambda p: p.open(mode=mode))


def _hash_to_filename(hash_str: Text) -> Text:
    return f"items/{hash_str}.json"


@gamla.curry
def _save_to_blob(bucket_name: Text, item_name: Text, obj: Any):
    storage.upload_blob(bucket_name, _hash_to_filename(item_name), obj)


@gamla.curry
def _load_item(bucket_name: Text, hash_to_load: Text):
    return toolz.pipe(
        hash_to_load,
        _hash_to_filename,
        storage.download_blob_as_string(bucket_name),
        json.loads,
    )


def get_local_path_for_hash(object_hash: Text) -> pathlib.Path:
    local_path = _LOCAL_CACHE_PATH.joinpath(_hash_to_filename(object_hash))
    local_path.parent.mkdir(parents=True, exist_ok=True)
    return local_path


@gamla.curry
def save_local(environment: Text, object_hash: Text, obj: Any) -> Any:
    if environment != "local":
        return
    local_path = get_local_path_for_hash(object_hash)
    if local_path.exists():
        return
    with local_path.open("w") as f:
        f.write(gamla.to_json(obj))
    logging.info(f"Saved {object_hash} to local cache.")


@gamla.curry
def load_by_hash(environment: Text, bucket_name: Text, object_hash: Text) -> dict:
    try:
        return toolz.pipe(
            object_hash,
            get_local_path_for_hash,
            lambda x: x.open("r"),
            json.load,
            gamla.log_text(f"Loaded {object_hash} from local cache."),
        )
    except FileNotFoundError:
        return toolz.pipe(
            object_hash,
            gamla.log_text(f"Loading {object_hash} from bucket..."),
            _load_item(bucket_name),
            curried.do(save_local(environment, object_hash)),
        )


def load_file_from_bucket(bucket_name: Text, file_name: Text):
    return toolz.pipe(
        file_name,
        gamla.log_text("Loading {} from bucket..."),
        storage.download_blob_as_string(bucket_name),
    )


def save_to_bucket_return_hash(environment: Text, bucket_name: Text):
    return gamla.compose_left(
        gamla.pair_with(gamla.compute_stable_json_hash),
        curried.do(gamla.star(_save_to_blob(bucket_name))),
        curried.do(gamla.star(save_local(environment))),
        toolz.first,
        gamla.log_text("Saved hash {}"),
    )


def _get_local_cache_filename(cache_name: Text) -> Text:
    return f"{cache_name}.pickle"


_make_path = toolz.compose(_LOCAL_CACHE_PATH.joinpath, _get_local_cache_filename)


def _load_cache_from_local(cache_name: Text) -> Dict[Tuple, Any]:
    with _make_path(cache_name).open("rb") as local_cache_file:
        return pickle.load(local_cache_file)


def _save_cache_locally(cache_name: Text, cache: Dict[Tuple, Any]):
    _LOCAL_CACHE_PATH.mkdir(parents=True, exist_ok=True)
    with _make_path(cache_name).open("wb") as local_cache_file:
        pickle.dump(cache, local_cache_file)
    logging.info(f"saved {len(cache)} cache items locally for {cache_name}")


def load_xml_to_dict(xml_file: Text) -> Dict:
    local_path = _LOCAL_CACHE_PATH.joinpath(xml_file)
    if not local_path.exists():
        storage.download_blob_to_file("hyro-bot-data", xml_file, local_path)
    zip_xml = zipfile.ZipFile(local_path)
    xml_string = zip_xml.open(zip_xml.namelist()[0]).read()
    return xmltodict.parse(xml_string, attr_prefix="")


def make_file_store(
    name: Text,
    num_misses_to_trigger_sync: int,
) -> Tuple[Callable, Callable]:
    change_count = 0
    sync_running = False
    sync_start = 0.0

    logging.info(
        f"initializing local file cache for {name} (num_misses_to_trigger_sync={num_misses_to_trigger_sync})",
    )

    # Initialize cache.
    try:
        cache = _load_cache_from_local(name)
        logging.info(f"loaded {len(cache)} cache items from local file for {name}")
    except (OSError, IOError, EOFError, pickle.UnpicklingError) as err:
        logging.info(
            f"cache {name} does not exist or is invalid. initializing an empty cache. error: {err}",
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
                f"more than {num_misses_to_trigger_sync} keys changed in cache {name}. syncing with local file",
            )

            sync_running = True
            sync_start = timeit.default_timer()

            try:
                _save_cache_locally(name, cache)
                sync_running = False
                logging.info(
                    f"synced cache {name} to local file in {timeit.default_timer() - sync_start}",
                )
                change_count -= num_misses_to_trigger_sync
            except (OSError, IOError, EOFError) as exception:
                logging.error(
                    f"could not sync {name} with local file. error: {exception}",
                )

    return get_item, set_item
