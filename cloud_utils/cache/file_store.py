import json
import logging
import pathlib
import pickle
import timeit
import zipfile
from typing import Any, Callable, Dict, Text, Tuple

import gamla
import xmltodict

from cloud_utils import storage
from cloud_utils.storage import utils

_LOCAL_CACHE_PATH: pathlib.Path = pathlib.Path.home().joinpath(".nlu_cache")


def open_file(mode: Text):
    return gamla.compose_left(pathlib.Path, lambda p: p.open(mode=mode))


@gamla.curry
def _save_to_blob(bucket_name: Text, item_name: Text, obj: Any):
    storage.upload_blob(bucket_name, utils.hash_to_filename(item_name), obj)


@gamla.curry
def _load_item(bucket_name: Text, hash_to_load: Text):
    return gamla.pipe(
        hash_to_load,
        utils.hash_to_filename,
        storage.download_blob_as_string(bucket_name),
        json.loads,
    )


def local_path_for_hash(object_hash: Text) -> pathlib.Path:
    local_path = gamla.pipe(
        object_hash,
        utils.hash_to_filename,
        _LOCAL_CACHE_PATH.joinpath,
    )
    local_path.parent.mkdir(parents=True, exist_ok=True)
    return local_path


@gamla.curry
def save_local(environment: Text, object_hash: Text, obj: Any) -> Any:
    if environment != "local":
        return
    local_path = local_path_for_hash(object_hash)
    if local_path.exists():
        return
    with local_path.open("w") as f:
        f.write(gamla.to_json(obj))
    logging.info(f"Saved {object_hash} to local cache.")


@gamla.curry
def load_by_hash(environment: Text, bucket_name: Text, object_hash: Text) -> Dict:
    try:
        return gamla.pipe(
            object_hash,
            local_path_for_hash,
            lambda x: x.open("r"),
            json.load,
            gamla.log_text(f"Loaded {object_hash} from local cache."),
        )
    except FileNotFoundError:
        return gamla.pipe(
            object_hash,
            gamla.log_text(f"Loading {object_hash} from bucket..."),
            gamla.translate_exception(
                _load_item(bucket_name),
                Exception,
                FileNotFoundError,
            ),
            gamla.side_effect(save_local(environment, object_hash)),
        )


def load_file_from_bucket(bucket_name: Text, file_name: Text):
    return gamla.pipe(
        file_name,
        gamla.log_text("Loading {} from bucket..."),
        storage.download_blob_as_string(bucket_name),
    )


async def file_hash_exists_in_bucket(bucket_name: str, file_name: str) -> bool:
    return await storage.blob_exists(bucket_name, utils.hash_to_filename(file_name))


def save_to_bucket_return_hash(environment: Text, bucket_name: Text):
    return gamla.compose_left(
        gamla.pair_with(gamla.compute_stable_json_hash),
        save_to_bucket(environment, bucket_name),
    )


def save_to_bucket(environment: Text, bucket_name: Text):
    return gamla.compose_left(
        gamla.side_effect(gamla.star(_save_to_blob(bucket_name))),
        gamla.side_effect(gamla.star(save_local(environment))),
        gamla.head,
        gamla.log_text("Saved hash {}"),
    )


_local_cache_filename = gamla.wrap_str("{}.pickle")


_make_path = gamla.compose(_LOCAL_CACHE_PATH.joinpath, _local_cache_filename)


def _load_cache_from_local(cache_name: Text) -> Dict[Tuple, Any]:
    with _make_path(cache_name).open("rb") as local_cache_file:
        return pickle.load(local_cache_file)


def _save_cache_locally(cache_name: Text, cache: Dict[Tuple, Any]):
    _LOCAL_CACHE_PATH.mkdir(parents=True, exist_ok=True)
    with _make_path(cache_name).open("wb") as local_cache_file:
        pickle.dump(cache, local_cache_file)
    logging.info(f"Saved {len(cache)} cache items locally for {cache_name}.")


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
        f"Initializing local file cache for {name} (num_misses_to_trigger_sync={num_misses_to_trigger_sync}).",
    )

    # Initialize cache.
    try:
        cache = _load_cache_from_local(name)
        logging.info(f"Loaded {len(cache):,} cache items from local file for {name}.")
    except (OSError, IOError, EOFError, pickle.UnpicklingError) as err:
        logging.info(
            f"Cache {name} does not exist or is invalid. Initializing an empty cache. Error: {err}.",
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
                f"More than {num_misses_to_trigger_sync:,} keys changed in cache {name}. Syncing with local file.",
            )

            sync_running = True
            sync_start = timeit.default_timer()

            try:
                _save_cache_locally(name, cache)
                sync_running = False
                logging.info(
                    f"Synced cache {name} to local file in {timeit.default_timer() - sync_start}.",
                )
                change_count -= num_misses_to_trigger_sync
            except (OSError, IOError, EOFError) as exception:
                logging.error(
                    f"Could not sync {name} with local file. Error: {exception}.",
                )

    return get_item, set_item
