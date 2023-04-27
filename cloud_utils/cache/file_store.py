import json
import logging
import pathlib
from typing import Any, Dict

import gamla

from cloud_utils import storage
from cloud_utils.storage import utils

_LOCAL_CACHE_PATH: pathlib.Path = pathlib.Path.home().joinpath(".nlu_cache")


def open_file(mode: str):
    return gamla.compose_left(pathlib.Path, lambda p: p.open(mode=mode))


@gamla.curry
def _save_to_blob(bucket_name: str, item_name: str, obj: Any):
    storage.upload_blob(bucket_name, utils.hash_to_filename(item_name), obj)


@gamla.curry
def _load_item(bucket_name: str, hash_to_load: str):
    return gamla.pipe(
        hash_to_load,
        utils.hash_to_filename,
        storage.download_blob_as_string(bucket_name),
        json.loads,
    )


def local_path_for_hash(object_hash: str) -> pathlib.Path:
    local_path = gamla.pipe(
        object_hash,
        utils.hash_to_filename,
        _LOCAL_CACHE_PATH.joinpath,
    )
    local_path.parent.mkdir(parents=True, exist_ok=True)
    return local_path


@gamla.curry
def _save_local(object_hash: str, obj: Any) -> Any:
    local_path = local_path_for_hash(object_hash)
    if local_path.exists():
        return
    with local_path.open("w") as f:
        f.write(gamla.to_json(obj))
    logging.info(f"Saved {object_hash} to local cache.")


@gamla.curry
@gamla.timeit
def load_by_hash(should_save_local: bool, bucket_name: str, object_hash: str) -> Dict:
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
            _load_item(bucket_name),
            gamla.side_effect(_save_local(object_hash))
            if should_save_local
            else gamla.identity,
        )


def load_file_from_bucket(bucket_name: str, file_name: str):
    return gamla.pipe(
        file_name,
        gamla.log_text("Loading {} from bucket..."),
        storage.download_blob_as_string(bucket_name),
    )


async def file_hash_exists_in_bucket(bucket_name: str, file_name: str) -> bool:
    return await storage.blob_exists(bucket_name, utils.hash_to_filename(file_name))


def save_to_bucket_return_hash(save_local: bool, bucket_name: str):
    return gamla.compose_left(
        gamla.pair_with(gamla.compute_stable_json_hash),
        save_to_bucket(save_local, bucket_name),
    )


def save_to_bucket(save_local: bool, bucket_name: str):
    return gamla.compose_left(
        gamla.side_effect(gamla.star(_save_to_blob(bucket_name))),
        gamla.side_effect(gamla.star(_save_local)) if save_local else gamla.identity,
        gamla.head,
        gamla.log_text("Saved hash {}"),
    )
