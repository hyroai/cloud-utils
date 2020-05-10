import asyncio
import datetime
import inspect
import json
import logging
import os
import pathlib
from typing import Text, Any, Callable

import gamla
import toolz
from nlu.cache import file_store
from nlu import config
from toolz import curried

from cloud_utils import storage

_BUCKET_NAME = "nlu-cache"


def _open_file(file_name: Text, mode="r"):
    return toolz.pipe(file_name, pathlib.Path, lambda p: p.open(mode=mode))


_HASH_VERSION = "hash_version"
_LAST_RUN_TIMESTAMP = "last_run_timestamp"


def _hash_to_filename(hash_str: Text) -> Text:
    return f"items/{hash_str}.json"


def _save_to_blob(item_name: Text, obj: Any):
    storage.upload_blob(_BUCKET_NAME, _hash_to_filename(item_name), obj)


def _load_item(hash_to_load: Text):
    return toolz.pipe(
        hash_to_load,
        _hash_to_filename,
        storage.download_blob_as_string(_BUCKET_NAME),
        json.loads,
    )


def _get_local_path_for_hash(object_hash: Text) -> pathlib.Path:
    local_path = file_store.LOCAL_CACHE_PATH.joinpath(_hash_to_filename(object_hash))
    local_path.parent.mkdir(parents=True, exist_ok=True)
    return local_path


@toolz.curry
def _save_local(object_hash: Text, obj: Any) -> Any:
    if config.ENVIRONMENT != "local":
        return
    local_path = _get_local_path_for_hash(object_hash)
    if local_path.exists():
        return
    with local_path.open("w") as f:
        if isinstance(obj, dict) or isinstance(obj, list):
            json.dump(obj, f)
        else:
            f.write(obj.to_json())
    logging.info(f"Saved {object_hash} to local cache.")


def load_by_hash(object_hash: Text) -> dict:
    try:
        return toolz.pipe(
            object_hash,
            _get_local_path_for_hash,
            lambda x: x.open("r"),
            json.load,
            gamla.log_text(f"Loaded {object_hash} from local cache."),
        )
    except FileNotFoundError:
        return toolz.pipe(
            object_hash,
            gamla.log_text(f"Loading {object_hash} from bucket..."),
            _load_item,
            curried.do(_save_local(object_hash)),
        )


def load_file_from_bucket(file_name: Text):
    return toolz.pipe(
        file_name,
        gamla.log_text("Loading {} from bucket..."),
        storage.download_blob_as_string(_BUCKET_NAME),
    )


save_to_bucket_return_hash = toolz.compose_left(
    gamla.pair_with(gamla.compute_stable_json_hash),
    curried.do(gamla.star(_save_to_blob)),
    curried.do(gamla.star(_save_local)),
    toolz.first,
    gamla.log_text("Saved hash {}"),
)


def _write_to_versions_file(
    deployment_name: Text, hash_to_load: Text
):
    versions_file = toolz.pipe(os.environ["VERSIONS_FILE_PATH"], _open_file("r+"))

    versions = toolz.assoc(
        json.load(versions_file),
        deployment_name,
        {
            _HASH_VERSION: hash_to_load,
            _LAST_RUN_TIMESTAMP: datetime.datetime.now().isoformat(),
        },
    )
    json.dump(versions, versions_file, indent=2)


def auto_updating_cache(factory: Callable) -> Callable:
    versions = toolz.pipe(os.environ["VERSIONS_FILE_PATH"], _open_file, json.load)

    # Deployment name is the concatenation of caller's module name and factory's function name.
    deployment_name = (
        f"{inspect.stack()[1].frame.f_locals['__name__']}.{factory.__name__}"
    )

    if deployment_name in versions and (
        not os.environ["AUTO_UPDATING_CACHE"]
        or datetime.datetime.now()
        - datetime.datetime.fromisoformat(
            versions[deployment_name][_LAST_RUN_TIMESTAMP]
        )
        <= datetime.timedelta(days=1)
    ):
        return gamla.just(versions[deployment_name][_HASH_VERSION])

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
            hash_to_load = versions[deployment_name][_HASH_VERSION]
            logging.error(
                f"Unable to update version '{deployment_name}'. Using old hash {hash_to_load} created on {versions[deployment_name][_LAST_RUN_TIMESTAMP]}."
            )
        else:
            raise e

    _write_to_versions_file(deployment_name, hash_to_load)

    return gamla.just(hash_to_load)
