import json
import logging
import pathlib
from typing import Any, Text

import toolz
from cloud_utils import config

from cloud_utils.cache import file_store
from cloud_utils.storage import utils


def get_local_path_for_hash(object_hash: Text) -> pathlib.Path:
    local_path = file_store.LOCAL_CACHE_PATH.joinpath(
        utils.hash_to_filename(object_hash)
    )
    local_path.parent.mkdir(parents=True, exist_ok=True)
    return local_path


@toolz.curry
def save_local(object_hash: Text, obj: Any) -> Any:
    if config.ENVIRONMENT != "local":
        return
    local_path = get_local_path_for_hash(object_hash)
    if local_path.exists():
        return
    with local_path.open("w") as f:
        if isinstance(obj, dict) or isinstance(obj, list):
            json.dump(obj, f)
        else:
            f.write(obj.to_json())
    logging.info(f"Saved {object_hash} to local cache.")


def open_file(file_name: Text, mode="r"):
    return toolz.pipe(file_name, pathlib.Path, lambda p: p.open(mode=mode))
