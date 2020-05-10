import json
from typing import Text

import gamla
import toolz
from nlu import config
from toolz import curried

from cloud_utils.storage import download_blob_as_string, persistent


def hash_to_filename(hash_str: Text) -> Text:
    return f"items/{hash_str}.json"


@toolz.curry
def _load_item(bucket_name: Text, hash_to_load: Text):
    return toolz.pipe(
        hash_to_load, hash_to_filename, download_blob_as_string(bucket_name), json.loads
    )


@toolz.curry
def load_by_hash(object_hash: Text, bucket_name: Text = config.BUCKET_NAME) -> dict:
    try:
        return toolz.pipe(
            object_hash,
            persistent.get_local_path_for_hash,
            lambda x: x.open("r"),
            json.load,
            gamla.log_text(f"Loaded {object_hash} from local cache."),
        )
    except FileNotFoundError:
        return toolz.pipe(
            object_hash,
            gamla.log_text(f"Loading {object_hash} from bucket..."),
            _load_item(bucket_name),
            curried.do(persistent.save_local(object_hash)),
        )


def load_file_from_bucket(file_name: Text, bucket_name: Text):
    return toolz.pipe(
        file_name,
        gamla.log_text("Loading {} from bucket..."),
        download_blob_as_string(bucket_name),
    )
