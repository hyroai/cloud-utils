import json
from typing import Text

import gamla
import toolz

from cloud_utils.storage import download_blob_as_string


def hash_to_filename(hash_str: Text) -> Text:
    return f"items/{hash_str}.json"


@toolz.curry
def _load_item(bucket_name: Text, hash_to_load: Text):
    return toolz.pipe(
        hash_to_load, hash_to_filename, download_blob_as_string(bucket_name), json.loads
    )


def load_file_from_bucket(file_name: Text, bucket_name: Text):
    return toolz.pipe(
        file_name,
        gamla.log_text("Loading {} from bucket..."),
        download_blob_as_string(bucket_name),
    )
