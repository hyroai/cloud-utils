from typing import Text

import gamla

from cloud_utils.storage import download_blob_as_string

hash_to_filename = gamla.wrap_str("items/{}.json")


def load_file_from_bucket(file_name: Text, bucket_name: Text):
    return gamla.pipe(
        file_name,
        gamla.log_text("Loading {} from bucket..."),
        download_blob_as_string(bucket_name),
    )
