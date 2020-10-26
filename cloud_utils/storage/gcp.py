import pathlib
from typing import Any, Text

import gamla
from google.cloud import storage


def _get_blob(bucket_name: Text, blob_name: Text):
    return storage.Client().get_bucket(bucket_name).blob(blob_name)


def upload_blob(bucket_name: Text, blob_name: Text, obj: Any):
    _get_blob(bucket_name, blob_name).upload_from_string(gamla.to_json(obj))


@gamla.curry
def download_blob_as_string(bucket_name: Text, blob_name: Text):
    return _get_blob(bucket_name, blob_name).download_as_string().decode("utf-8")


def download_blob_to_file(bucket_name: Text, blob_name: Text, path: pathlib.Path):
    _get_blob(bucket_name, blob_name).download_to_file(path.open("wb"))
