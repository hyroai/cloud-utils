import pathlib
from typing import Any

import gamla
from google.cloud import storage


def _blob(bucket_name: str, blob_name: str):
    return storage.Client().get_bucket(bucket_name).blob(blob_name)


def upload_blob(bucket_name: str, blob_name: str, obj: Any):
    _blob(bucket_name, blob_name).upload_from_string(gamla.to_json(obj))


@gamla.curry
def download_blob_as_string(bucket_name: str, blob_name: str):
    return _blob(bucket_name, blob_name).download_as_string().decode("utf-8")


def download_blob_to_file(bucket_name: str, blob_name: str, path: pathlib.Path):
    _blob(bucket_name, blob_name).download_to_file(path.open("wb"))
