import io
import os
import pathlib
from typing import Any, Text

import toolz
from azure.storage import blob


def _get_blob_service():
    return blob.BlockBlobService(
        connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )


def upload_blob(bucket_name: Text, blob_name: Text, obj: Any):
    stream = io.BytesIO(bytes(obj.to_json(), "utf-8"))
    _get_blob_service().create_blob_from_stream(
        bucket_name, blob_name, stream, timeout=1800
    )


@toolz.curry
def download_blob_as_string(bucket_name: Text, blob_name: Text):
    return (
        _get_blob_service()
        .get_blob_to_text(bucket_name, blob_name, encoding="utf-8")
        .content
    )


def download_blob_to_file(bucket_name: Text, blob_name: Text, path: pathlib.Path):
    return _get_blob_service().get_blob_to_path(
        bucket_name, blob_name, str(path.resolve())
    )
