import io
import os
import pathlib
from typing import Any, Text

import gamla
from azure.storage import blob


def _get_blob_service():
    return blob.BlockBlobService(
        connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        socket_timeout=(2000, 2000),
    )


def upload_blob(bucket_name: Text, blob_name: Text, obj: Any):
    stream = io.BytesIO(bytes(gamla.to_json(obj), "utf-8"))
    _get_blob_service().create_blob_from_stream(
        bucket_name, blob_name, stream, timeout=1800,
    )


@gamla.curry
def download_blob_as_string(bucket_name: Text, blob_name: Text) -> Text:
    return (
        _get_blob_service()
        .get_blob_to_text(bucket_name, blob_name, encoding="utf-8")
        .content
    )


@gamla.curry
def download_blob_as_stream(bucket_name: Text, blob_name: Text) -> io.BytesIO:
    stream = io.BytesIO()
    _get_blob_service().get_blob_to_stream(bucket_name, blob_name, stream)
    stream.seek(0)
    return stream


def download_blob_to_file(bucket_name: Text, blob_name: Text, path: pathlib.Path):
    return _get_blob_service().get_blob_to_path(
        bucket_name, blob_name, str(path.resolve()),
    )
