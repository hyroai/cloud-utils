import gzip
import io
import os
import pathlib
from typing import Any

import gamla
from azure.storage import blob


def _blob_service():
    return blob.BlockBlobService(
        connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        socket_timeout=(2000, 2000),
    )


def _to_bytes(text: str):
    return bytes(text, "utf-8")


def upload_blob(bucket_name: str, blob_name: str, obj: Any):
    return gamla.pipe(
        obj,
        gamla.to_json,
        _to_bytes,
        gzip.compress,
        io.BytesIO,
        lambda stream: _blob_service().create_blob_from_stream(
            bucket_name,
            blob_name,
            stream,
            timeout=1800,
            content_settings=blob.ContentSettings(content_encoding="gzip"),
        ),
    )


@gamla.curry
def download_blob_as_string_with_encoding(
    encoding: str,
    bucket_name: str,
    blob_name: str,
) -> str:
    return (
        _blob_service()
        .get_blob_to_text(bucket_name, blob_name, encoding=encoding)
        .content
    )


download_blob_as_string = download_blob_as_string_with_encoding("utf-8")


@gamla.curry
def download_blob_as_stream(bucket_name: str, blob_name: str) -> io.BytesIO:
    stream = io.BytesIO()
    _blob_service().get_blob_to_stream(bucket_name, blob_name, stream)
    stream.seek(0)
    return stream


def download_blob_to_file(bucket_name: str, blob_name: str, path: pathlib.Path):
    return _blob_service().get_blob_to_path(
        bucket_name,
        blob_name,
        str(path.resolve()),
    )
