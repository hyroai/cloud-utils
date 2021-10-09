import base64
import datetime
import gzip
import hashlib
import hmac
import io
import os
import pathlib
from typing import Any, Text

import gamla
from azure.storage import blob

_API_VERSION = "2019-02-02"


def _get_connection_config():
    return gamla.pipe(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        gamla.split_text(";"),
        gamla.map(lambda text: text.split("=", 1)),
        dict,
    )


def _sign_params(key: str, params: dict):
    return base64.b64encode(
        hmac.new(
            base64.b64decode(key),
            msg="\n".join(params.values()).encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest(),
    ).decode()


def _head_headers_and_url(bucket_name: str, blob_name: str):
    now = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    config = _get_connection_config()
    params = {
        "verb": "HEAD",
        "Content-Encoding": "",
        "Content-Language": "",
        "Content-Length": "",
        "Content-MD5": "",
        "Content-Type": "",
        "Date": "",
        "If-Modified-Since": "",
        "If-Match": "",
        "If-None-Match": "",
        "If-Unmodified-Since": "",
        "Range": "",
        "CanonicalizedHeaders": f"x-ms-date:{now}\nx-ms-version:{_API_VERSION}",
        "CanonicalizedResource": f"/{config['AccountName']}/{bucket_name}/{blob_name}",
    }

    return (
        {
            "x-ms-version": _API_VERSION,
            "x-ms-date": now,
            "Authorization": f"SharedKey {config['AccountName']}:{_sign_params(config['AccountKey'], params)}",
        },
        f"https://{config['AccountName']}.blob.{config['EndpointSuffix']}/{bucket_name}/{blob_name}",
    )


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
) -> Text:
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


@gamla.curry
async def blob_exists(bucket_name: str, blob_name: str) -> bool:
    headers, url = _head_headers_and_url(bucket_name, blob_name)
    return (
        True
        if (await gamla.head_async_with_headers(headers, 60, url)).status_code == 200
        else False
    )
