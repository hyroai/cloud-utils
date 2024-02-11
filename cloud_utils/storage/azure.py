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
        os.environ["AZURE_STORAGE_CONNECTION_STRING"],
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


def head_headers_and_url(bucket_name: str, blob_name: str):
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


def _upload_blob(bucket_name: str, blob_name: str, data: str | bytes, zipped: bool):
    blob.BlobClient.from_connection_string(
        conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
        container_name=bucket_name,
        blob_name=blob_name,
        connection_timeout=120,
        max_single_put_size=4 * 1024 * 1024,
    ).upload_blob(
        data,
        overwrite=True,
        max_concurrency=10,
        timeout=600,
        content_settings=blob.ContentSettings(content_encoding="gzip")
        if zipped
        else None,
    )


def _download_blob(bucket_name: str, blob_name: str) -> bytes:
    return (
        blob.BlobClient.from_connection_string(
            conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
            container_name=bucket_name,
            blob_name=blob_name,
            max_single_get_size=64 * 1024 * 1024,
        )
        .download_blob(max_concurrency=10)
        .readall()
    )


def upload_blob(bucket_name: str, blob_name: str, obj: Any):
    return gamla.pipe(
        obj,
        gamla.to_json,
        lambda text: bytes(text, "utf-8"),
        gzip.compress,
        io.BytesIO,
        lambda stream: _upload_blob(bucket_name, blob_name, stream, True),
    )


def upload_text(bucket_name: str, blob_name: str, text: Text):
    _upload_blob(bucket_name, blob_name, text, False)


@gamla.curry
def download_blob_as_string_with_encoding(
    encoding: str,
    bucket_name: str,
    blob_name: str,
) -> Text:
    return _download_blob(bucket_name, blob_name).decode(encoding)


download_blob_as_string = download_blob_as_string_with_encoding("utf-8")


@gamla.curry
def download_blob_as_stream(bucket_name: str, blob_name: str) -> io.BytesIO:
    return io.BytesIO(_download_blob(bucket_name, blob_name))


def download_blob_to_file(bucket_name: str, blob_name: str, path: pathlib.Path):
    with open(path.resolve(), "wb") as target_file:
        target_file.write(_download_blob(bucket_name, blob_name))


@gamla.curry
async def blob_exists(bucket_name: str, blob_name: str) -> bool:
    headers, url = head_headers_and_url(bucket_name, blob_name)
    return (
        True
        if (await gamla.head_async_with_headers(headers, 60, url)).status_code == 200
        else False
    )
