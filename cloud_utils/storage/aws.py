import pathlib
from typing import Any, Text

import boto3
import gamla

s3 = boto3.resource("s3")


def upload_blob(bucket_name: Text, blob_name: Text, obj: Any):
    s3.Bucket(bucket_name).put_object(Key=blob_name, Body=obj.to_json())


@gamla.curry
def download_blob_as_string(bucket_name: Text, blob_name: Text) -> Text:
    return s3.Object(bucket_name, blob_name).get()["Body"].read().decode("utf-8")


def download_blob_to_file(bucket_name: Text, blob_name: Text, path: pathlib.Path):
    boto3.client("s3").download_file(bucket_name, blob_name, str(path.resolve()))
