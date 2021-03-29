import logging
import os

try:
    if os.getenv("STORAGE_PROVIDER", "azure") == "azure":
        from cloud_utils.storage import azure as _storage_service

        download_blob_as_string_with_encoding = (
            _storage_service.download_blob_as_string_with_encoding
        )
    elif os.getenv("STORAGE_PROVIDER") == "gcp":
        from cloud_utils.storage import gcp as _storage_service  # type: ignore
    else:
        from cloud_utils.storage import aws as _storage_service  # type: ignore

    download_blob_as_string = _storage_service.download_blob_as_string
    download_blob_to_file = _storage_service.download_blob_to_file
    upload_blob = _storage_service.upload_blob
except Exception as e:
    logging.error(f"Could not load storage utils: {e}.")
