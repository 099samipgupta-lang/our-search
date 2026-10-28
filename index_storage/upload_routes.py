"""HTTP helpers for secure resumable model uploads."""
import json
import os
from urllib.parse import unquote

from index_storage.resumable_uploads import ResumableUploadManager, UploadError
from index_storage.upload_auth import (
    UploadAuthorizationError,
    verify_upload_token,
)


def make_upload_manager(storage_root):
    staging_root = os.environ.get(
        "UPLOAD_STAGING_DIR",
        os.path.join(storage_root, ".upload_staging"),
    )
    return ResumableUploadManager(staging_root)


def token_from_handler(handler):
    auth = handler.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    return auth[7:].strip()


def read_upload_json(handler, max_bytes=65536):
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except (TypeError, ValueError):
        raise UploadError("invalid_content_length")
    if length <= 0 or length > max_bytes:
        raise UploadError("invalid_request_size")
    raw = handler.rfile.read(length)
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise UploadError("invalid_json")
    if not isinstance(data, dict):
        raise UploadError("json_object_required")
    return data


def validate_capability(handler, manager, upload_id):
    token = token_from_handler(handler)
    if not token:
        raise UploadAuthorizationError("upload_token_required")
    owner = manager.owner(upload_id)
    return verify_upload_token(token, owner, upload_id)


def expected_chunk_length(payload, index, chunk_size):
    size = int(payload["size"])
    count = (size + chunk_size - 1) // chunk_size
    if index < 0 or index >= count:
        raise UploadError("chunk_index_out_of_range")
    return min(chunk_size, size - index * chunk_size)
