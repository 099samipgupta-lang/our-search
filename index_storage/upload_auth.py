"""Short-lived HMAC upload capabilities for direct model uploads."""

import base64
import hashlib
import hmac
import json
import os
import re
import time


class UploadAuthorizationError(ValueError):
    pass


def _secret():
    value = os.environ.get("UPLOAD_SIGNING_SECRET", "")
    if len(value.encode("utf-8")) < 32:
        raise UploadAuthorizationError(
            "UPLOAD_SIGNING_SECRET must be configured with at least 32 bytes"
        )
    return value.encode("utf-8")


def issue_upload_token(user_id, storage_key, size, upload_id, ttl=600):
    if not isinstance(user_id, str) or not user_id:
        raise UploadAuthorizationError("Invalid user")
    if not isinstance(storage_key, str):
        raise UploadAuthorizationError("Invalid storage key")
    if not storage_key.startswith(f"ai-models/{user_id}/"):
        raise UploadAuthorizationError("Storage key does not belong to user")
    if ".." in storage_key or not re.fullmatch(
        r"[A-Za-z0-9._/-]+", storage_key
    ):
        raise UploadAuthorizationError("Invalid storage key")
    if not isinstance(size, int) or size <= 0:
        raise UploadAuthorizationError("Invalid upload size")
    if not isinstance(upload_id, str) or not re.fullmatch(
        r"[a-f0-9]{32}", upload_id
    ):
        raise UploadAuthorizationError("Invalid upload ID")
    if not 1 <= ttl <= 600:
        raise UploadAuthorizationError("Invalid token lifetime")

    payload = {
        "uid": user_id,
        "key": storage_key,
        "size": size,
        "upload_id": upload_id,
        "exp": int(time.time()) + ttl,
    }
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    encoded = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    signature = hmac.new(_secret(), encoded.encode("ascii"), hashlib.sha256)
    return encoded + "." + signature.hexdigest()


def verify_upload_token(token, user_id, upload_id):
    if not isinstance(token, str) or len(token) > 4096:
        raise UploadAuthorizationError("Invalid upload token")

    try:
        encoded, supplied_signature = token.split(".", 1)
    except ValueError as exc:
        raise UploadAuthorizationError("Malformed upload token") from exc

    expected_signature = hmac.new(
        _secret(), encoded.encode("ascii"), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(supplied_signature, expected_signature):
        raise UploadAuthorizationError("Invalid upload signature")

    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode(padded.encode("ascii"))
        )
    except (ValueError, UnicodeError) as exc:
        raise UploadAuthorizationError("Invalid upload payload") from exc

    if payload.get("exp", 0) < int(time.time()):
        raise UploadAuthorizationError("Upload token expired")
    if payload.get("uid") != user_id:
        raise UploadAuthorizationError("User mismatch")
    if payload.get("upload_id") != upload_id:
        raise UploadAuthorizationError("Upload session mismatch")

    key = payload.get("key", "")
    if not isinstance(key, str) or not key.startswith(f"ai-models/{user_id}/"):
        raise UploadAuthorizationError("Storage key ownership mismatch")

    return payload
