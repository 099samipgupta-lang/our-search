import base64
import hashlib
import os
import secrets

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


AAD = b"our-search-creator-ai-v1"


def _encryption_key():
    value = os.environ.get(
        "OUR_SEARCH_CREDENTIAL_ENCRYPTION_KEY", ""
    )

    try:
        key = bytes.fromhex(value)
    except ValueError:
        raise RuntimeError("Invalid credential encryption configuration")

    if len(key) != 32:
        raise RuntimeError("Invalid credential encryption configuration")

    return key


def encrypt_api_key(api_key):
    if not isinstance(api_key, str) or not api_key:
        raise ValueError("API key is required")

    nonce = secrets.token_bytes(12)

    encrypted = AESGCM(_encryption_key()).encrypt(
        nonce,
        api_key.encode("utf-8"),
        AAD,
    )

    return base64.urlsafe_b64encode(
        nonce + encrypted
    ).decode("ascii")


def decrypt_api_key(encrypted_value):
    raw = base64.urlsafe_b64decode(encrypted_value)

    nonce = raw[:12]
    ciphertext = raw[12:]

    return AESGCM(_encryption_key()).decrypt(
        nonce,
        ciphertext,
        AAD,
    ).decode("utf-8")


def create_test_token():
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(
        token.encode("utf-8")
    ).hexdigest()

    return token, token_hash


def hash_test_token(token):
    return hashlib.sha256(
        token.encode("utf-8")
    ).hexdigest()
