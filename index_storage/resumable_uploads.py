"""Disk-backed, resumable upload staging for large model files."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import threading
import uuid
from pathlib import Path


DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024


class UploadError(ValueError):
    """Raised when an upload request is invalid."""


class ResumableUploadManager:
    def __init__(
        self,
        root,
        chunk_size=DEFAULT_CHUNK_SIZE,
        max_upload_size=20 * 1024 * 1024 * 1024,
    ):
        if chunk_size <= 0 or max_upload_size <= 0:
            raise ValueError("Chunk size and upload limit must be positive")

        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.chunk_size = int(chunk_size)
        self.max_upload_size = int(max_upload_size)
        self._lock = threading.RLock()

    @staticmethod
    def _valid_id(upload_id):
        if not isinstance(upload_id, str) or not re.fullmatch(
            r"[a-f0-9]{32}", upload_id
        ):
            raise UploadError("Invalid upload ID")
        return upload_id

    def _directory(self, upload_id):
        upload_id = self._valid_id(upload_id)
        path = (self.root / upload_id).resolve()
        if path.parent != self.root:
            raise UploadError("Invalid upload path")
        return path

    def start(self, filename, size, user_id, storage_key):
        if not isinstance(filename, str) or not filename.strip():
            raise UploadError("Filename is required")
        if not isinstance(user_id, str) or not user_id:
            raise UploadError("Authenticated user is required")
        if not isinstance(storage_key, str) or not storage_key.startswith(
            f"ai-models/{user_id}/"
        ):
            raise UploadError("Storage key does not belong to this user")
        if ".." in storage_key or not re.fullmatch(
            r"[A-Za-z0-9._/-]+", storage_key
        ):
            raise UploadError("Invalid storage key")
        if not isinstance(size, int) or size <= 0:
            raise UploadError("Upload size must be positive")
        if size > self.max_upload_size:
            raise UploadError("Upload exceeds the configured size limit")

        upload_id = uuid.uuid4().hex
        directory = self._directory(upload_id)
        directory.mkdir(mode=0o700)

        metadata = {
            "upload_id": upload_id,
            "filename": filename,
            "size": size,
            "user_id": user_id,
            "storage_key": storage_key,
            "chunk_size": self.chunk_size,
            "chunks_received": 0,
            "status": "uploading",
        }

        try:
            self._write_metadata(directory, metadata)
        except Exception:
            shutil.rmtree(directory, ignore_errors=True)
            raise

        return {
            "upload_id": upload_id,
            "chunk_size": self.chunk_size,
            "chunks_received": 0,
        }

    def _read_metadata(self, directory):
        try:
            metadata = json.loads(
                (directory / "metadata.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise UploadError("Upload session not found or invalid") from exc

        if metadata.get("status") != "uploading":
            raise UploadError("Upload session is not active")
        return metadata

    @staticmethod
    def _write_metadata(directory, metadata):
        temporary = directory / "metadata.json.tmp"
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(metadata, file)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, directory / "metadata.json")

    def owner(self, upload_id):
        """Return the recorded owner of an active upload session."""
        directory = self._directory(upload_id)
        metadata = self._read_metadata(directory)
        user_id = metadata.get("user_id")
        if not user_id:
            raise UploadError("upload owner is missing")
        return user_id

    def status(self, upload_id, user_id):
        directory = self._directory(upload_id)
        with self._lock:
            metadata = self._read_metadata(directory)
            if metadata["user_id"] != user_id:
                raise UploadError("Upload belongs to another user")
            return {
                "upload_id": upload_id,
                "chunks_received": metadata["chunks_received"],
                "chunk_size": metadata["chunk_size"],
                "size": metadata["size"],
                "complete": False,
            }

    def write_chunk(self, upload_id, user_id, index, data):
        if not isinstance(index, int) or index < 0:
            raise UploadError("Invalid chunk index")
        if not isinstance(data, bytes):
            raise UploadError("Chunk body must be bytes")

        directory = self._directory(upload_id)

        with self._lock:
            metadata = self._read_metadata(directory)
            if metadata["user_id"] != user_id:
                raise UploadError("Upload belongs to another user")

            chunk_size = metadata["chunk_size"]
            total_size = metadata["size"]
            chunk_count = (total_size + chunk_size - 1) // chunk_size

            if index >= chunk_count:
                raise UploadError("Chunk index is outside the upload")
            if index != metadata["chunks_received"]:
                raise UploadError(
                    f"Expected chunk {metadata['chunks_received']}, got {index}"
                )

            expected_size = min(chunk_size, total_size - index * chunk_size)
            if len(data) != expected_size:
                raise UploadError(
                    f"Incorrect chunk size: expected {expected_size} bytes"
                )

            chunk_path = directory / f"{index:08d}.part"
            with chunk_path.open("xb") as file:
                file.write(data)
                file.flush()
                os.fsync(file.fileno())

            metadata["chunks_received"] += 1
            self._write_metadata(directory, metadata)

            return {
                "stored": True,
                "chunk_index": index,
                "chunks_received": metadata["chunks_received"],
                "total_chunks": chunk_count,
            }

    def verify(self, upload_id, user_id):
        directory = self._directory(upload_id)

        with self._lock:
            metadata = self._read_metadata(directory)
            if metadata["user_id"] != user_id:
                raise UploadError("Upload belongs to another user")

            expected_chunks = (
                metadata["size"] + metadata["chunk_size"] - 1
            ) // metadata["chunk_size"]

            if metadata["chunks_received"] != expected_chunks:
                raise UploadError("Upload is incomplete")

            digest = hashlib.sha256()
            actual_size = 0

            for index in range(expected_chunks):
                path = directory / f"{index:08d}.part"
                if not path.is_file():
                    raise UploadError(f"Missing chunk {index}")

                with path.open("rb") as file:
                    while True:
                        block = file.read(1024 * 1024)
                        if not block:
                            break
                        actual_size += len(block)
                        digest.update(block)

            if actual_size != metadata["size"]:
                raise UploadError("Completed file size does not match")

            return {
                "upload_id": upload_id,
                "size": actual_size,
                "sha256": digest.hexdigest(),
                "storage_key": metadata["storage_key"],
                "verified": True,
            }

    def discard(self, upload_id, user_id):
        directory = self._directory(upload_id)

        with self._lock:
            metadata = self._read_metadata(directory)
            if metadata["user_id"] != user_id:
                raise UploadError("Upload belongs to another user")
            shutil.rmtree(directory)
            return {"discarded": True}
