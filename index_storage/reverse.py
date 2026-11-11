import base64
import json
import threading
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from index_storage.backend import IndexStorageBackend


class ReverseIndexStorage(IndexStorageBackend):

    def __init__(
        self,
        command_url,
        api_key,
        timeout=180,
    ):

        if not isinstance(command_url, str):
            raise TypeError(
                "command_url must be a string"
            )

        if not isinstance(api_key, str):
            raise TypeError(
                "api_key must be a string"
            )

        command_url = command_url.strip().rstrip("/")
        api_key = api_key.strip()

        if not command_url:
            raise ValueError(
                "command_url must not be empty"
            )

        if not api_key:
            raise ValueError(
                "api_key must not be empty"
            )

        self.command_url = command_url
        self.api_key = api_key
        self.timeout = max(
            1,
            int(timeout),
        )

        # The reverse storage protocol uses a shared response queue.
        # Serialize commands so concurrent requests cannot receive
        # another request's response.
        self._command_lock = threading.Lock()

    def _command(self, command):

        with self._command_lock:

            request_id = uuid.uuid4().hex

            wire_command = (
                "REQUEST "
                + request_id
                + " "
                + command
            )

            request = Request(
                self.command_url,
                data=wire_command.encode("utf-8"),
                method="POST",
                headers={
                    "X-Storage-API-Key": self.api_key,
                    "Content-Type": "text/plain",
                },
            )

            payload = None
            last_error = None

            for attempt in range(4):
                try:
                    with urlopen(
                        request,
                        timeout=self.timeout,
                    ) as response:
                        payload = json.loads(
                            response.read().decode("utf-8")
                        )
                    break

                except HTTPError as error:
                    last_error = error

                    if error.code != 429 or attempt == 3:
                        raise

                    retry_after = error.headers.get(
                        "Retry-After"
                    )

                    try:
                        delay = float(retry_after)
                    except (TypeError, ValueError):
                        delay = 2 ** attempt

                    time.sleep(
                        min(max(delay, 1.0), 15.0)
                    )

            if payload is None:
                raise last_error

            if payload.get("command") != wire_command:
                raise RuntimeError(
                    "reverse storage command mismatch"
                )

            result = payload.get("result", "")

            return result

    def put(self, key, data):

        if not isinstance(data, bytes):
            raise TypeError(
                "data must be bytes"
            )

        encoded = base64.b64encode(data).decode("ascii")

        result = self._command(
            "PUT "
            + key
            + " "
            + encoded
        )

        if result != "OK":
            raise RuntimeError(
                f"reverse storage PUT failed: {result}"
            )

    def _get_raw(self, key):
        import base64

        result = self._command("GET " + key)

        if result == "NOT_FOUND":
            return None

        if not result.startswith("DATA:"):
            raise RuntimeError(
                "reverse storage GET returned invalid data"
            )

        try:
            return base64.b64decode(
                result[5:].encode("ascii"),
                validate=True,
            )
        except Exception as exc:
            raise RuntimeError(
                "reverse storage GET returned invalid Base64"
            ) from exc

    def get(self, key):
        import hashlib
        import json

        data = self._get_raw(key)

        if data is None:
            return None

        try:
            manifest = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return data

        if not isinstance(manifest, dict) or (
            manifest.get("format") != "our-search-chunked-media-v1"
        ):
            return data

        count = manifest.get("chunk_count")
        size = manifest.get("size")
        prefix = manifest.get("parts_prefix")
        expected_checksum = manifest.get("sha256")

        if (
            not isinstance(count, int)
            or isinstance(count, bool)
            or count <= 0
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size <= 0
            or not isinstance(prefix, str)
            or not isinstance(expected_checksum, str)
        ):
            raise RuntimeError("Invalid chunked storage manifest")

        output = bytearray()
        digest = hashlib.sha256()

        for index in range(count):
            part = self._get_raw(
                prefix + f"{index:08d}"
            )

            if part is None:
                raise RuntimeError(
                    f"Missing stored object part {index}"
                )

            output.extend(part)
            digest.update(part)

        if len(output) != size:
            raise RuntimeError("Stored object size verification failed")

        if digest.hexdigest() != expected_checksum:
            raise RuntimeError("Stored object checksum verification failed")

        return bytes(output)

    def exists(self, key):

        result = self._command(
            "EXISTS " + key
        )

        if result == "TRUE":
            return True

        if result == "FALSE":
            return False

        raise RuntimeError(
            "reverse storage EXISTS returned invalid result"
        )

    def delete(self, key):
        import json

        raw = self._get_raw(key)

        if raw is None:
            return False

        try:
            manifest = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            manifest = None

        if (
            isinstance(manifest, dict)
            and manifest.get("format") == "our-search-chunked-media-v1"
        ):
            count = manifest.get("chunk_count")
            prefix = manifest.get("parts_prefix")

            if (
                not isinstance(count, int)
                or isinstance(count, bool)
                or count < 0
                or not isinstance(prefix, str)
            ):
                raise RuntimeError("Invalid chunked storage manifest")

            for index in range(count):
                result = self._command(
                    "DELETE " + prefix + f"{index:08d}"
                )
                if result not in ("OK", "NOT_FOUND"):
                    raise RuntimeError(
                        f"Failed to delete stored object part {index}"
                    )

        result = self._command("DELETE " + key)
        return result == "OK"

    def list_keys(self, prefix=""):
        result = self._command(
            "LIST " + prefix
            if prefix
            else "LIST"
        )

        if not result:
            return []

        return [
            key for key in result.splitlines()
            if ".parts/" not in key
        ]

    def publish_staged_upload(
        self,
        key,
        chunk_paths,
        expected_size,
        expected_checksum,
    ):
        import hashlib
        import json
        import os

        transport_chunk_size = 1024 * 1024
        part_prefix = key + ".parts/"
        uploaded_keys = []
        digest = hashlib.sha256()
        actual_size = 0
        part_index = 0

        try:
            for path in chunk_paths:
                with open(path, "rb") as source:
                    while True:
                        block = source.read(transport_chunk_size)
                        if not block:
                            break

                        digest.update(block)
                        actual_size += len(block)

                        part_key = (
                            part_prefix
                            + f"{part_index:08d}"
                        )

                        self.put(part_key, block)
                        uploaded_keys.append(part_key)
                        part_index += 1

            actual_checksum = digest.hexdigest()

            if actual_size != expected_size:
                raise RuntimeError(
                    "reverse upload size verification failed"
                )

            if actual_checksum != expected_checksum:
                raise RuntimeError(
                    "reverse upload checksum verification failed"
                )

            manifest = {
                "format": "our-search-chunked-media-v1",
                "storage_key": key,
                "size": actual_size,
                "sha256": actual_checksum,
                "chunk_size": transport_chunk_size,
                "chunk_count": part_index,
                "parts_prefix": part_prefix,
            }

            self.put(
                key,
                json.dumps(
                    manifest,
                    separators=(",", ":"),
                ).encode("utf-8"),
            )

            return manifest

        except Exception:
            for part_key in uploaded_keys:
                try:
                    self.delete(part_key)
                except Exception:
                    pass
            raise
