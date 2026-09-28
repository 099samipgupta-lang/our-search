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
        timeout=35,
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

    def get(self, key):

        result = self._command(
            "GET " + key
        )

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

        result = self._command(
            "DELETE " + key
        )

        return result == "OK"

    def list_keys(self, prefix=""):

        result = self._command(
            "LIST " + prefix
            if prefix
            else "LIST"
        )

        if not result:
            return []

        return result.splitlines()
