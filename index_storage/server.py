import socket
import hmac
import json
import os
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from index_storage.local import LocalIndexStorage
from index_storage.reverse import ReverseIndexStorage


STORAGE_ROOT = os.environ.get(
    "OUR_SEARCH_STORAGE_ROOT",
    "our_search_storage_data",
)

HOST = os.environ.get(
    "OUR_SEARCH_STORAGE_HOST",
    "0.0.0.0",
)

PORT = int(
    os.environ.get(
        "PORT",
        os.environ.get(
            "OUR_SEARCH_STORAGE_PORT",
            "9090",
        ),
    )
)

STORAGE_API_KEY = os.environ.get(
    "OUR_SEARCH_STORAGE_API_KEY",
)


if not STORAGE_API_KEY:
    raise RuntimeError(
        "OUR_SEARCH_STORAGE_API_KEY is required"
    )


if os.environ.get(
    "OUR_SEARCH_STORAGE_MODE",
    "local",
).strip().lower() == "reverse":

    storage = ReverseIndexStorage(
        command_url=os.environ.get(
            "OUR_SEARCH_STORAGE_REVERSE_URL",
            "http://127.0.0.1:9090/reverse-command",
        ),
        api_key=STORAGE_API_KEY,
        timeout=int(
            os.environ.get(
                "OUR_SEARCH_STORAGE_TIMEOUT",
                "35",
            )
        ),
    )

else:

    storage = LocalIndexStorage(
        STORAGE_ROOT
    )

REVERSE_COMMANDS = queue.Queue()
REVERSE_RESPONSES = queue.Queue()
REVERSE_PHONE_CONNECTED = threading.Event()


class StorageHTTPHandler(BaseHTTPRequestHandler):

    protocol_version = "HTTP/1.1"

    server_version = "OurSearchStorage/1.1"

    def _authorized(self):

        provided = self.headers.get(
            "X-Storage-API-Key",
            "",
        )

        return hmac.compare_digest(
            provided,
            STORAGE_API_KEY,
        )

    def _require_auth(self):

        if self._authorized():
            return True

        self._send_json(
            401,
            {
                "error": "unauthorized",
            },
        )

        return False

    def _send_json(
        self,
        status,
        payload,
    ):

        data = json.dumps(
            payload,
            ensure_ascii=False,
        ).encode("utf-8")

        self.send_response(status)

        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8",
        )

        self.send_header(
            "Content-Length",
            str(len(data)),
        )

        self.end_headers()

        self.wfile.write(data)

    def _send_bytes(
        self,
        status,
        data,
    ):

        self.send_response(status)

        self.send_header(
            "Content-Type",
            "application/octet-stream",
        )

        self.send_header(
            "Content-Length",
            str(len(data)),
        )

        self.end_headers()

        self.wfile.write(data)

    def _read_body(self):

        length = int(
            self.headers.get(
                "Content-Length",
                "0",
            )
        )

        return self.rfile.read(
            length
        )

    def do_GET(self):

        if self.path == "/health":

            self._send_json(
                200,
                {
                    "status": "ok",
                    "service": "our_search_storage",
                },
            )

            return

        if self.path == "/reverse-poll":
            if not self._require_auth():
                return

            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            REVERSE_PHONE_CONNECTED.set()

            ready = b"READY\\n"

            ready_chunk = (
                f"{len(ready):X}\\r\\n".encode("ascii")
                + ready
                + b"\\r\\n"
            )

            self.wfile.write(ready_chunk)
            self.wfile.flush()

            try:
                while True:
                    command = REVERSE_COMMANDS.get()

                    payload = (
                        command.encode("utf-8") + b"\\n"
                    )

                    chunk = (
                        f"{len(payload):X}\\r\\n".encode("ascii")
                        + payload
                        + b"\\r\\n"
                    )

                    self.wfile.write(chunk)
                    self.wfile.flush()

            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                REVERSE_PHONE_CONNECTED.clear()

            return

        if not self._require_auth():
            return

        parsed = urlparse(
            self.path
        )

        query = parse_qs(
            parsed.query
        )

        if parsed.path == "/exists":

            key = query.get(
                "key",
                [""],
            )[0]

            self._send_json(
                200,
                {
                    "key": key,
                    "exists": storage.exists(key),
                },
            )

            return

        if parsed.path == "/get":

            key = query.get(
                "key",
                [""],
            )[0]

            data = storage.get(key)

            if data is None:

                self._send_json(
                    404,
                    {
                        "error": "not_found",
                        "key": key,
                    },
                )

                return

            self._send_bytes(
                200,
                data,
            )

            return

        if parsed.path == "/list":

            prefix = query.get(
                "prefix",
                [""],
            )[0]

            self._send_json(
                200,
                {
                    "keys": storage.list_keys(
                        prefix
                    ),
                },
            )

            return

        self._send_json(
            404,
            {
                "error": "not_found",
            },
        )

    def do_POST(self):

        if self.path == "/reverse-command":
            if not self._require_auth():
                return

            length = int(
                self.headers.get("Content-Length", "0")
            )

            command = self.rfile.read(length).decode("utf-8").strip()

            if not command:
                self._send_json(
                    400,
                    {"error": "command_required"},
                )
                return

            if not REVERSE_PHONE_CONNECTED.is_set():
                self._send_json(
                    503,
                    {"error": "phone_not_connected"},
                )
                return

            REVERSE_COMMANDS.put(command)

            try:
                result = REVERSE_RESPONSES.get(
                    timeout=30
                )
            except queue.Empty:
                self._send_json(
                    504,
                    {
                        "error": "phone_response_timeout",
                        "command": command,
                    },
                )
                return

            self._send_json(
                200,
                {
                    "command": command,
                    "result": result,
                },
            )
            return

        if self.path == "/reverse-response":
            if not self._require_auth():
                return

            result = self._read_body().decode("utf-8")

            REVERSE_RESPONSES.put(result)

            self._send_json(
                200,
                {"received": True},
            )
            return

        self._send_json(
            404,
            {"error": "not_found"},
        )

    def do_PUT(self):

        if self.path != "/put":

            self._send_json(
                404,
                {
                    "error": "not_found",
                },
            )

            return

        if not self._require_auth():
            return

        key = self.headers.get(
            "X-Storage-Key",
            "",
        )

        if not key:

            self._send_json(
                400,
                {
                    "error": "storage_key_required",
                },
            )

            return

        data = self._read_body()

        storage.put(
            key,
            data,
        )

        self._send_json(
            200,
            {
                "stored": True,
                "key": key,
                "size": len(data),
            },
        )

    def do_DELETE(self):

        if self.path != "/delete":

            self._send_json(
                404,
                {
                    "error": "not_found",
                },
            )

            return

        if not self._require_auth():
            return

        key = self.headers.get(
            "X-Storage-Key",
            "",
        )

        if not key:

            self._send_json(
                400,
                {
                    "error": "storage_key_required",
                },
            )

            return

        deleted = storage.delete(
            key
        )

        self._send_json(
            200,
            {
                "deleted": deleted,
                "key": key,
            },
        )

    def log_message(
        self,
        format,
        *args,
    ):

        print(
            "%s - %s"
            % (
                self.address_string(),
                format % args,
            )
        )


def main():

    server = ThreadingHTTPServer(
        (
            HOST,
            PORT,
        ),
        StorageHTTPHandler,
    )

    print(
        "OUR SEARCH STORAGE SERVER"
    )

    print(
        f"Running on {HOST}:{PORT}"
    )

    print(
        f"Storage root: {STORAGE_ROOT}"
    )

    print(
        "API key authentication: enabled"
    )

    print(
        "Press Ctrl+C to stop."
    )

    try:

        server.serve_forever()

    except KeyboardInterrupt:

        print(
            "\nStopping storage server..."
        )

    finally:

        server.server_close()


if __name__ == "__main__":
    main()
