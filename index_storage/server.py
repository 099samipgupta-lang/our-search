import hmac
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from index_storage.local import LocalIndexStorage


STORAGE_ROOT = os.environ.get(
    "OUR_SEARCH_STORAGE_ROOT",
    "our_search_storage_data",
)

HOST = os.environ.get(
    "OUR_SEARCH_STORAGE_HOST",
    "127.0.0.1",
)

PORT = int(
    os.environ.get(
        "OUR_SEARCH_STORAGE_PORT",
        "9090",
    )
)

STORAGE_API_KEY = os.environ.get(
    "OUR_SEARCH_STORAGE_API_KEY",
)


if not STORAGE_API_KEY:
    raise RuntimeError(
        "OUR_SEARCH_STORAGE_API_KEY is required"
    )


storage = LocalIndexStorage(
    STORAGE_ROOT
)


class StorageHTTPHandler(BaseHTTPRequestHandler):

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
