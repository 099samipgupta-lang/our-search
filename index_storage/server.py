import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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


storage = LocalIndexStorage(
    STORAGE_ROOT
)


class StorageHTTPHandler(BaseHTTPRequestHandler):

    server_version = "OurSearchStorage/1.0"

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

    def _read_json(self):

        length = int(
            self.headers.get(
                "Content-Length",
                "0",
            )
        )

        body = self.rfile.read(
            length
        )

        return json.loads(
            body.decode("utf-8")
        )

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

        if self.path.startswith(
            "/exists?key="
        ):

            from urllib.parse import parse_qs, urlparse

            query = parse_qs(
                urlparse(
                    self.path
                ).query
            )

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

        if self.path.startswith(
            "/get?key="
        ):

            from urllib.parse import parse_qs, urlparse

            query = parse_qs(
                urlparse(
                    self.path
                ).query
            )

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

        if self.path.startswith(
            "/list?prefix="
        ):

            from urllib.parse import parse_qs, urlparse

            query = parse_qs(
                urlparse(
                    self.path
                ).query
            )

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

        if not self.path == "/put":

            self._send_json(
                404,
                {
                    "error": "not_found",
                },
            )

            return

        try:

            key = self.headers.get(
                "X-Storage-Key"
            )

            if not key:

                self._send_json(
                    400,
                    {
                        "error": "missing_storage_key",
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
                    "status": "stored",
                    "key": key,
                    "bytes": len(data),
                },
            )

        except Exception as error:

            self._send_json(
                400,
                {
                    "error": str(error),
                },
            )

    def do_DELETE(self):

        if not self.path == "/delete":

            self._send_json(
                404,
                {
                    "error": "not_found",
                },
            )

            return

        try:

            key = self.headers.get(
                "X-Storage-Key"
            )

            if not key:

                self._send_json(
                    400,
                    {
                        "error": "missing_storage_key",
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

        except Exception as error:

            self._send_json(
                400,
                {
                    "error": str(error),
                },
            )

    def log_message(
        self,
        format,
        *args,
    ):

        print(
            f"[storage] {self.address_string()} "
            f"{format % args}"
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
        f"Storage root: {os.path.abspath(STORAGE_ROOT)}"
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
