import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


BACKENDS = [
    "http://127.0.0.1:8083",
    "http://127.0.0.1:8084",
    "http://127.0.0.1:8085",
]

BACKEND_TIMEOUT = 5
_gateway_lock = threading.Lock()
_next_backend = 0


def get_next_backend():
    global _next_backend

    with _gateway_lock:
        backend = BACKENDS[_next_backend]
        _next_backend = (_next_backend + 1) % len(BACKENDS)

    return backend


def request_backend(method, path, body=None):
    tried = set()

    for _ in range(len(BACKENDS)):
        backend = get_next_backend()

        if backend in tried:
            continue

        tried.add(backend)

        try:
            url = backend + path

            request = Request(
                url,
                data=body,
                method=method,
                headers={
                    "Content-Type": "application/json",
                },
            )

            with urlopen(request, timeout=BACKEND_TIMEOUT) as response:
                response_body = response.read()

                return (
                    response.status,
                    response_body,
                    response.headers.get(
                        "Content-Type",
                        "application/json",
                    ),
                )

        except HTTPError as error:
            try:
                error_body = error.read()
            except Exception:
                error_body = json.dumps({
                    "error": "backend_http_error",
                    "status": error.code,
                }).encode("utf-8")

            return (
                error.code,
                error_body,
                error.headers.get(
                    "Content-Type",
                    "application/json",
                ),
            )

        except (URLError, TimeoutError, ConnectionError):
            continue

    return 503, json.dumps({
        "error": "no_search_servers_available"
    }).encode("utf-8"), "application/json"


class SearchGatewayHandler(BaseHTTPRequestHandler):

    def _send_response(self, status, body, content_type):
        self.send_response(status)
        self.send_header(
            "Content-Type",
            content_type,
        )
        self.send_header(
            "Content-Length",
            str(len(body)),
        )
        self.send_header(
            "Access-Control-Allow-Origin",
            "*",
        )
        self.send_header(
            "Cache-Control",
            "no-store",
        )
        self.end_headers()

        try:
            self.wfile.write(body)
            self.wfile.flush()
        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
        ):
            pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header(
            "Access-Control-Allow-Origin",
            "*",
        )
        self.send_header(
            "Access-Control-Allow-Methods",
            "GET, POST, OPTIONS",
        )
        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type",
        )
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({
                "status": "ok",
                "service": "search_gateway",
                "backends": BACKENDS,
            }).encode("utf-8")

            self._send_response(
                200,
                body,
                "application/json",
            )
            return

        if self.path == "/search":
            status, body, content_type = request_backend(
                "GET",
                "/search",
            )

            self._send_response(
                status,
                body,
                content_type,
            )
            return

        self._send_response(
            404,
            json.dumps({
                "error": "not_found"
            }).encode("utf-8"),
            "application/json",
        )

    def do_POST(self):
        if self.path not in (
            "/search",
            "/index",
            "/delete",
            "/flush",
        ):
            self._send_response(
                404,
                json.dumps({
                    "error": "not_found"
                }).encode("utf-8"),
                "application/json",
            )
            return

        try:
            content_length = int(
                self.headers.get("Content-Length", "0")
            )

            if content_length <= 0:
                raise ValueError("request body is empty")

            if content_length > 1_000_000:
                raise ValueError("request body is too large")

            body = self.rfile.read(content_length)

            status, response_body, content_type = request_backend(
                "POST",
                self.path,
                body,
            )

            self._send_response(
                status,
                response_body,
                content_type,
            )

        except ValueError as error:
            self._send_response(
                400,
                json.dumps({
                    "error": str(error)
                }).encode("utf-8"),
                "application/json",
            )

    def log_message(self, format_string, *args):
        return


class SearchGatewayServer:
    def __init__(
        self,
        host="0.0.0.0",
        port=8080,
    ):
        self.server = ThreadingHTTPServer(
            (host, port),
            SearchGatewayHandler,
        )

    def serve_forever(self):
        self.server.serve_forever()

    def server_close(self):
        self.server.server_close()


if __name__ == "__main__":
    HOST = "0.0.0.0"
    PORT = 8080

    print(
        "OUR SEARCH SEARCH GATEWAY",
        flush=True,
    )
    print(
        f"Running on {HOST}:{PORT}",
        flush=True,
    )

    server = SearchGatewayServer(
        host=HOST,
        port=PORT,
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(
            "\nStopping gateway...",
            flush=True,
        )
    finally:
        server.server_close()
