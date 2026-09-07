import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class SearchRequestHandler(BaseHTTPRequestHandler):
    service = None

    def _send_json(self, status_code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, self.service.health())
            return
        self._send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path not in (
            "/search",
            "/index",
            "/delete",
        ):
            self._send_json(404, {"error": "not_found"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length <= 0:
                raise ValueError("request body is empty")
            if content_length > 1_000_000:
                raise ValueError("request body is too large")

            body = self.rfile.read(content_length)
            payload = json.loads(body.decode("utf-8"))

            if self.path == "/index":
                result = self.service.index_document(payload)

            elif self.path == "/delete":
                result = self.service.delete_document(payload)

            else:
                result = self.service.handle_request(payload)

            self._send_json(200, result)

        except json.JSONDecodeError:
            self._send_json(400, {"error": "invalid_json"})
        except ValueError as error:
            self._send_json(400, {"error": str(error)})
        except Exception as error:
            self._send_json(500, {"error": "internal_server_error", "message": str(error)})

    def log_message(self, format_string, *args):
        return


class SearchHTTPServer:
    def __init__(self, service, host="127.0.0.1", port=8080):
        SearchRequestHandler.service = service
        self.server = ThreadingHTTPServer((host, port), SearchRequestHandler)

    def serve_forever(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()

    def server_close(self):
        self.server.server_close()
