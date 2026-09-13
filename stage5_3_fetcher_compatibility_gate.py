import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from crawler_system.fetcher import Fetcher


class Handler(BaseHTTPRequestHandler):
    counter = 0
    lock = threading.Lock()

    def do_GET(self):
        with self.lock:
            type(self).counter += 1

        if self.path == "/success":
            body = b"our-search-success"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", '"test-etag"')
            self.send_header("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/not-modified":
            self.send_response(304)
            self.send_header("ETag", '"test-etag"')
            self.end_headers()
            return

        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/success")
            self.end_headers()
            return

        if self.path == "/loop-a":
            self.send_response(302)
            self.send_header("Location", "/loop-b")
            self.end_headers()
            return

        if self.path == "/loop-b":
            self.send_response(302)
            self.send_header("Location", "/loop-a")
            self.end_headers()
            return

        if self.path == "/not-found":
            self.send_response(404)
            self.end_headers()
            return

        if self.path == "/large":
            body = b"x" * (1024 * 1024)
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        self.send_response(500)
        self.end_headers()

    def log_message(self, format, *args):
        return


def main():
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        Handler,
    )

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )
    thread.start()

    host, port = server.server_address
    base = f"http://{host}:{port}"

    fetcher = Fetcher(
        max_retries=0,
        max_redirects=5,
        timeout=5,
        max_body_bytes=2 * 1024 * 1024,
    )

    try:
        result = fetcher.fetch(base + "/success")

        assert result["status"] == 200
        assert result["status_type"] == "success"
        assert result["body"] == b"our-search-success"
        assert result["etag"] == '"test-etag"'
        assert result["last_modified"] == "Wed, 01 Jan 2025 00:00:00 GMT"
        print("PASS — successful fetch")

        result = fetcher.fetch(
            base + "/not-modified",
            etag='"test-etag"',
            last_modified="Wed, 01 Jan 2025 00:00:00 GMT",
        )

        assert result["status"] == 304
        assert result["status_type"] == "not_modified"
        print("PASS — conditional fetch / 304")

        result = fetcher.fetch(base + "/redirect")

        assert result["status"] == 200
        assert result["final_url"].endswith("/success")
        assert len(result["redirect_chain"]) == 1
        print("PASS — redirect handling")

        result = fetcher.fetch(base + "/loop-a")

        assert result["status_type"] == "redirect_loop"
        print("PASS — redirect-loop protection")

        result = fetcher.fetch(base + "/not-found")

        assert result["status"] == 404
        assert result["status_type"] == "not_found"
        print("PASS — 404 classification")

        limited_fetcher = Fetcher(
            max_retries=0,
            max_body_bytes=1024,
        )

        result = limited_fetcher.fetch(
            base + "/large"
        )

        assert result["status_type"] == "body_too_large"
        assert len(result["body"]) <= 1024
        limited_fetcher.close()

        print("PASS — response-size protection")

        fetcher.close()

        print("=" * 72)
        print("STAGE 5.3 FETCHER COMPATIBILITY GATE: PASS")
        print("=" * 72)

    finally:
        try:
            fetcher.close()
        except Exception:
            pass

        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
