import http.server
import socketserver
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from crawler_system.fetcher import Fetcher


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    connection_ports = set()
    request_count = 0
    lock = threading.Lock()

    def do_GET(self):
        body = b'{"ok":true,"source":"stage5_3_pool_scaling"}'

        with Handler.lock:
            Handler.connection_ports.add(
                self.client_address[1]
            )
            Handler.request_count += 1

        self.send_response(200)
        self.send_header(
            "Content-Type",
            "application/json"
        )
        self.send_header(
            "Content-Length",
            str(len(body))
        )
        self.send_header(
            "Connection",
            "keep-alive"
        )
        self.end_headers()

        self.wfile.write(body)
        self.wfile.flush()

    def log_message(self, *args):
        pass


class Server(
    socketserver.ThreadingMixIn,
    http.server.HTTPServer
):
    daemon_threads = True
    allow_reuse_address = True


def run_case(
    pool_size,
    request_count=1000,
    concurrency=32,
):
    Handler.connection_ports.clear()
    Handler.request_count = 0

    server = Server(
        ("127.0.0.1", 0),
        Handler
    )

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True
    )
    thread.start()

    url = (
        f"http://127.0.0.1:"
        f"{server.server_port}/page"
    )

    fetcher = Fetcher(
        max_retries=0,
        max_connections_per_origin=pool_size,
    )

    def fetch_one(_):
        result = fetcher.fetch(url)

        if result["status"] != 200:
            raise RuntimeError(
                f"unexpected fetch result: {result}"
            )

        return True

    started = time.perf_counter()

    with ThreadPoolExecutor(
        max_workers=concurrency
    ) as executor:
        results = list(
            executor.map(
                fetch_one,
                range(request_count)
            )
        )

    elapsed = (
        time.perf_counter()
        - started
    )

    successes = sum(results)

    stats = fetcher.pool_stats()

    fetcher.close()

    server.shutdown()
    server.server_close()

    rate = (
        successes / elapsed
        if elapsed
        else 0.0
    )

    actual_connections = len(
        Handler.connection_ports
    )

    print(
        f"pool={pool_size:2d} | "
        f"requests={request_count:5d} | "
        f"success={successes:5d} | "
        f"time={elapsed:8.4f}s | "
        f"throughput={rate:9.2f}/s | "
        f"tcp_connections={actual_connections:4d} | "
        f"server_requests={Handler.request_count:5d}"
    )

    return rate, actual_connections, stats


print("=" * 72)
print("STAGE 5.3 — BOUNDED ORIGIN POOL SCALING")
print("=" * 72)
print("Concurrency: 32")
print("Requests:    1000")
print("=" * 72)

results = []

for pool_size in (1, 2, 4, 8, 16):
    results.append(
        (
            pool_size,
            *run_case(
                pool_size
            )
        )
    )

print("=" * 72)

best = max(
    results,
    key=lambda item: item[1]
)

print(
    f"BEST POOL: {best[0]}"
)
print(
    f"BEST THROUGHPUT: {best[1]:.2f}/s"
)

print("=" * 72)

if best[1] >= 166.74:
    print(
        "PASS — v3 reaches/exceeds "
        "established baseline"
    )
else:
    print(
        "FAIL — v3 remains below "
        "established baseline"
    )

print("=" * 72)
