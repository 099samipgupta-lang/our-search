import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from crawler_system.fetcher import Fetcher


# ============================================================
# TEST SERVER
# ============================================================

class GateHandler(BaseHTTPRequestHandler):

    protocol_version = "HTTP/1.1"

    server_version = "Stage5_3_Gate/1.0"

    def do_GET(self):

        self.server.request_count += 1

        path = self.path

        # Track TCP connections by client source port.
        self.server.connection_ports.add(
            self.client_address[1]
        )

        if path == "/large":

            body = b"x" * (
                self.server.large_body_size
            )

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "application/octet-stream",
            )
            self.send_header(
                "Content-Length",
                str(len(body)),
            )
            self.send_header(
                "Connection",
                "keep-alive",
            )
            self.end_headers()

            try:
                self.wfile.write(body)
            except (
                BrokenPipeError,
                ConnectionResetError,
            ):
                pass

            return

        if path == "/slow":

            time.sleep(
                self.server.response_delay
            )

        if path == "/fail":

            # Deliberately terminate the TCP connection
            # without producing a valid HTTP response.
            self.close_connection = True

            try:
                self.connection.shutdown(
                    2
                )
            except OSError:
                pass

            try:
                self.connection.close()
            except OSError:
                pass

            return

        body = json.dumps(
            {
                "ok": True,
                "path": path,
            }
        ).encode()

        self.send_response(200)

        self.send_header(
            "Content-Type",
            "application/json",
        )

        self.send_header(
            "Content-Length",
            str(len(body)),
        )

        self.send_header(
            "Connection",
            "keep-alive",
        )

        self.end_headers()

        try:
            self.wfile.write(body)
        except (
            BrokenPipeError,
            ConnectionResetError,
        ):
            pass

    def log_message(
        self,
        format_string,
        *args,
    ):
        return

    def handle_one_request(self):
        try:
            return super().handle_one_request()
        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
        ):
            return


class GateServer(
    ThreadingHTTPServer
):

    daemon_threads = True

    allow_reuse_address = True

    def __init__(
        self,
        address,
    ):

        super().__init__(
            address,
            GateHandler,
        )

        self.request_count = 0

        self.connection_ports = set()

        self.large_body_size = (
            2 * 1024 * 1024
        )

        self.response_delay = 0.05


# ============================================================
# SERVER LIFECYCLE
# ============================================================

def start_server():

    server = GateServer(
        ("127.0.0.1", 0)
    )

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )

    thread.start()

    return server, thread


def stop_server(
    server,
    thread,
):

    server.shutdown()

    server.server_close()

    thread.join(
        timeout=5
    )

    return not thread.is_alive()


# ============================================================
# TEST 1 — CONNECTION LIMIT
# ============================================================

def test_connection_limit():

    server, thread = start_server()

    fetcher = Fetcher(
        max_retries=0,
        max_connections_per_origin=4,
        timeout=10,
    )

    try:

        url = (
            f"http://127.0.0.1:"
            f"{server.server_port}/limit"
        )

        def fetch_one(_):

            result = fetcher.fetch(
                url
            )

            if result["status"] != 200:

                raise RuntimeError(
                    f"unexpected status: "
                    f"{result}"
                )

            return result

        with ThreadPoolExecutor(
            max_workers=32
        ) as executor:

            futures = [
                executor.submit(
                    fetch_one,
                    i,
                )
                for i in range(200)
            ]

            for future in as_completed(
                futures
            ):
                future.result()

        stats = fetcher.pool_stats()

        origin = (
            "http",
            "127.0.0.1",
            server.server_port,
        )

        origin_stats = stats[
            origin
        ]

        created = origin_stats[
            "total_created"
        ]

        peak = origin_stats["created"]

        tcp_connections = len(
            server.connection_ports
        )

        passed = (
            peak <= 4
            and tcp_connections <= 4
            and created <= 4
        )

        print(
            "TEST 1 — CONNECTION LIMIT"
        )

        print(
            f"configured_limit=4 "
            f"peak_pool={peak} "
            f"total_created={created} "
            f"tcp_connections="
            f"{tcp_connections}"
        )

        if passed:
            print(
                "PASS — connection limit enforced"
            )
        else:
            print(
                "FAIL — connection limit violated"
            )

        return passed

    finally:

        fetcher.close()

        stop_server(
            server,
            thread,
        )


# ============================================================
# TEST 2 — CONNECTION REPLACEMENT
# ============================================================

def test_connection_replacement():

    server, thread = start_server()

    fetcher = Fetcher(
        max_retries=0,
        max_connections_per_origin=2,
        timeout=3,
    )

    try:

        fail_url = (
            f"http://127.0.0.1:"
            f"{server.server_port}/fail"
        )

        success_url = (
            f"http://127.0.0.1:"
            f"{server.server_port}/replacement"
        )

        first = fetcher.fetch(
            fail_url
        )

        failed = (
            first["status"] == 0
            and first["status_type"]
            in {
                "network_error",
                "fetch_error",
            }
        )

        before = (
            fetcher.pool_stats()
        )

        second = fetcher.fetch(
            success_url
        )

        after = (
            fetcher.pool_stats()
        )

        replacement_success = (
            second["status"] == 200
        )

        created = 0

        for stats in after.values():

            created += stats[
                "total_created"
            ]

        discarded = 0

        for stats in after.values():

            discarded += stats[
                "total_discarded"
            ]

        passed = (
            failed
            and replacement_success
            and discarded >= 1
            and created >= 2
        )

        print(
            "TEST 2 — CONNECTION REPLACEMENT"
        )

        print(
            f"initial_failure="
            f"{failed} "
            f"replacement_success="
            f"{replacement_success} "
            f"total_created="
            f"{created} "
            f"total_discarded="
            f"{discarded}"
        )

        if passed:
            print(
                "PASS — failed connections "
                "are discarded and replaced"
            )
        else:
            print(
                "FAIL — connection replacement "
                "invariant violated"
            )

        return passed

    finally:

        fetcher.close()

        stop_server(
            server,
            thread,
        )


# ============================================================
# TEST 3 — CONCURRENT ORIGIN ISOLATION
# ============================================================

def test_origin_isolation():

    server_a, thread_a = start_server()

    server_b, thread_b = start_server()

    fetcher = Fetcher(
        max_retries=0,
        max_connections_per_origin=2,
        timeout=10,
    )

    try:

        url_a = (
            f"http://127.0.0.1:"
            f"{server_a.server_port}/a"
        )

        url_b = (
            f"http://127.0.0.1:"
            f"{server_b.server_port}/b"
        )

        def fetch(url):

            result = fetcher.fetch(
                url
            )

            if result["status"] != 200:

                raise RuntimeError(
                    f"unexpected result: "
                    f"{result}"
                )

            return result

        urls = (
            [url_a] * 100
            + [url_b] * 100
        )

        with ThreadPoolExecutor(
            max_workers=32
        ) as executor:

            futures = [
                executor.submit(
                    fetch,
                    url,
                )
                for url in urls
            ]

            for future in as_completed(
                futures
            ):

                future.result()

        stats = fetcher.pool_stats()

        origin_a = (
            "http",
            "127.0.0.1",
            server_a.server_port,
        )

        origin_b = (
            "http",
            "127.0.0.1",
            server_b.server_port,
        )

        stats_a = stats[
            origin_a
        ]

        stats_b = stats[
            origin_b
        ]

        connections_a = len(
            server_a.connection_ports
        )

        connections_b = len(
            server_b.connection_ports
        )

        passed = (
            stats_a["created"] <= 2
            and stats_b["created"] <= 2
            and connections_a <= 2
            and connections_b <= 2
            and server_a.request_count
            == 100
            and server_b.request_count
            == 100
        )

        print(
            "TEST 3 — CONCURRENT ORIGIN ISOLATION"
        )

        print(
            f"origin_a_connections="
            f"{connections_a} "
            f"origin_b_connections="
            f"{connections_b} "
            f"origin_a_requests="
            f"{server_a.request_count} "
            f"origin_b_requests="
            f"{server_b.request_count}"
        )

        if passed:
            print(
                "PASS — origins have independent "
                "connection pools"
            )
        else:
            print(
                "FAIL — origin isolation violated"
            )

        return passed

    finally:

        fetcher.close()

        stop_server(
            server_a,
            thread_a,
        )

        stop_server(
            server_b,
            thread_b,
        )


# ============================================================
# TEST 4 — RESPONSE SIZE UNDER CONCURRENCY
# ============================================================

def test_concurrent_size_protection():

    server, thread = start_server()

    fetcher = Fetcher(
        max_retries=0,
        max_connections_per_origin=4,
        timeout=10,
        max_body_bytes=1024,
    )

    try:

        url = (
            f"http://127.0.0.1:"
            f"{server.server_port}/large"
        )

        def fetch_one(_):

            return fetcher.fetch(
                url
            )

        with ThreadPoolExecutor(
            max_workers=32
        ) as executor:

            futures = [
                executor.submit(
                    fetch_one,
                    i,
                )
                for i in range(100)
            ]

            results = [
                future.result()
                for future in as_completed(
                    futures
                )
            ]

        oversized = [
            result
            for result in results
            if result["status_type"]
            == "body_too_large"
        ]

        unexpected = [
            result
            for result in results
            if result["status_type"]
            != "body_too_large"
        ]

        stats = fetcher.pool_stats()

        max_created = max(
            (
                item["created"]
                + item["available"]
                for item in stats.values()
            ),
            default=0,
        )

        passed = (
            len(oversized) == 100
            and not unexpected
            and max_created <= 4
        )

        print(
            "TEST 4 — CONCURRENT "
            "RESPONSE-SIZE PROTECTION"
        )

        print(
            f"requests=100 "
            f"protected={len(oversized)} "
            f"unexpected="
            f"{len(unexpected)} "
            f"max_pool={max_created}"
        )

        if passed:
            print(
                "PASS — response-size protection "
                "holds under concurrency"
            )
        else:
            print(
                "FAIL — concurrent size protection "
                "invariant violated"
            )

        return passed

    finally:

        fetcher.close()

        stop_server(
            server,
            thread,
        )


# ============================================================
# TEST 5 — CLEAN SHUTDOWN
# ============================================================

def test_clean_shutdown():

    server, thread = start_server()

    fetcher = Fetcher(
        max_retries=0,
        max_connections_per_origin=4,
        timeout=10,
    )

    try:

        url = (
            f"http://127.0.0.1:"
            f"{server.server_port}/shutdown"
        )

        with ThreadPoolExecutor(
            max_workers=16
        ) as executor:

            futures = [
                executor.submit(
                    fetcher.fetch,
                    url,
                )
                for _ in range(100)
            ]

            results = [
                future.result()
                for future in as_completed(
                    futures
                )
            ]

        successful = sum(
            result["status"] == 200
            for result in results
        )

        before = (
            fetcher.pool_stats()
        )

        fetcher.close()

        after = (
            fetcher.pool_stats()
        )

        closed_empty = all(
            item["created"] == 0
            and item["available"] == 0
            and item["in_use"] == 0
            for item in after.values()
        )

        post_close_result = fetcher.fetch(
            url
        )

        post_close_rejected = (
            post_close_result.get("status") == 0
            and post_close_result.get("status_type")
            == "fetch_error"
            and post_close_result.get("error")
            == "Fetcher is closed"
        )

        passed = (
            successful == 100
            and closed_empty
            and post_close_rejected
        )

        print(
            "TEST 5 — CLEAN SHUTDOWN"
        )

        print(
            f"successful={successful} "
            f"closed_empty="
            f"{closed_empty} "
            f"post_close_rejected="
            f"{post_close_rejected}"
        )

        if passed:
            print(
                "PASS — clean shutdown verified"
            )
        else:
            print(
                "FAIL — shutdown invariant violated"
            )

        return passed

    finally:

        try:
            fetcher.close()
        except Exception:
            pass

        stop_server(
            server,
            thread,
        )


# ============================================================
# FINAL GATE
# ============================================================

def main():

    print("=" * 72)

    print(
        "STAGE 5.3 — RESOURCE CONTROL & "
        "FAILURE GATE"
    )

    print("=" * 72)

    tests = [
        test_connection_limit,
        test_connection_replacement,
        test_origin_isolation,
        test_concurrent_size_protection,
        test_clean_shutdown,
    ]

    results = []

    for test in tests:

        try:

            result = test()

        except Exception as error:

            print(
                "FAIL — unexpected test exception:"
            )

            print(
                repr(error)
            )

            result = False

        results.append(
            result
        )

        print("=" * 72)

    passed = all(
        results
    )

    print(
        "RESOURCE CONTROL & FAILURE RESULTS"
    )

    print(
        f"connection_limit="
        f"{'PASS' if results[0] else 'FAIL'}"
    )

    print(
        f"connection_replacement="
        f"{'PASS' if results[1] else 'FAIL'}"
    )

    print(
        f"origin_isolation="
        f"{'PASS' if results[2] else 'FAIL'}"
    )

    print(
        f"concurrent_size_protection="
        f"{'PASS' if results[3] else 'FAIL'}"
    )

    print(
        f"clean_shutdown="
        f"{'PASS' if results[4] else 'FAIL'}"
    )

    print("=" * 72)

    if passed:

        print(
            "RESULT: PASS"
        )

        print(
            "STAGE 5.3 RESOURCE CONTROL & "
            "FAILURE GATE: PASS"
        )

    else:

        print(
            "RESULT: FAIL"
        )

        print(
            "STAGE 5.3 RESOURCE CONTROL & "
            "FAILURE GATE: FAIL"
        )

    print("=" * 72)

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
