import os
import shutil
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from crawler_system.whole_web_crawler import WholeWebCrawler


# ============================================================
# Local deterministic Web
# ============================================================

class TestHandler(BaseHTTPRequestHandler):

    content = (
        b"<html><head><title>Stage 10</title></head>"
        b"<body>Stage 10 production continuous crawler test"
        b"<a href='/page2'>page two</a></body></html>"
    )

    page2 = (
        b"<html><head><title>Page Two</title></head>"
        b"<body>Second indexed page</body></html>"
    )

    def do_GET(self):
        if self.path == "/robots.txt":
            body = b"User-agent: OurSearchBot\nAllow: /\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/page2":
            body = self.page2
            self.send_response(200)
        elif self.path == "/gone":
            self.send_response(404)
            self.end_headers()
            return
        else:
            body = self.content
            self.send_response(200)

        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)

    print("PASS — " + message)


def wait_until(predicate, timeout=15.0, interval=0.2):
    deadline = time.time() + timeout

    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)

    return False


def main():

    root = tempfile.mkdtemp(
        prefix="stage10_lifecycle_"
    )

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        TestHandler,
    )

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )
    thread.start()

    port = server.server_address[1]
    seed = f"http://127.0.0.1:{port}/"

    crawler = None
    loop_thread = None

    try:

        print("==============================================")
        print("STAGE 10 — LIFECYCLE MISSION")
        print("==============================================")

        # ----------------------------------------------------
        # 10.A Construction
        # ----------------------------------------------------

        crawler = WholeWebCrawler(
            worker_count=2,
            frontier_delay=0,
            task_timeout=5,
            max_attempts=3,
            storage_root=root,
        )

        assert_true(
            crawler.running is False,
            "crawler starts stopped",
        )

        assert_true(
            crawler.url_state is not None,
            "durable URL state exists",
        )

        assert_true(
            crawler.index_integration is not None,
            "crawler-to-index integration exists",
        )

        # ----------------------------------------------------
        # 10.B Seed
        # ----------------------------------------------------

        added = crawler.add_seed(seed)

        assert_true(
            added is True,
            "local deterministic seed accepted",
        )

        # ----------------------------------------------------
        # 10.C Continuous operation
        # ----------------------------------------------------

        loop_thread = threading.Thread(
            target=crawler.continuous_loop,
            kwargs={
                "interval": 0.1,
                "persist_every": 0.5,
            },
            daemon=True,
        )

        loop_thread.start()

        assert_true(
            wait_until(
                lambda: crawler.status()["stats"][
                    "stage10_cycles"
                ] >= 2,
                timeout=10,
            ),
            "continuous loop executes multiple cycles",
        )

        assert_true(
            wait_until(
                lambda: crawler.status()["stats"][
                    "pages_completed"
                ] >= 1,
                timeout=15,
            ),
            "continuous crawler completes a real Web fetch",
        )

        assert_true(
            crawler.status()["stats"][
                "stage10_persistence_saves"
            ] >= 1,
            "continuous operation performs durable checkpoints",
        )

        # ----------------------------------------------------
        # 10.D Discovery
        # ----------------------------------------------------

        assert_true(
            wait_until(
                lambda: crawler.status()["stats"][
                    "discovered"
                ] >= 2,
                timeout=15,
            ),
            "continuous crawl discovers linked Web content",
        )

        # ----------------------------------------------------
        # 10.E Index integration
        # ----------------------------------------------------

        indexing_stats = crawler.status()[
            "indexing"
        ].get("stats", {})

        assert_true(
            indexing_stats.get("new_indexed", 0) >= 1,
            "crawler successfully propagates a new page to the index",
        )

        # ----------------------------------------------------
        # 10.F Stop
        # ----------------------------------------------------

        crawler.stop()

        assert_true(
            crawler.running is False,
            "stop requests crawler shutdown",
        )

        assert_true(
            wait_until(
                lambda: not loop_thread.is_alive(),
                timeout=10,
            ),
            "continuous loop exits after stop",
        )

        print(
            "PASS — continuous shutdown thread completed"
        )

        # ----------------------------------------------------
        # 10.G Durable restart
        # ----------------------------------------------------

        # Create a new crawler against the SAME durable
        # storage directory.
        crawler2 = WholeWebCrawler(
            worker_count=2,
            frontier_delay=0,
            task_timeout=5,
            max_attempts=3,
            storage_root=root,
        )

        try:

            counts = crawler2.url_state.counts()

            assert_true(
                sum(counts.values()) >= 1,
                "URL state survives crawler restart",
            )

            assert_true(
                crawler2.status()["indexing"] != {},
                "index integration reconstructs after restart",
            )

            # ------------------------------------------------
            # 10.H Lease recovery
            # ------------------------------------------------

            # Explicitly create a leased URL and then verify
            # the recovery mechanism can return it to durable
            # retry state.
            leased_url = f"http://127.0.0.1:{port}/lease-test"

            crawler2.url_state.add_discovered(
                leased_url,
                "stage10-lease-test",
                "127.0.0.1",
                priority=100,
                source="stage10-test",
            )

            crawler2.url_state.mark_queued(
                leased_url,
                priority=100,
            )

            claimed = crawler2.url_state.claim_next(
                owner="stage10-test-owner",
                now=time.time(),
            )

            assert_true(
                claimed is not None,
                "test URL can enter durable leased state",
            )

            # Force lease to become recoverable by directly
            # setting an old leased_at timestamp.
            crawler2.url_state._connection.execute(
                """
                UPDATE urls
                SET leased_at = ?
                WHERE url = ?
                """,
                (
                    time.time() - 100,
                    leased_url,
                ),
            )
            crawler2.url_state._connection.commit()

            recovered = crawler2.url_state.recover_expired_leases(
                lease_timeout=10,
                now=time.time(),
            )

            assert_true(
                recovered >= 1,
                "expired durable lease is recovered",
            )

            recovered_row = crawler2.url_state.get(
                leased_url
            )

            assert_true(
                recovered_row["state"] == "retry",
                "recovered lease returns to retry state",
            )

        finally:
            crawler2.stop()

        print("==============================================")
        print("STAGE 10 LIFECYCLE MISSION RESULT: PASS")
        print("==============================================")

    finally:

        if crawler is not None and crawler.running:
            try:
                crawler.stop()
            except Exception:
                pass

        if loop_thread is not None and loop_thread.is_alive():
            loop_thread.join(timeout=5)

        server.shutdown()
        server.server_close()

        shutil.rmtree(
            root,
            ignore_errors=True,
        )


if __name__ == "__main__":
    main()
