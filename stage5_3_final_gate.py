import http.server
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from crawler_system.fetcher import Fetcher
from crawler_system.policy import CrawlPolicy
from crawler_system.task import CrawlTask
from crawler_system.worker import CrawlWorker
from crawler_system.worker_pool import WorkerPool


BASELINE_THROUGHPUT = 166.74
PERFORMANCE_REQUESTS = 1000
PERFORMANCE_CONCURRENCY = 32
PERFORMANCE_POOL_SIZE = 16
POOL_TASKS = 32


class TestHandler(http.server.BaseHTTPRequestHandler):

    protocol_version = "HTTP/1.1"

    def do_GET(self):

        body = (
            b'{"ok":true,"source":"stage5_3_final_gate"}'
        )

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

        self.wfile.write(body)
        self.wfile.flush()

    def log_message(self, format, *args):
        return


class TestServer(
    http.server.ThreadingHTTPServer
):

    allow_reuse_address = True
    daemon_threads = True


def start_server():

    server = TestServer(
        ("127.0.0.1", 0),
        TestHandler,
    )

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )

    thread.start()

    return server, thread


def make_url(server):

    host, port = server.server_address

    return f"http://{host}:{port}/page"


def test_worker_fetcher_integration():

    print("=" * 72)
    print("TEST 1 — CRAWLWORKER + FETCHER INTEGRATION")
    print("=" * 72)

    server, thread = start_server()
    fetcher = None

    try:

        url = make_url(server)

        fetcher = Fetcher(
            max_retries=0,
            timeout=5,
            max_connections_per_origin=4,
        )

        policy = CrawlPolicy(
            user_agent="OurSearchBot/1.0"
        )

        worker = CrawlWorker(
            fetcher,
            policy,
            worker_id=0,
        )

        task = CrawlTask(
            url=url,
            document_id="stage5_3_worker_test",
            priority=100,
            attempt=0,
        )

        result = worker.process(task)

        if result is None:
            raise RuntimeError(
                "CrawlWorker returned None"
            )

        if result.task is not task:
            raise RuntimeError(
                "CrawlResult does not reference "
                "original task"
            )

        if result.response.get("status") != 200:
            raise RuntimeError(
                "Unexpected HTTP status: "
                f"{result.response.get('status')}"
            )

        if not result.success():
            raise RuntimeError(
                "CrawlResult.success() returned False"
            )

        if worker.tasks_processed != 1:
            raise RuntimeError(
                "Unexpected tasks_processed: "
                f"{worker.tasks_processed}"
            )

        if worker.tasks_failed != 0:
            raise RuntimeError(
                "Unexpected tasks_failed: "
                f"{worker.tasks_failed}"
            )

        print(
            "PASS — CrawlTask constructed "
            "with document_id"
        )

        print(
            "PASS — CrawlWorker processed task"
        )

        print(
            "PASS — Fetcher returned HTTP 200"
        )

        print(
            "PASS — CrawlResult.success() verified"
        )

        print(
            "PASS — worker counters verified"
        )

        return True

    finally:

        if fetcher is not None:
            fetcher.close()

        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def wait_for_available_worker(
    pool,
    timeout=10,
):

    deadline = time.time() + timeout

    while time.time() < deadline:

        pool.monitor_workers()

        available = (
            pool.available_worker_ids()
        )

        if available:
            return available[0]

        time.sleep(0.02)

    return None


def test_multiprocess_worker_pool():

    print("=" * 72)
    print("TEST 2 — MULTIPROCESS WORKER POOL INTEGRATION")
    print("=" * 72)

    server, thread = start_server()

    pool = WorkerPool(
        worker_count=4
    )

    submitted = 0
    completed = 0
    successful = 0

    try:

        url = make_url(server)

        pool.start()

        deadline = time.time() + 10

        while time.time() < deadline:

            pool.monitor_workers()

            if pool.active_workers() == 4:
                break

            time.sleep(0.05)

        if pool.active_workers() != 4:
            raise RuntimeError(
                "Expected 4 active workers, got "
                f"{pool.active_workers()}"
            )

        initial_status = (
            pool.worker_status()
        )

        if len(initial_status) != 4:
            raise RuntimeError(
                "Expected 4 worker status entries, "
                f"got {len(initial_status)}"
            )

        print(
            "PASS — 4 multiprocessing workers active"
        )

        for index in range(POOL_TASKS):

            worker_id = (
                wait_for_available_worker(
                    pool,
                    timeout=10,
                )
            )

            if worker_id is None:
                raise RuntimeError(
                    "Timed out waiting for available worker"
                )

            task = CrawlTask(
                url=url,
                document_id=(
                    f"stage5_3_pool_{index}"
                ),
                priority=100,
                attempt=0,
            )

            pool.submit(
                task,
                worker_id,
            )

            submitted += 1

            result = pool.get_result(
                timeout=10
            )

            if result is None:
                raise RuntimeError(
                    "WorkerPool returned None result"
                )

            completed += 1

            if result.success():
                successful += 1

            pool.release_worker(
                worker_id
            )

        if submitted != POOL_TASKS:
            raise RuntimeError(
                f"submitted={submitted}"
            )

        if completed != POOL_TASKS:
            raise RuntimeError(
                f"completed={completed}"
            )

        if successful != POOL_TASKS:
            raise RuntimeError(
                f"successful={successful}"
            )

        if pool.active_workers() != 4:
            raise RuntimeError(
                "Active worker count changed: "
                f"{pool.active_workers()}"
            )

        final_status = (
            pool.worker_status()
        )

        alive_workers = sum(
            1
            for status in final_status.values()
            if status.get("alive")
        )

        if alive_workers != 4:
            raise RuntimeError(
                "Expected 4 alive workers, got "
                f"{alive_workers}"
            )

        print(
            f"PASS — submitted {submitted} tasks"
        )

        print(
            f"PASS — completed {completed} tasks"
        )

        print(
            f"PASS — successful {successful} tasks"
        )

        print(
            "PASS — all 4 workers remained alive"
        )

        print(
            "PASS — multiprocessing WorkerPool "
            "remained operational"
        )

        return True

    finally:

        pool.stop()

        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_failure_survival():

    print("=" * 72)
    print("TEST 3 — WORKER FAILURE SURVIVAL")
    print("=" * 72)

    server, thread = start_server()

    pool = WorkerPool(
        worker_count=2
    )

    try:

        url = make_url(server)

        pool.start()

        deadline = time.time() + 10

        while time.time() < deadline:

            pool.monitor_workers()

            if pool.active_workers() == 2:
                break

            time.sleep(0.05)

        if pool.active_workers() != 2:
            raise RuntimeError(
                "Initial worker startup failed"
            )

        workers = (
            pool.available_worker_ids()
        )

        if len(workers) != 2:
            raise RuntimeError(
                "Expected 2 available workers, got "
                f"{len(workers)}"
            )

        failed_worker = workers[0]

        process = pool.workers[
            failed_worker
        ]

        if not process.is_alive():
            raise RuntimeError(
                "Selected worker is not alive"
            )

        process.terminate()
        process.join(timeout=5)

        if process.is_alive():
            raise RuntimeError(
                "Worker did not terminate"
            )

        pool.monitor_workers()

        pool.restart_worker(
            failed_worker
        )

        deadline = time.time() + 10

        recovered = False

        while time.time() < deadline:

            pool.monitor_workers()

            status = pool.worker_status()

            worker_info = status.get(
                failed_worker
            )

            if (
                worker_info is not None
                and worker_info.get("alive")
            ):

                recovered = True
                break

            time.sleep(0.05)

        if not recovered:
            raise RuntimeError(
                "Failed worker did not recover"
            )

        deadline = time.time() + 10

        recovered_worker = None

        while time.time() < deadline:

            pool.monitor_workers()

            available = (
                pool.available_worker_ids()
            )

            if available:
                recovered_worker = (
                    available[0]
                )
                break

            time.sleep(0.05)

        if recovered_worker is None:
            raise RuntimeError(
                "No worker became available "
                "after recovery"
            )

        task = CrawlTask(
            url=url,
            document_id="stage5_3_recovery",
            priority=100,
            attempt=0,
        )

        pool.submit(
            task,
            recovered_worker,
        )

        result = pool.get_result(
            timeout=10
        )

        if result is None:
            raise RuntimeError(
                "No result after worker recovery"
            )

        if not result.success():
            raise RuntimeError(
                "Recovered worker failed task"
            )

        pool.release_worker(
            recovered_worker
        )

        if pool.active_workers() != 2:
            raise RuntimeError(
                "Worker pool did not return to "
                "2 workers"
            )

        print(
            "PASS — worker failure detected"
        )

        print(
            "PASS — failed worker restarted"
        )

        print(
            "PASS — recovered worker became available"
        )

        print(
            "PASS — recovered worker processed task"
        )

        print(
            "PASS — worker pool restored to 2 workers"
        )

        return True

    finally:

        pool.stop()

        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_performance():

    print("=" * 72)
    print("TEST 4 — CONCURRENT PERFORMANCE REGRESSION")
    print("=" * 72)

    server, thread = start_server()

    fetcher = Fetcher(
        max_retries=0,
        timeout=5,
        max_connections_per_origin=(
            PERFORMANCE_POOL_SIZE
        ),
    )

    try:

        url = make_url(server)

        def fetch_one(_):

            result = fetcher.fetch(
                url
            )

            if result.get("status") != 200:

                raise RuntimeError(
                    "Unexpected fetch result: "
                    f"{result}"
                )

            return True

        started = time.perf_counter()

        with ThreadPoolExecutor(
            max_workers=PERFORMANCE_CONCURRENCY
        ) as executor:

            results = list(
                executor.map(
                    fetch_one,
                    range(
                        PERFORMANCE_REQUESTS
                    ),
                )
            )

        elapsed = (
            time.perf_counter()
            - started
        )

        successful = sum(
            results
        )

        throughput = (
            successful / elapsed
            if elapsed > 0
            else 0.0
        )

        print(
            f"requests={PERFORMANCE_REQUESTS} "
            f"successful={successful} "
            f"concurrency={PERFORMANCE_CONCURRENCY} "
            f"pool={PERFORMANCE_POOL_SIZE} "
            f"time={elapsed:.4f}s "
            f"throughput={throughput:.2f}/s "
            f"baseline={BASELINE_THROUGHPUT:.2f}/s"
        )

        if successful != PERFORMANCE_REQUESTS:
            raise RuntimeError(
                "Performance test had failed requests"
            )

        if throughput < BASELINE_THROUGHPUT:
            raise RuntimeError(
                "Throughput regression: "
                f"{throughput:.2f}/s < "
                f"{BASELINE_THROUGHPUT:.2f}/s"
            )

        print(
            "PASS — concurrent throughput "
            "meets/exceeds baseline"
        )

        return True

    finally:

        fetcher.close()

        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_final_lifecycle():

    print("=" * 72)
    print("TEST 5 — FINAL FETCHER LIFECYCLE")
    print("=" * 72)

    server, thread = start_server()

    fetcher = Fetcher(
        max_retries=0,
        timeout=5,
        max_connections_per_origin=4,
    )

    try:

        url = make_url(server)

        initial = fetcher.fetch(
            url
        )

        if initial.get("status") != 200:
            raise RuntimeError(
                "Initial lifecycle fetch failed"
            )

        fetcher.close()

        pools_empty = (
            len(fetcher.pool_stats()) == 0
        )

        post_close = fetcher.fetch(
            url
        )

        post_close_status = (
            post_close.get("status_type")
        )

        if not pools_empty:
            raise RuntimeError(
                "Fetcher pools were not empty "
                "after close"
            )

        if post_close_status != "fetch_error":
            raise RuntimeError(
                "Post-close fetch did not return "
                "fetch_error"
            )

        print(
            f"initial_status="
            f"{initial.get('status')} "
            f"pools_empty={pools_empty} "
            f"post_close_rejected="
            f"{post_close_status}"
        )

        print(
            "PASS — final lifecycle verified"
        )

        return True

    finally:

        fetcher.close()

        server.shutdown()
        server.server_close()

        thread.join(timeout=2)


def main():

    print("=" * 72)
    print(
        "STAGE 5.3 — FINAL HIGH-THROUGHPUT "
        "FETCHING GATE"
    )
    print("=" * 72)

    results = {}

    tests = [
        (
            "worker_fetcher_integration",
            test_worker_fetcher_integration,
        ),
        (
            "multiprocess_worker_pool",
            test_multiprocess_worker_pool,
        ),
        (
            "failure_survival",
            test_failure_survival,
        ),
        (
            "performance_regression",
            test_performance,
        ),
        (
            "final_lifecycle",
            test_final_lifecycle,
        ),
    ]

    for name, test in tests:

        try:

            results[name] = bool(
                test()
            )

        except Exception as error:

            results[name] = False

            print(
                "FAIL — test exception:"
            )

            print(
                repr(error)
            )

    print("=" * 72)
    print("FINAL STAGE 5.3 RESULTS")

    for name, passed in results.items():

        print(
            f"{name}="
            f"{'PASS' if passed else 'FAIL'}"
        )

    print("=" * 72)

    if all(results.values()):

        print("RESULT: PASS")

        print(
            "STAGE 5.3 HIGH-THROUGHPUT "
            "FETCHING: 100% COMPLETE"
        )

        return 0

    print("RESULT: FAIL")

    print(
        "STAGE 5.3 HIGH-THROUGHPUT "
        "FETCHING: NOT COMPLETE"
    )

    return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
