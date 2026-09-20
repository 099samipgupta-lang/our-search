"""
OUR SEARCH - Distributed Crawl Worker Runtime

Production distributed crawl execution layer.

Architecture:

    Durable URL Universe
            |
            v
      Atomic URL Lease
            |
            v
    Distributed Runtime
            |
            v
     Persistent WorkerPool
            |
            v
       CrawlWorker
            |
            v
        CrawlResult
            |
            v
    WholeWebCrawler pipeline
       /       |        \
 discovery   indexing   state
            |
            v
       next leased URL

The durable URL-state database is the distributed allocator.
WorkerPool provides persistent isolated crawl processes.
"""

import os
import signal
import socket
import time
import traceback
from typing import Optional

from crawler_system.task import CrawlTask
from crawler_system.worker_pool import WorkerPool
from crawler_system.whole_web_crawler import WholeWebCrawler


SCALE_TARGET = (
    "billions_to_trillions_of_public_web_resources"
)

GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = (
    "distributed-crawl-worker-runtime.v4"
)


class DistributedCrawlWorkerRuntime:

    def __init__(
        self,
        worker_id: Optional[str] = None,
        worker_count: int = 8,
        task_timeout: int = 30,
        max_attempts: int = 2,
        report_interval: float = 10.0,
        idle_sleep: float = 0.25,
    ):

        self.worker_id = (
            str(worker_id)
            if worker_id
            else f"{socket.gethostname()}-{os.getpid()}"
        )

        self.worker_count = max(
            1,
            int(worker_count)
        )

        self.task_timeout = max(
            1,
            int(task_timeout)
        )

        self.max_attempts = max(
            1,
            int(max_attempts)
        )

        self.report_interval = max(
            1.0,
            float(report_interval)
        )

        self.idle_sleep = max(
            0.01,
            float(idle_sleep)
        )

        self.stop_requested = False

        self.started_at = time.time()
        self.last_report = self.started_at

        self.claimed = 0
        self.processed = 0
        self.failed = 0
        self.timeouts = 0
        self.worker_restarts = 0

        self.in_flight = {}

        self.crawler = WholeWebCrawler(
            worker_count=1,
            frontier_delay=0,
            task_timeout=self.task_timeout,
            max_attempts=self.max_attempts,
        )

        self.crawler.running = True

        self.url_state = getattr(
            self.crawler,
            "url_state",
            None
        )

        if self.url_state is None:
            raise RuntimeError(
                "WholeWebCrawler does not expose url_state"
            )

        self.pool = WorkerPool(
            worker_count=self.worker_count
        )

    def request_stop(
        self,
        signum=None,
        frame=None,
    ):

        self.stop_requested = True

    def claim(self):

        return self.url_state.claim_next(
            owner=self.worker_id,
            now=time.time(),
        )

    def build_task(self, lease):

        return CrawlTask(
            url=lease["url"],
            document_id=lease.get(
                "document_id"
            ),
            etag=lease.get("etag"),
            last_modified=lease.get(
                "last_modified"
            ),
        )

    def start_pool(self):

        self.pool.start()

    def dispatch(self):

        dispatched = 0

        available = (
            self.pool.available_worker_ids()
        )

        for pool_worker_id in available:

            lease = self.claim()

            if lease is None:
                break

            task = self.build_task(
                lease
            )

            self.pool.submit(
                task,
                pool_worker_id
            )

            self.in_flight[
                pool_worker_id
            ] = {
                "lease": lease,
                "task": task,
                "submitted_at": time.time(),
            }

            self.claimed += 1
            dispatched += 1

            print(
                "LEASE_CLAIMED:",
                f"worker={self.worker_id}",
                f"pool_worker={pool_worker_id}",
                f"url={lease['url']}",
                flush=True,
            )

        return dispatched

    def process_result(
        self,
        pool_worker_id,
        result,
    ):

        record = self.in_flight.pop(
            pool_worker_id,
            None
        )

        self.pool.release_worker(
            pool_worker_id
        )

        if record is None:

            print(
                "ORPHAN_RESULT:",
                f"pool_worker={pool_worker_id}",
                flush=True,
            )

            return

        lease = record["lease"]

        try:

            response = getattr(
                result,
                "response",
                None
            )

            if response is None:
                response = {}

            status = response.get(
                "status"
            )

            if (
                status is not None
                and int(status) >= 400
            ):

                self.url_state.mark_failed(
                    url=lease["url"],
                    error=f"HTTP {status}",
                    status=int(status),
                    retry_at=(
                        time.time()
                        if int(
                            lease.get(
                                "attempts",
                                0
                            )
                        ) + 1
                        < self.max_attempts
                        else None
                    ),
                    backoff_base=30.0,
                    backoff_max=3600.0,
                    retryable=True,
                )

                self.failed += 1

                return

            # The existing WholeWebCrawler pipeline performs:
            #
            #   change tracking
            #   content deduplication
            #   indexing
            #   URL discovery
            #   durable URL-state completion
            #
            # Therefore we deliberately do NOT call
            # mark_crawled() again here.

            self.crawler._process_result(
                result
            )

            self.processed += 1

        except Exception as error:

            print(
                "RESULT_PROCESSING_ERROR:",
                f"url={lease['url']}",
                f"error={error}",
                flush=True,
            )

            try:

                self.url_state.mark_failed(
                    url=lease["url"],
                    error=str(error),
                    status=None,
                    retry_at=(
                        time.time()
                        if int(
                            lease.get(
                                "attempts",
                                0
                            )
                        ) + 1
                        < self.max_attempts
                        else None
                    ),
                    backoff_base=30.0,
                    backoff_max=3600.0,
                    retryable=True,
                )

            except Exception:

                traceback.print_exc()

            self.failed += 1

    def collect_results(self):

        collected = 0

        while True:

            result = self.pool.get_result(
                timeout=0
            )

            if result is None:
                break

            task = getattr(
                result,
                "task",
                None
            )

            pool_worker_id = None

            if task is not None:

                for (
                    candidate_worker_id,
                    record
                ) in list(
                    self.in_flight.items()
                ):

                    if record["task"] is task:

                        pool_worker_id = (
                            candidate_worker_id
                        )

                        break

                    record_task = record[
                        "task"
                    ]

                    if (
                        getattr(
                            record_task,
                            "url",
                            None
                        )
                        == getattr(
                            task,
                            "url",
                            None
                        )
                        and getattr(
                            record_task,
                            "document_id",
                            None
                        )
                        == getattr(
                            task,
                            "document_id",
                            None
                        )
                    ):

                        pool_worker_id = (
                            candidate_worker_id
                        )

                        break

            if pool_worker_id is None:

                # Fallback: if exactly one worker is
                # currently waiting for a result, associate it.

                busy = list(
                    self.in_flight.keys()
                )

                if len(busy) == 1:
                    pool_worker_id = busy[0]

            if pool_worker_id is None:

                print(
                    "RESULT_MAPPING_ERROR:",
                    flush=True,
                )

                continue

            self.process_result(
                pool_worker_id,
                result
            )

            collected += 1

        return collected

    def recover_dead_workers(self):

        dead_workers = (
            self.pool.monitor_workers()
        )

        if not dead_workers:
            return

        for pool_worker_id in dead_workers:

            record = self.in_flight.pop(
                pool_worker_id,
                None
            )

            if record is not None:

                lease = record[
                    "lease"
                ]

                try:

                    self.url_state.mark_failed(
                        url=lease["url"],
                        error=(
                            "crawl worker "
                            "process died"
                        ),
                        status=None,
                        retry_at=time.time(),
                        backoff_base=30.0,
                        backoff_max=3600.0,
                        retryable=True,
                    )

                except Exception:

                    try:

                        self.url_state.release_lease(
                            url=lease["url"],
                            retry_at=time.time(),
                            increment_attempts=True,
                        )

                    except Exception:

                        traceback.print_exc()

                self.failed += 1

            self.pool.restart_worker(
                pool_worker_id
            )

            self.worker_restarts += 1

            print(
                "WORKER_RESTARTED:",
                f"pool_worker={pool_worker_id}",
                flush=True,
            )

    def recover_timeouts(self):

        now = time.time()

        for pool_worker_id, record in list(
            self.in_flight.items()
        ):

            age = (
                now
                - record["submitted_at"]
            )

            if age < self.task_timeout:
                continue

            lease = record[
                "lease"
            ]

            print(
                "CRAWL_TIMEOUT:",
                f"pool_worker={pool_worker_id}",
                f"url={lease['url']}",
                f"timeout={self.task_timeout}",
                flush=True,
            )

            self.timeouts += 1

            try:

                self.url_state.mark_failed(
                    url=lease["url"],
                    error=(
                        "crawl task timeout"
                    ),
                    status=None,
                    retry_at=time.time(),
                    backoff_base=30.0,
                    backoff_max=3600.0,
                    retryable=True,
                )

            except Exception:

                try:

                    self.url_state.release_lease(
                        url=lease["url"],
                        retry_at=time.time(),
                        increment_attempts=True,
                    )

                except Exception:

                    traceback.print_exc()

            self.failed += 1

            self.in_flight.pop(
                pool_worker_id,
                None
            )

            self.pool.restart_worker(
                pool_worker_id
            )

            self.worker_restarts += 1

    def run_discovery_cycle(self):

        global_discovery = getattr(
            self.crawler,
            "global_web_discovery",
            None
        )

        if global_discovery is not None:

            try:
                global_discovery.cycle()

            except Exception:

                self.crawler.stats[
                    "global_discovery_errors"
                ] = (
                    self.crawler.stats.get(
                        "global_discovery_errors",
                        0
                    )
                    + 1
                )

        expansion_controller = getattr(
            self.crawler,
            "expansion_controller",
            None
        )

        if expansion_controller is not None:

            try:
                expansion_controller.run_cycle()

            except Exception:
                pass

    def persist(self):

        try:

            integration = getattr(
                self.crawler,
                "index_integration",
                None
            )

            if integration is not None:
                integration.flush()

        except Exception:

            traceback.print_exc()

        try:

            state_storage = getattr(
                self.crawler,
                "state_storage",
                None
            )

            if state_storage is not None:

                state_storage.save(
                    self.crawler.url_dedup,
                    self.crawler.content_dedup,
                    self.crawler.change_tracker,
                )

        except Exception:

            traceback.print_exc()

    def report(self):

        crawler_status = (
            self.crawler.status()
        )

        stats = crawler_status.get(
            "stats",
            {}
        )

        indexing = crawler_status.get(
            "indexing",
            {}
        )

        bridge = indexing.get(
            "bridge",
            {}
        )

        print(
            "WORKER_PROGRESS:",
            f"worker={self.worker_id}",
            f"claimed={self.claimed}",
            f"processed={self.processed}",
            f"failed={self.failed}",
            f"timeouts={self.timeouts}",
            f"restarts={self.worker_restarts}",
            f"in_flight={len(self.in_flight)}",
            f"discovered={stats.get('discovered', 0)}",
            f"accepted={stats.get('accepted_urls', 0)}",
            f"frontier={crawler_status.get('frontier', 0)}",
            f"indexed={bridge.get('indexed_documents', 0)}",
            flush=True,
        )

    def run(self):

        print(
            "DISTRIBUTED CRAWL RUNTIME STARTED",
            flush=True,
        )

        print(
            f"WORKER_ID: {self.worker_id}",
            flush=True,
        )

        print(
            f"ARCHITECTURE_VERSION: "
            f"{ARCHITECTURE_VERSION}",
            flush=True,
        )

        print(
            f"WORKER_COUNT: "
            f"{self.worker_count}",
            flush=True,
        )

        print(
            f"SCALE_TARGET: "
            f"{SCALE_TARGET}",
            flush=True,
        )

        print(
            f"TASK_TIMEOUT: "
            f"{self.task_timeout}",
            flush=True,
        )

        self.start_pool()

        try:

            while not self.stop_requested:

                self.recover_dead_workers()

                self.recover_timeouts()

                self.url_state.recover_expired_leases(
                    lease_timeout=self.task_timeout
                )

                self.collect_results()

                self.dispatch()

                now = time.time()

                if (
                    now - self.last_report
                    >= self.report_interval
                ):

                    self.report()

                    self.last_report = now

                if not self.in_flight:

                    self.run_discovery_cycle()

                    dispatched = self.dispatch()

                    if dispatched == 0:

                        time.sleep(
                            min(
                               self.idle_sleep,
                               1.0,
                            )
                        )

        except KeyboardInterrupt:

            self.stop_requested = True

        except Exception:

            traceback.print_exc()

            self.stop_requested = True

        finally:

            print(
                "DISTRIBUTED CRAWL RUNTIME STOPPING",
                flush=True,
            )

            self.persist()

            try:
                self.pool.stop()
            except Exception:
                traceback.print_exc()

            self.crawler.running = False

            print(
                f"FINAL_CLAIMED: "
                f"{self.claimed}",
                flush=True,
            )

            print(
                f"FINAL_PROCESSED: "
                f"{self.processed}",
                flush=True,
            )

            print(
                f"FINAL_FAILED: "
                f"{self.failed}",
                flush=True,
            )

            print(
                f"FINAL_TIMEOUTS: "
                f"{self.timeouts}",
                flush=True,
            )

            print(
                f"FINAL_WORKER_RESTARTS: "
                f"{self.worker_restarts}",
                flush=True,
            )


def main():

    worker_id = os.environ.get(
        "OUR_SEARCH_WORKER_ID"
    )

    worker_count = int(
        os.environ.get(
            "OUR_SEARCH_WORKER_COUNT",
            "8"
        )
    )

    task_timeout = int(
        os.environ.get(
            "OUR_SEARCH_TASK_TIMEOUT",
            "30"
        )
    )

    max_attempts = int(
        os.environ.get(
            "OUR_SEARCH_MAX_ATTEMPTS",
            "2"
        )
    )

    report_interval = float(
        os.environ.get(
            "OUR_SEARCH_REPORT_INTERVAL",
            "10"
        )
    )

    runtime = (
        DistributedCrawlWorkerRuntime(
            worker_id=worker_id,
            worker_count=worker_count,
            task_timeout=task_timeout,
            max_attempts=max_attempts,
            report_interval=report_interval,
        )
    )

    signal.signal(
        signal.SIGINT,
        runtime.request_stop
    )

    signal.signal(
        signal.SIGTERM,
        runtime.request_stop
    )

    runtime.run()


if __name__ == "__main__":

    main()
