import tempfile
from pathlib import Path

from crawler_system.whole_web_crawler import WholeWebCrawler


def check(name, condition):
    if condition:
        print(f"{name}: PASS")
    else:
        print(f"{name}: FAIL")
        raise AssertionError(name)


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        storage_root = Path(temp_dir) / "crawler_storage"

        crawler = WholeWebCrawler(
            worker_count=1,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=3,
            storage_root=str(storage_root),
        )

        failed_hostname = "failure-recovery.example"
        failed_url = "https://failure-recovery.example/"

        # ---------------------------------------------------------
        # Failure path
        # ---------------------------------------------------------

        crawler.expansion_queue.enqueue(
            {
                "hostname": failed_hostname,
                "first_url": failed_url,
                "source_hostname": "source.example",
                "discovered_at": 1.0,
            },
            priority=80.0,
        )

        # Force the real expansion processor down its failure path.
        crawler._add_url = lambda *args, **kwargs: False
        crawler.url_state.get = lambda *args, **kwargs: None

        processed = crawler._process_expansion_queue(limit=1)

        state = crawler.expansion_failure_recovery.get_state(
            failed_hostname
        )

        check(
            "REAL CRAWLER FAILURE PATH",
            state is not None
            and state["attempts"] == 1,
        )

        check(
            "FIRST FAILURE REQUEUED",
            crawler.expansion_queue.count("queued") == 1,
        )

        check(
            "FIRST FAILURE NOT EXHAUSTED",
            state["status"] == "active",
        )

        # ---------------------------------------------------------
        # Second failure
        # ---------------------------------------------------------

        crawler._process_expansion_queue(limit=1)

        state = crawler.expansion_failure_recovery.get_state(
            failed_hostname
        )

        check(
            "SECOND FAILURE RECORDED",
            state is not None
            and state["attempts"] == 2,
        )

        check(
            "SECOND FAILURE REQUEUED",
            crawler.expansion_queue.count("queued") == 1,
        )

        # ---------------------------------------------------------
        # Third failure -> exhaustion
        # ---------------------------------------------------------

        crawler._process_expansion_queue(limit=1)

        state = crawler.expansion_failure_recovery.get_state(
            failed_hostname
        )

        check(
            "RETRY BUDGET EXHAUSTED",
            state is not None
            and state["attempts"] == 3
            and state["status"] == "exhausted",
        )

        check(
            "PERMANENT FAILURE ISOLATED",
            crawler.expansion_queue.count("failed") == 1
            and crawler.expansion_queue.count("queued") == 0,
        )

        check(
            "NO INFINITE RETRY",
            crawler.expansion_failure_recovery.can_retry(
                failed_hostname
            ) is False,
        )

        # ---------------------------------------------------------
        # Healthy candidate must continue working
        # ---------------------------------------------------------

        healthy_hostname = "healthy-expansion.example"
        healthy_url = "https://healthy-expansion.example/"

        crawler.expansion_queue.enqueue(
            {
                "hostname": healthy_hostname,
                "first_url": healthy_url,
                "source_hostname": "source.example",
                "discovered_at": 2.0,
            },
            priority=70.0,
        )

        crawler._add_url = lambda *args, **kwargs: True

        processed = crawler._process_expansion_queue(limit=1)

        check(
            "HEALTHY CANDIDATE PROCESSED",
            processed == 1,
        )

        check(
            "HEALTHY CANDIDATE COMPLETED",
            crawler.expansion_queue.count("complete") == 1,
        )

        check(
            "HEALTHY CANDIDATE NOT BLOCKED",
            crawler.expansion_failure_recovery.get_state(
                healthy_hostname
            ) is None,
        )

        # ---------------------------------------------------------
        # Successful candidate clears prior failure history
        # ---------------------------------------------------------

        recovery_hostname = "eventual-success.example"
        recovery_url = "https://eventual-success.example/"

        crawler.expansion_queue.enqueue(
            {
                "hostname": recovery_hostname,
                "first_url": recovery_url,
                "source_hostname": "source.example",
                "discovered_at": 3.0,
            },
            priority=60.0,
        )

        crawler._add_url = lambda *args, **kwargs: False
        crawler.url_state.get = lambda *args, **kwargs: None

        crawler._process_expansion_queue(limit=1)

        failed_state = crawler.expansion_failure_recovery.get_state(
            recovery_hostname
        )

        check(
            "EVENTUAL SUCCESS FAILURE RECORDED",
            failed_state is not None
            and failed_state["attempts"] == 1,
        )

        # The candidate has been requeued by recovery.
        crawler._add_url = lambda *args, **kwargs: True

        processed = crawler._process_expansion_queue(limit=1)

        check(
            "EVENTUAL SUCCESS PROCESSED",
            processed == 1,
        )

        check(
            "SUCCESS CLEARS FAILURE HISTORY",
            crawler.expansion_failure_recovery.get_state(
                recovery_hostname
            ) is None,
        )

        # ---------------------------------------------------------
        # Runtime status exposes recovery
        # ---------------------------------------------------------

        status = crawler.status()

        check(
            "RECOVERY STATUS EXPOSED",
            "expansion_failure_recovery" in status,
        )

        # Capture durable failure state before restart.
        exhausted_before_restart = (
            crawler.expansion_failure_recovery.get_state(
                failed_hostname
            )
        )

        check(
            "EXHAUSTED STATE BEFORE RESTART",
            exhausted_before_restart is not None
            and exhausted_before_restart["attempts"] == 3
            and exhausted_before_restart["status"] == "exhausted",
        )

        crawler.stop()

        # ---------------------------------------------------------
        # Restart crawler using the same durable storage
        # ---------------------------------------------------------

        restarted = WholeWebCrawler(
            worker_count=1,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=3,
            storage_root=str(storage_root),
        )

        exhausted_after_restart = (
            restarted.expansion_failure_recovery.get_state(
                failed_hostname
            )
        )

        check(
            "EXHAUSTED STATE SURVIVES RESTART",
            exhausted_after_restart is not None
            and exhausted_after_restart["attempts"] == 3
            and exhausted_after_restart["status"] == "exhausted",
        )

        check(
            "RESTARTED CRAWLER CANNOT RETRY EXHAUSTED CANDIDATE",
            restarted.expansion_failure_recovery.can_retry(
                failed_hostname
            ) is False,
        )

        restarted.stop()

    print("RESULT: PASS")


if __name__ == "__main__":
    main()
