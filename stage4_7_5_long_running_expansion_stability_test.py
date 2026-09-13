import sqlite3
import tempfile
import time
import tracemalloc
from pathlib import Path

from crawler_system.whole_web_crawler import WholeWebCrawler


def check(name, condition):
    if condition:
        print(f"{name}: PASS")
    else:
        print(f"{name}: FAIL")
        raise AssertionError(name)


def enqueue_candidate(crawler, hostname, priority=50.0):
    return crawler.expansion_queue.enqueue(
        {
            "hostname": hostname,
            "first_url": f"https://{hostname}/",
            "source_hostname": "stability-source.example",
            "discovered_at": time.time(),
        },
        priority=priority,
    )


def controller_cycles(crawler):
    status = crawler.expansion_controller.status()
    return int(status["stats"]["cycles"])


def assert_sqlite_integrity(database_path):
    connection = sqlite3.connect(database_path, timeout=30)
    try:
        result = connection.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]
        return result == "ok"
    finally:
        connection.close()


def successful_add_url(
    url,
    source="unknown",
    depth=0,
    seed=False,
    source_url=None,
):
    return True


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

        crawler_original_add_url = crawler._add_url
        crawler._add_url = successful_add_url

        try:
            # =========================================================
            # 1. CONTROLLER LIFECYCLE STABILITY
            # =========================================================

            controller = crawler.expansion_controller

            controller.start()

            check(
                "CONTROLLER STARTS",
                controller.status()["running"] is True,
            )

            controller.stop()

            check(
                "CONTROLLER STOPS",
                controller.status()["running"] is False,
            )

            # =========================================================
            # 2. SUSTAINED SUCCESSFUL WORKLOAD
            # =========================================================

            workload_size = 2000

            for index in range(workload_size):
                hostname = f"stability-{index}.example"

                queued = enqueue_candidate(
                    crawler,
                    hostname,
                    priority=50.0,
                )

                if not queued:
                    raise AssertionError(
                        f"failed to enqueue {hostname}"
                    )

            check(
                "FULL STABILITY WORKLOAD QUEUED",
                crawler.expansion_queue.count("queued")
                == workload_size,
            )

            successful_cycles = 0

            while crawler.expansion_queue.count("queued") > 0:
                before_cycles = controller_cycles(crawler)

                crawler.expansion_controller.run_cycle()

                after_cycles = controller_cycles(crawler)

                check(
                    "CONTROLLER CYCLE PROGRESSED",
                    after_cycles == before_cycles + 1,
                )

                successful_cycles += 1

                if successful_cycles > workload_size:
                    raise AssertionError(
                        "successful workload did not drain"
                    )

            check(
                "SUSTAINED WORKLOAD FULLY DRAINED",
                crawler.expansion_queue.count("queued") == 0,
            )

            check(
                "NO PROCESSING LEASES AFTER DRAIN",
                crawler.expansion_queue.count("processing") == 0,
            )

            check(
                "ALL SUSTAINED WORK COMPLETED",
                crawler.expansion_queue.count("complete")
                == workload_size,
            )

            controller_after_workload = (
                crawler.expansion_controller.status()
            )

            check(
                "CONTROLLER PROCESSED FULL WORKLOAD",
                controller_after_workload["stats"]["processed"]
                == workload_size,
            )

            # =========================================================
            # 3. LONG EMPTY-CYCLE STABILITY
            # =========================================================

            empty_cycles = 1000

            cycles_before_empty = controller_cycles(crawler)

            for _ in range(empty_cycles):
                crawler.expansion_controller.run_cycle()

            cycles_after_empty = controller_cycles(crawler)

            check(
                "LONG EMPTY RUN COMPLETED",
                cycles_after_empty
                == cycles_before_empty + empty_cycles,
            )

            check(
                "EMPTY RUN DOES NOT CREATE QUEUED WORK",
                crawler.expansion_queue.count("queued") == 0,
            )

            check(
                "EMPTY RUN DOES NOT CREATE PROCESSING WORK",
                crawler.expansion_queue.count("processing") == 0,
            )

            check(
                "EMPTY RUN PRESERVES COMPLETED WORK",
                crawler.expansion_queue.count("complete")
                == workload_size,
            )

            # =========================================================
            # 4. EXPIRED LEASE RECOVERY
            # =========================================================

            lease_hostname = "lease-recovery.example"

            queued = enqueue_candidate(
                crawler,
                lease_hostname,
                priority=80.0,
            )

            check(
                "LEASE TEST CANDIDATE QUEUED",
                queued is True,
            )

            claimed = crawler.expansion_queue.claim_next()

            check(
                "LEASE TEST CANDIDATE CLAIMED",
                claimed is not None
                and claimed["hostname"] == lease_hostname,
            )

            queue_database = crawler.expansion_queue.database_path

            connection = sqlite3.connect(
                queue_database,
                timeout=30,
            )

            try:
                connection.execute(
                    """
                    UPDATE expansion_queue
                    SET processing_started_at = ?
                    WHERE hostname = ?
                    """,
                    (
                        time.time() - 120.0,
                        lease_hostname,
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            check(
                "EXPIRED LEASE PRESENT",
                crawler.expansion_queue.count("processing") >= 1,
            )

            recovered = (
                crawler.expansion_queue.recover_expired_leases()
            )

            check(
                "EXPIRED LEASE RECOVERED",
                recovered >= 1,
            )

            check(
                "RECOVERED WORK RETURNS TO QUEUE",
                crawler.expansion_queue.count("queued") >= 1,
            )

            crawler.expansion_controller.run_cycle()

            check(
                "RECOVERED WORK COMPLETES",
                crawler.expansion_queue.count("complete")
                == workload_size + 1,
            )

            check(
                "NO STUCK PROCESSING AFTER LEASE RECOVERY",
                crawler.expansion_queue.count("processing") == 0,
            )

            # =========================================================
            # 5. RESTART / DURABLE QUEUE PERSISTENCE
            # =========================================================

            restart_queued_count = 40

            for index in range(restart_queued_count):
                hostname = f"restart-{index}.example"

                queued = enqueue_candidate(
                    crawler,
                    hostname,
                    priority=60.0,
                )

                if not queued:
                    raise AssertionError(
                        f"failed to enqueue {hostname}"
                    )

            before_restart_queued = (
                crawler.expansion_queue.count("queued")
            )

            check(
                "RESTART WORKLOAD QUEUED",
                before_restart_queued == restart_queued_count,
            )

            crawler.expansion_controller.run_cycle()
            crawler.expansion_controller.run_cycle()

            before_restart_complete = (
                crawler.expansion_queue.count("complete")
            )

            before_restart_remaining = (
                crawler.expansion_queue.count("queued")
            )

            expected_complete_before_restart = (
                workload_size + 1 + 20
            )

            check(
                "PARTIAL WORK PROCESSED BEFORE RESTART",
                before_restart_complete
                == expected_complete_before_restart,
            )

            check(
                "UNPROCESSED WORK REMAINS BEFORE RESTART",
                before_restart_remaining
                == restart_queued_count - 20,
            )

            processed_before_restart = (
                crawler.expansion_controller
                .status()["stats"]["processed"]
            )

            expected_processed_before_restart = (
                workload_size + 1 + 20
            )

            check(
                "CONTROLLER SUCCESS METRIC BEFORE RESTART",
                processed_before_restart
                == expected_processed_before_restart,
            )

            crawler.stop()

            restarted = WholeWebCrawler(
                worker_count=1,
                frontier_delay=0,
                task_timeout=10,
                max_attempts=3,
                storage_root=str(storage_root),
            )

            restarted_original_add_url = restarted._add_url
            restarted._add_url = successful_add_url

            try:
                check(
                    "RESTARTED CRAWLER PRESERVES QUEUED WORK",
                    restarted.expansion_queue.count("queued")
                    == restart_queued_count - 20,
                )

                check(
                    "RESTARTED CRAWLER PRESERVES COMPLETED WORK",
                    restarted.expansion_queue.count("complete")
                    == expected_complete_before_restart,
                )

                while (
                    restarted.expansion_queue.count("queued") > 0
                ):
                    restarted.expansion_controller.run_cycle()

                check(
                    "RESTARTED WORKLOAD FULLY DRAINED",
                    restarted.expansion_queue.count("queued") == 0,
                )

                check(
                    "RESTARTED WORKLOAD COMPLETED",
                    restarted.expansion_queue.count("complete")
                    == workload_size
                    + 1
                    + restart_queued_count,
                )

                check(
                    "RESTARTED CRAWLER HAS NO PROCESSING LEASES",
                    restarted.expansion_queue.count("processing")
                    == 0,
                )

                restarted_processed = (
                    restarted.expansion_controller
                    .status()["stats"]["processed"]
                )

                check(
                    "RESTARTED CONTROLLER PROCESSED WORK",
                    restarted_processed == restart_queued_count - 20,
                )

            finally:
                restarted._add_url = restarted_original_add_url
                restarted.stop()

            # =========================================================
            # 6. REPEATED FAILURE / RETRY STABILITY
            # =========================================================

            crawler = WholeWebCrawler(
                worker_count=1,
                frontier_delay=0,
                task_timeout=10,
                max_attempts=3,
                storage_root=str(storage_root),
            )

            crawler_original_add_url = crawler._add_url

            try:
                crawler._add_url = (
                    lambda *args, **kwargs: False
                )

                failing_count = 100

                for index in range(failing_count):
                    hostname = f"failure-{index}.example"

                    queued = enqueue_candidate(
                        crawler,
                        hostname,
                        priority=40.0,
                    )

                    if not queued:
                        raise AssertionError(
                            f"failed to enqueue {hostname}"
                        )

                failure_cycles = 0

                while (
                    crawler.expansion_queue.count("queued") > 0
                    or crawler.expansion_queue.count("processing") > 0
                ):
                    crawler.expansion_controller.run_cycle()

                    failure_cycles += 1

                    check(
                        "FAILURE QUEUE REMAINS BOUNDED",
                        crawler.expansion_queue.count("queued")
                        <= failing_count,
                    )

                    check(
                        "FAILURE PROCESSING REMAINS BOUNDED",
                        crawler.expansion_queue.count("processing")
                        <= 1,
                    )

                    if failure_cycles > 100:
                        raise AssertionError(
                            "failure workload did not converge"
                        )

                check(
                    "FAILURE WORKLOAD FULLY DRAINED",
                    crawler.expansion_queue.count("queued") == 0,
                )

                check(
                    "FAILURE WORKLOAD HAS NO PROCESSING LEASES",
                    crawler.expansion_queue.count("processing") == 0,
                )

                check(
                    "FAILURES ARE ISOLATED",
                    crawler.expansion_queue.count("failed")
                    >= failing_count,
                )

                recovery_status = (
                    crawler.expansion_failure_recovery.status()
                )

                check(
                    "ALL FAILURES REACH EXHAUSTION",
                    recovery_status["exhausted_failures"]
                    >= failing_count,
                )

                # Failed attempts do NOT increment controller
                # processed because _process_expansion_queue()
                # increments processed only when a candidate is
                # successfully consumed.

                failure_controller_status = (
                    crawler.expansion_controller.status()
                )

                check(
                    "FAILED RETRIES DO NOT FAKE SUCCESS METRIC",
                    failure_controller_status["stats"]["processed"]
                    == 0,
                )

                # =====================================================
                # 7. FAILURE STATE SURVIVES RESTART
                # =====================================================

                exhausted_before_restart = (
                    recovery_status["exhausted_failures"]
                )

                crawler.stop()

                failure_restarted = WholeWebCrawler(
                    worker_count=1,
                    frontier_delay=0,
                    task_timeout=10,
                    max_attempts=3,
                    storage_root=str(storage_root),
                )

                failure_restarted_original = (
                    failure_restarted._add_url
                )

                failure_restarted._add_url = (
                    lambda *args, **kwargs: False
                )

                try:
                    restarted_recovery_status = (
                        failure_restarted
                        .expansion_failure_recovery
                        .status()
                    )

                    check(
                        "EXHAUSTED FAILURE STATE SURVIVES RESTART",
                        restarted_recovery_status[
                            "exhausted_failures"
                        ]
                        >= exhausted_before_restart,
                    )

                    check(
                        "RESTARTED RECOVERY TRACKS FAILURES",
                        restarted_recovery_status[
                            "tracked_candidates"
                        ]
                        >= failing_count,
                    )

                    check(
                        "RESTARTED EXHAUSTION COUNT REMAINS VALID",
                        restarted_recovery_status[
                            "exhausted_failures"
                        ]
                        >= failing_count,
                    )

                    # =================================================
                    # 8. SQLITE INTEGRITY / RESOURCE STABILITY
                    # =================================================

                    queue_db = (
                        failure_restarted
                        .expansion_queue
                        .database_path
                    )

                    check(
                        "QUEUE SQLITE INTEGRITY",
                        assert_sqlite_integrity(queue_db),
                    )

                    recovery_db = (
                        failure_restarted
                        .expansion_failure_recovery
                        .database_path
                    )

                    check(
                        "RECOVERY SQLITE INTEGRITY",
                        assert_sqlite_integrity(recovery_db),
                    )

                    queue_size = Path(queue_db).stat().st_size
                    recovery_size = Path(recovery_db).stat().st_size

                    check(
                        "QUEUE DATABASE EXISTS",
                        queue_size > 0,
                    )

                    check(
                        "RECOVERY DATABASE EXISTS",
                        recovery_size > 0,
                    )

                    # Repeated SQLite connection/query stress.
                    for _ in range(500):
                        check_count = (
                            failure_restarted
                            .expansion_queue
                            .count()
                        )

                        if check_count < 0:
                            raise AssertionError(
                                "invalid queue count"
                            )

                    check(
                        "REPEATED SQLITE ACCESS REMAINS HEALTHY",
                        failure_restarted
                        .expansion_queue
                        .count()
                        >= workload_size
                        + 1
                        + restart_queued_count
                        + failing_count,
                    )

                    # =================================================
                    # 9. CONTROLLED LONG EMPTY-RUN RESOURCE TEST
                    # =================================================

                    tracemalloc.start()

                    memory_cycles = 500

                    memory_cycles_before = (
                        controller_cycles(
                            failure_restarted
                        )
                    )

                    for _ in range(memory_cycles):
                        failure_restarted.expansion_controller.run_cycle()

                    memory_cycles_after = (
                        controller_cycles(
                            failure_restarted
                        )
                    )

                    current_memory, peak_memory = (
                        tracemalloc.get_traced_memory()
                    )

                    tracemalloc.stop()

                    check(
                        "RESOURCE STABILITY CYCLES COMPLETED",
                        memory_cycles_after
                        == memory_cycles_before
                        + memory_cycles,
                    )

                    check(
                        "RESOURCE ALLOCATION REMAINS BOUNDED",
                        peak_memory < 64 * 1024 * 1024,
                    )

                    print(
                        f"TRACED CURRENT MEMORY: "
                        f"{current_memory} bytes"
                    )

                    print(
                        f"TRACED PEAK MEMORY: "
                        f"{peak_memory} bytes"
                    )

                    # =================================================
                    # 10. FINAL STATUS / METRIC INTEGRITY
                    # =================================================

                    controller_final = (
                        failure_restarted
                        .expansion_controller
                        .status()
                    )

                    recovery_final = (
                        failure_restarted
                        .expansion_failure_recovery
                        .status()
                    )

                    check(
                        "CONTROLLER CYCLES REMAIN MONOTONIC",
                        controller_final["stats"]["cycles"]
                        >= memory_cycles_after,
                    )

                    # This restarted runtime has processed only
                    # empty cycles, so its processed metric must remain
                    # zero. Failed retry attempts are not counted as
                    # successful processed work.
                    check(
                        "CONTROLLER PROCESSED METRIC REMAINS VALID",
                        controller_final["stats"]["processed"]
                        == 0,
                    )

                    check(
                        "NO STUCK PROCESSING WORK",
                        failure_restarted
                        .expansion_queue
                        .count("processing")
                        == 0,
                    )

                    check(
                        "RECOVERY TRACKS FAILURE CANDIDATES",
                        recovery_final["tracked_candidates"]
                        >= failing_count,
                    )

                    check(
                        "RECOVERY EXHAUSTION COUNT IS VALID",
                        recovery_final["exhausted_failures"]
                        >= failing_count,
                    )

                    crawler_status = failure_restarted.status()

                    check(
                        "CRAWLER STATUS AVAILABLE",
                        isinstance(crawler_status, dict),
                    )

                    check(
                        "CONTROLLER STATUS EXPOSED",
                        isinstance(
                            crawler_status.get(
                                "expansion_controller"
                            ),
                            dict,
                        ),
                    )

                    check(
                        "CONTROLLER METRICS EXPOSED",
                        isinstance(
                            crawler_status[
                                "expansion_controller"
                            ].get("stats"),
                            dict,
                        ),
                    )

                    check(
                        "RECOVERY STATUS EXPOSED",
                        isinstance(
                            crawler_status.get(
                                "expansion_failure_recovery"
                            ),
                            dict,
                        ),
                    )

                    print(
                        f"TOTAL CONTROLLER CYCLES: "
                        f"{controller_final['stats']['cycles']}"
                    )

                    print(
                        f"TOTAL COMPLETED WORK: "
                        f"{failure_restarted.expansion_queue.count('complete')}"
                    )

                    print(
                        f"TOTAL FAILED WORK: "
                        f"{failure_restarted.expansion_queue.count('failed')}"
                    )

                    print(
                        f"TOTAL EXHAUSTED CANDIDATES: "
                        f"{recovery_final['exhausted_failures']}"
                    )

                    print(
                        f"QUEUE DATABASE SIZE: "
                        f"{queue_size} bytes"
                    )

                    print(
                        f"RECOVERY DATABASE SIZE: "
                        f"{recovery_size} bytes"
                    )

                    print("RESULT: PASS")

                finally:
                    failure_restarted._add_url = (
                        failure_restarted_original
                    )
                    failure_restarted.stop()

            finally:
                crawler._add_url = crawler_original_add_url
                crawler.stop()

        finally:
            crawler._add_url = crawler_original_add_url

            try:
                crawler.stop()
            except Exception:
                pass


if __name__ == "__main__":
    main()
