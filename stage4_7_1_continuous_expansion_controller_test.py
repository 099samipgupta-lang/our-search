import os
import shutil
import tempfile
import time

from crawler_system.expansion_queue import ExpansionQueue
from crawler_system.continuous_expansion_controller import (
    ContinuousExpansionController,
)


TEST_ROOT = tempfile.mkdtemp(
    prefix="stage4_7_1_"
)

DB_PATH = os.path.join(
    TEST_ROOT,
    "expansion.db"
)


def check(name, condition):
    print(
        f"{name}: {'PASS' if condition else 'FAIL'}"
    )

    if not condition:
        raise AssertionError(name)


try:

    queue = ExpansionQueue(
        database_path=DB_PATH,
        lease_timeout=1.0,
    )

    processed = []

    def processor(limit=10):
        count = 0

        while count < limit:

            candidate = queue.claim_next()

            if candidate is None:
                break

            processed.append(
                candidate["hostname"]
            )

            queue.mark_complete(
                candidate["hostname"]
            )

            count += 1

        return count

    controller = ContinuousExpansionController(
        expansion_queue=queue,
        processor=processor,
        batch_size=3,
        cycle_interval=0.1,
    )

    check(
        "CONTROLLER INITIALIZED",
        controller.status()["running"] is False,
    )

    # ---------------------------------------------------------
    # Durable candidate creation
    # ---------------------------------------------------------

    for index in range(7):

        queue.enqueue(
            {
                "hostname": f"domain-{index}.example",
                "first_url": (
                    f"https://domain-{index}.example/"
                ),
                "source_hostname": "source.example",
                "discovered_at": float(index),
            },
            priority=50.0,
        )

    check(
        "CANDIDATES QUEUED",
        queue.count("queued") == 7,
    )

    # ---------------------------------------------------------
    # Bounded processing
    # ---------------------------------------------------------

    result = controller.run_cycle()

    check(
        "FIRST CYCLE PROCESSED 3",
        result["processed"] == 3,
    )

    check(
        "BOUND IS ENFORCED",
        queue.count("queued") == 4,
    )

    check(
        "COMPLETE COUNT AFTER FIRST CYCLE",
        queue.count("complete") == 3,
    )

    # ---------------------------------------------------------
    # Second cycle
    # ---------------------------------------------------------

    result = controller.run_cycle()

    check(
        "SECOND CYCLE PROCESSED 3",
        result["processed"] == 3,
    )

    check(
        "QUEUED AFTER SECOND CYCLE",
        queue.count("queued") == 1,
    )

    check(
        "COMPLETE AFTER SECOND CYCLE",
        queue.count("complete") == 6,
    )

    # ---------------------------------------------------------
    # Third cycle
    # ---------------------------------------------------------

    result = controller.run_cycle()

    check(
        "THIRD CYCLE PROCESSED 1",
        result["processed"] == 1,
    )

    check(
        "QUEUE FULLY DRAINED",
        queue.count("queued") == 0,
    )

    check(
        "ALL CANDIDATES COMPLETE",
        queue.count("complete") == 7,
    )

    check(
        "NO LOST CANDIDATES",
        len(processed) == 7,
    )

    check(
        "NO DUPLICATE PROCESSING",
        len(set(processed)) == 7,
    )

    # ---------------------------------------------------------
    # Lease recovery
    # ---------------------------------------------------------

    queue.enqueue(
        {
            "hostname": "recovery.example",
            "first_url": "https://recovery.example/",
            "source_hostname": "source.example",
            "discovered_at": time.time(),
        },
        priority=90.0,
    )

    claimed = queue.claim_next()

    check(
        "RECOVERY CANDIDATE CLAIMED",
        claimed is not None
        and claimed["hostname"] == "recovery.example",
    )

    time.sleep(1.2)

    recovery_result = controller.run_cycle()

    check(
        "EXPIRED LEASE RECOVERED",
        recovery_result["recovered"] == 1,
    )

    check(
        "RECOVERED CANDIDATE PROCESSED",
        recovery_result["processed"] == 1,
    )

    check(
        "RECOVERED CANDIDATE COMPLETE",
        queue.count("complete") == 8,
    )

    check(
        "PROCESSING EMPTY AFTER RECOVERY",
        queue.count("processing") == 0,
    )

    # ---------------------------------------------------------
    # Failure isolation
    # ---------------------------------------------------------

    failure_queue = ExpansionQueue(
        database_path=os.path.join(
            TEST_ROOT,
            "failure.db"
        ),
    )

    def failing_processor(limit=10):
        raise RuntimeError(
            "synthetic controller failure"
        )

    failing_controller = ContinuousExpansionController(
        expansion_queue=failure_queue,
        processor=failing_processor,
        batch_size=2,
    )

    failure_result = failing_controller.run_cycle()

    check(
        "PROCESSOR FAILURE ISOLATED",
        failure_result["error"]
        == "synthetic controller failure",
    )

    check(
        "FAILURE METRIC RECORDED",
        failing_controller.status()["stats"][
            "processor_errors"
        ] == 1,
    )

    # ---------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------

    lifecycle = ContinuousExpansionController(
        expansion_queue=failure_queue,
        processor=lambda limit=10: 0,
        batch_size=2,
        cycle_interval=0.1,
    )

    check(
        "START WORKS",
        lifecycle.start() is True,
    )

    check(
        "START IS IDEMPOTENT",
        lifecycle.start() is False,
    )

    check(
        "RUNNING STATE",
        lifecycle.status()["running"] is True,
    )

    check(
        "STOP WORKS",
        lifecycle.stop() is True,
    )

    check(
        "STOP IS IDEMPOTENT",
        lifecycle.stop() is False,
    )

    check(
        "STOPPED STATE",
        lifecycle.status()["running"] is False,
    )

    # ---------------------------------------------------------
    # Final metrics
    # ---------------------------------------------------------

    status = controller.status()

    check(
        "CYCLE METRICS",
        status["stats"]["cycles"] >= 4,
    )

    check(
        "PROCESSED METRICS",
        status["stats"]["processed"] == 8,
    )

    print("RESULT: PASS")

finally:
    shutil.rmtree(
        TEST_ROOT,
        ignore_errors=True
    )
