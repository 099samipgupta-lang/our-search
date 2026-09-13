import os
import tempfile
import time

from crawler_system.expansion_queue import ExpansionQueue
from crawler_system.continuous_expansion_failure_recovery import (
    ContinuousExpansionFailureRecovery,
)


def candidate(hostname, priority=50.0):
    return {
        "hostname": hostname,
        "first_url": f"https://{hostname}/",
        "source_hostname": "source.example",
        "priority": priority,
        "discovered_at": time.time(),
    }


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        queue_db = os.path.join(temp_dir, "queue.sqlite3")
        recovery_db = os.path.join(temp_dir, "recovery.sqlite3")

        queue = ExpansionQueue(
            database_path=queue_db,
            lease_timeout=1.0,
        )

        recovery = ContinuousExpansionFailureRecovery(
            expansion_queue=queue,
            database_path=recovery_db,
            max_attempts=3,
        )

        assert queue.enqueue(
            candidate("recover.example"),
            priority=50.0,
        )

        claimed = queue.claim_next()
        assert claimed is not None
        assert claimed["status"] == "processing"
        print("PROCESSING LEASE CREATED: PASS")

        expired_now = (
            float(claimed["processing_started_at"]) + 2.0
        )

        recovered = recovery.recover_leases(
            now=expired_now,
        )

        assert recovered == 1

        recovered_candidate = queue.next()
        assert recovered_candidate is not None
        assert recovered_candidate["status"] == "queued"

        print("EXPIRED LEASE RECOVERY: PASS")
        print("RECOVERED WORK RETURNS TO QUEUE: PASS")

        first = recovery.record_failure(
            "recover.example",
            now=100.0,
        )

        assert first["recorded"] is True
        assert first["retry"] is True
        assert first["exhausted"] is False
        assert first["attempts"] == 1
        assert recovery.can_retry("recover.example", now=100.0)
        print("FIRST FAILURE RETRY: PASS")

        second = recovery.record_failure(
            "recover.example",
            now=101.0,
        )

        assert second["retry"] is True
        assert second["exhausted"] is False
        assert second["attempts"] == 2
        print("SECOND FAILURE RETRY: PASS")

        third = recovery.record_failure(
            "recover.example",
            now=102.0,
        )

        assert third["retry"] is False
        assert third["exhausted"] is True
        assert third["attempts"] == 3
        assert recovery.can_retry("recover.example", now=102.0) is False
        print("RETRY BUDGET EXHAUSTION: PASS")
        print("PERMANENT FAILURE ISOLATION: PASS")

        # Runtime metrics belong to the current recovery process.
        # Verify them before restart.
        pre_restart_status = recovery.status()

        assert pre_restart_status["max_attempts"] == 3
        assert pre_restart_status["tracked_candidates"] == 1
        assert pre_restart_status["exhausted_failures"] == 1
        assert pre_restart_status["stats"]["retry_exhausted"] == 1
        assert pre_restart_status["stats"]["lease_recoveries"] == 1

        print("RECOVERY METRICS: PASS")
        print("EXHAUSTED FAILURE METRICS: PASS")
        print("LEASE RECOVERY METRICS: PASS")

        recovery.close()

        restarted = ContinuousExpansionFailureRecovery(
            expansion_queue=queue,
            database_path=recovery_db,
            max_attempts=3,
        )

        # Durable failure state must survive process restart.
        state = restarted.get_state("recover.example")

        assert state is not None
        assert state["attempts"] == 3
        assert state["status"] == "exhausted"
        assert restarted.can_retry(
            "recover.example",
            now=200.0,
        ) is False

        print("FAILURE STATE RESTART PERSISTENCE: PASS")
        print("EXHAUSTED STATE SURVIVES RESTART: PASS")

        assert queue.enqueue(
            candidate("healthy.example"),
            priority=75.0,
        )

        healthy = queue.next()
        assert healthy is not None
        assert healthy["hostname"] == "healthy.example"
        print("HEALTHY CANDIDATE REMAINS AVAILABLE: PASS")

        cycle = restarted.process_cycle(now=200.0)

        assert cycle["error"] is None
        assert cycle["recovered"] == 0

        print("RECOVERY CYCLE CONTINUES AFTER FAILURE: PASS")

        # Restarted runtime metrics are expected to start fresh.
        restarted_status = restarted.status()

        assert restarted_status["max_attempts"] == 3
        assert restarted_status["tracked_candidates"] == 1
        assert restarted_status["exhausted_failures"] == 1
        assert restarted_status["stats"]["retry_exhausted"] == 0
        assert restarted_status["stats"]["lease_recoveries"] == 0

        print("RESTARTED RUNTIME METRICS RESET: PASS")
        print("DURABLE EXHAUSTED STATE REMAINS: PASS")
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
