import os
import tempfile
import time

from crawler_system.expansion_queue import ExpansionQueue


COUNT = 2000
LEASE_TIMEOUT = 60.0


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        database_path = os.path.join(
            temp_dir,
            "failure_recovery.db",
        )

        queue = ExpansionQueue(
            database_path,
            lease_timeout=LEASE_TIMEOUT,
        )

        for i in range(COUNT):
            candidate = {
                "hostname": f"recovery-{i}.example",
                "first_url": f"https://recovery-{i}.example/",
                "source_hostname": "source.example",
                "discovered_at": float(i),
            }

            assert queue.enqueue(
                candidate,
                float((i % 100) + 1),
            )

        assert queue.count(status="queued") == COUNT

        failed = []

        for _ in range(100):
            candidate = queue.claim_next()

            assert candidate is not None

            hostname = candidate["hostname"]

            assert queue.mark_failed(hostname)

            failed.append(hostname)

        print(f"INITIAL_QUEUED: {COUNT}")
        print(f"FAILED_CANDIDATES: {len(failed)}")
        print(
            f"QUEUED_AFTER_FAILURES: "
            f"{queue.count(status='queued')}"
        )

        assert len(failed) == 100
        assert queue.count(status="queued") == COUNT - 100

        retry_candidates = []

        for hostname in failed:
            candidate = {
                "hostname": hostname,
                "first_url": f"https://{hostname}/",
                "source_hostname": "retry.example",
                "discovered_at": time.time(),
            }

            if queue.enqueue(candidate, 90.0):
                retry_candidates.append(hostname)

        print(f"REQUEUED_FAILED: {len(retry_candidates)}")

        assert len(retry_candidates) == 100
        assert queue.count(status="queued") == COUNT

        leased = queue.claim_next()

        assert leased is not None

        leased_hostname = leased["hostname"]
        started_at = leased["processing_started_at"]

        print(
            f"LEASED_FOR_CRASH_SIMULATION: "
            f"{leased_hostname}"
        )

        assert leased["status"] == "processing"
        assert isinstance(started_at, (int, float))

        simulated_now = float(started_at) + LEASE_TIMEOUT + 1.0

        recovered = queue.recover_expired_leases(
            now=simulated_now
        )

        print(f"EXPIRED_LEASES_RECOVERED: {recovered}")

        assert recovered >= 1

        recovered_candidate = queue.claim_next()

        assert recovered_candidate is not None

        assert recovered_candidate["hostname"] == leased_hostname
        assert recovered_candidate["status"] == "processing"

        assert queue.mark_complete(
            recovered_candidate["hostname"]
        )

        completed = 1

        while True:
            candidate = queue.claim_next()

            if candidate is None:
                break

            assert queue.mark_complete(
                candidate["hostname"]
            )

            completed += 1

        print(f"COMPLETED_AFTER_RECOVERY: {completed}")

        assert completed == COUNT
        assert queue.count(status="queued") == 0
        assert queue.count(status="processing") == 0
        assert queue.count(status="complete") == COUNT

        reopened = ExpansionQueue(database_path)

        assert reopened.count(status="complete") == COUNT
        assert reopened.count(status="queued") == 0
        assert reopened.count(status="processing") == 0

        print("FAILURE HANDLING: PASS")
        print("FAILED WORK REQUEUE: PASS")
        print("LEASE EXPIRY RECOVERY: PASS")
        print("NO LOST WORK: PASS")
        print("FINAL COMPLETION: PASS")
        print("PERSISTENCE AFTER RECOVERY: PASS")
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
