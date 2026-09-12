import os
import tempfile
import threading
import time

from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_discovery_sources import DomainCandidate


WORKERS = 8
PER_WORKER = 250
TOTAL = WORKERS * PER_WORKER


def make_candidate(worker_id, index):
    number = worker_id * PER_WORKER + index
    return DomainCandidate(
        hostname=f"concurrent-{number}.example",
        url=f"https://concurrent-{number}.example/",
        source="certificate_transparency",
        evidence=f"worker-{worker_id}",
        discovered_at=float(number),
        metadata={
            "batch": "4.6.3",
            "worker": worker_id,
            "index": index,
        },
    )


def worker(store, worker_id, results):
    inserted = 0

    for index in range(PER_WORKER):
        if store.add(make_candidate(worker_id, index)):
            inserted += 1

    results[worker_id] = inserted


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        database_path = os.path.join(
            temp_dir,
            "concurrent_discovery.db",
        )

        store = DomainCandidateStore(database_path)

        results = {}
        threads = []

        start = time.perf_counter()

        for worker_id in range(WORKERS):
            thread = threading.Thread(
                target=worker,
                args=(store, worker_id, results),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        elapsed = time.perf_counter() - start

        inserted = sum(results.values())
        count = store.count()

        candidates = store.list_candidates(
            status="discovered",
            limit=TOTAL,
        )

        print(f"WORKERS: {WORKERS}")
        print(f"TOTAL_TARGET: {TOTAL}")
        print(f"INSERTED: {inserted}")
        print(f"COUNT: {count}")
        print(f"LISTED: {len(candidates)}")
        print(f"ELAPSED_SECONDS: {elapsed:.3f}")

        assert inserted == TOTAL
        assert count == TOTAL
        assert len(candidates) == TOTAL

        hostnames = {
            candidate["hostname"]
            for candidate in candidates
        }

        assert len(hostnames) == TOTAL

        print("CONCURRENT INSERTION: PASS")
        print("NO DATA LOSS: PASS")
        print("NO DUPLICATES: PASS")

        reopened = DomainCandidateStore(database_path)

        assert reopened.count() == TOTAL

        print("PERSISTENCE AFTER REOPEN: PASS")
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
