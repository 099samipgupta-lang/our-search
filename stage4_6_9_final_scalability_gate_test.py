import os
import tempfile
import threading

from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.expansion_queue import ExpansionQueue


WORKERS = 8
PER_WORKER = 250
TOTAL = WORKERS * PER_WORKER


def make_candidate(i):
    return DomainCandidate(
        hostname=f"final-scale-{i}.example",
        url=f"https://final-scale-{i}.example/",
        source="certificate_transparency",
        evidence=f"final-scale-{i}",
        discovered_at=float(i),
        metadata={"batch": "4.6.9", "id": i},
    )


def insert_worker(store, worker_id, results):
    inserted = 0

    start = worker_id * PER_WORKER
    end = start + PER_WORKER

    for i in range(start, end):
        if store.add(make_candidate(i)):
            inserted += 1

    results[worker_id] = inserted


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        domain_db = os.path.join(temp_dir, "final_domains.db")
        queue_db = os.path.join(temp_dir, "final_queue.db")

        store = DomainCandidateStore(domain_db)

        results = {}
        threads = []

        for worker_id in range(WORKERS):
            thread = threading.Thread(
                target=insert_worker,
                args=(store, worker_id, results),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        inserted = sum(results.values())

        assert inserted == TOTAL
        assert store.count() == TOTAL

        queue = ExpansionQueue(queue_db)

        queued = 0

        for i in range(TOTAL):
            candidate = {
                "hostname": f"final-scale-{i}.example",
                "first_url": f"https://final-scale-{i}.example/",
                "source_hostname": "source.example",
                "discovered_at": float(i),
            }

            if queue.enqueue(candidate, float((i % 100) + 1)):
                queued += 1

        assert queued == TOTAL
        assert queue.count(status="queued") == TOTAL

        claim_results = {}
        threads = []

        def claim_worker(worker_id):
            completed = 0

            while True:
                candidate = queue.claim_next()

                if candidate is None:
                    break

                if queue.mark_complete(candidate["hostname"]):
                    completed += 1

            claim_results[worker_id] = completed

        for worker_id in range(WORKERS):
            thread = threading.Thread(
                target=claim_worker,
                args=(worker_id,),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        completed = sum(claim_results.values())

        assert completed == TOTAL
        assert queue.count(status="complete") == TOTAL
        assert queue.count(status="queued") == 0
        assert queue.count(status="processing") == 0

        reopened_store = DomainCandidateStore(domain_db)
        reopened_queue = ExpansionQueue(queue_db)

        assert reopened_store.count() == TOTAL
        assert reopened_queue.count(status="complete") == TOTAL

        print(f"WORKERS: {WORKERS}")
        print(f"TOTAL_CANDIDATES: {TOTAL}")
        print(f"CONCURRENT_INSERTED: {inserted}")
        print(f"QUEUE_INSERTED: {queued}")
        print(f"QUEUE_COUNT: {reopened_queue.count(status='complete')}")

        print("CANDIDATE VOLUME: PASS")
        print("CONCURRENT DISCOVERY: PASS")
        print("QUEUE SCALABILITY: PASS")
        print("QUEUE COMPLETION: PASS")
        print("NO LOST WORK: PASS")
        print("NO DUPLICATES: PASS")
        print("RESTART PERSISTENCE: PASS")
        print("FINAL SCALABILITY GATE: PASS")
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
