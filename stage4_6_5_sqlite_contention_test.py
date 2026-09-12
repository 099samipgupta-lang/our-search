import os
import tempfile
import threading
import time

from crawler_system.expansion_queue import ExpansionQueue


WORKERS = 8
PER_WORKER = 250
TOTAL = WORKERS * PER_WORKER


def enqueue_worker(queue, worker_id, results):
    success = 0

    for i in range(PER_WORKER):
        number = worker_id * PER_WORKER + i

        candidate = {
            "hostname": f"contention-{number}.example",
            "first_url": f"https://contention-{number}.example/",
            "source_hostname": "source.example",
            "discovered_at": float(number),
        }

        if queue.enqueue(candidate, float((number % 100) + 1)):
            success += 1

    results[worker_id] = success


def claim_worker(queue, results, worker_id):
    processed = 0

    while True:
        candidate = queue.claim_next()

        if candidate is None:
            break

        hostname = candidate["hostname"]

        if queue.mark_complete(hostname):
            processed += 1

    results[worker_id] = processed


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        database_path = os.path.join(
            temp_dir,
            "sqlite_contention.db",
        )

        queue = ExpansionQueue(database_path)

        enqueue_results = {}
        threads = []

        start = time.perf_counter()

        for worker_id in range(WORKERS):
            thread = threading.Thread(
                target=enqueue_worker,
                args=(queue, worker_id, enqueue_results),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        enqueue_elapsed = time.perf_counter() - start

        queued = queue.count(status="queued")

        print(f"WORKERS: {WORKERS}")
        print(f"TARGET: {TOTAL}")
        print(f"ENQUEUE_SUCCESS: {sum(enqueue_results.values())}")
        print(f"QUEUED_AFTER_INSERT: {queued}")
        print(f"ENQUEUE_SECONDS: {enqueue_elapsed:.3f}")

        assert sum(enqueue_results.values()) == TOTAL
        assert queued == TOTAL

        claim_results = {}
        threads = []

        start = time.perf_counter()

        for worker_id in range(WORKERS):
            thread = threading.Thread(
                target=claim_worker,
                args=(queue, claim_results, worker_id),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        claim_elapsed = time.perf_counter() - start

        processed = sum(claim_results.values())
        complete = queue.count(status="complete")
        queued_remaining = queue.count(status="queued")
        processing_remaining = queue.count(status="processing")

        print(f"PROCESSED: {processed}")
        print(f"COMPLETE: {complete}")
        print(f"QUEUED_REMAINING: {queued_remaining}")
        print(f"PROCESSING_REMAINING: {processing_remaining}")
        print(f"CLAIM_SECONDS: {claim_elapsed:.3f}")

        assert processed == TOTAL
        assert complete == TOTAL
        assert queued_remaining == 0
        assert processing_remaining == 0

        reopened = ExpansionQueue(database_path)

        assert reopened.count(status="complete") == TOTAL
        assert reopened.count(status="queued") == 0
        assert reopened.count(status="processing") == 0

        print("CONCURRENT SQLITE INSERTION: PASS")
        print("CONCURRENT CLAIMING: PASS")
        print("NO LOST WORK: PASS")
        print("NO DUPLICATE COMPLETION: PASS")
        print("NO QUEUED REMAINDER: PASS")
        print("NO PROCESSING REMAINDER: PASS")
        print("PERSISTENCE AFTER REOPEN: PASS")
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
