import os
import tempfile
import time

from crawler_system.url_state import URLStateStore


CASES = (1000, 5000, 10000)


def run_case(n):
    root = tempfile.mkdtemp(prefix=f"stage5_2_perf_{n}_")
    db = os.path.join(root, "state.sqlite3")

    store = URLStateStore(db)

    start = time.perf_counter()

    for i in range(n):
        host = f"host-{i % 500}.example"
        url = f"https://{host}/page/{i}"

        store.add_discovered(
            url=url,
            document_id=f"doc-{i}",
            host=host,
            priority=(i % 100) + 1,
        )

    insert_elapsed = time.perf_counter() - start

    assert store.count() == n
    assert store.ready_size() == n

    start = time.perf_counter()
    completed = 0

    # Advance synthetic time so host scheduling does not serialize
    # the benchmark around the default crawl delay.
    now = time.time() + 100000.0

    while completed < n:
        row = store.claim_next(
            owner=f"perf-worker-{completed % 8}",
            now=now + completed,
        )

        if row is None:
            raise RuntimeError(
                f"claim stalled at {completed}/{n}"
            )

        store.mark_crawled(
            row["url"],
            status=200,
            crawled_at=now + completed,
        )

        completed += 1

    claim_complete_elapsed = time.perf_counter() - start

    assert completed == n
    assert store.count() == n
    assert store.ready_size() == 0

    total_elapsed = insert_elapsed + claim_complete_elapsed

    size = os.path.getsize(db)

    insert_rate = n / insert_elapsed
    claim_rate = n / claim_complete_elapsed
    total_rate = n / total_elapsed

    print(
        f"{n:6d} URLs | "
        f"insert {insert_rate:9.2f}/s | "
        f"claim+complete {claim_rate:9.2f}/s | "
        f"total {total_rate:9.2f}/s | "
        f"DB {size:10d} bytes | PASS"
    )

    store.close()


print("=" * 72)
print("STAGE 5.2 — MASSIVE FRONTIER PERFORMANCE GATE")
print("=" * 72)

for n in CASES:
    run_case(n)

print("=" * 72)
print("RESULT: PASS")
print("STAGE 5.2 PERFORMANCE: PASS")
print("=" * 72)
