import os
import shutil
import tempfile
import time

from crawler_system.frontier import CrawlFrontier
from crawler_system.url_state import URLStateStore


DATASETS = (1_000, 5_000, 10_000)


def run_case(count):
    workspace = tempfile.mkdtemp(
        prefix=f"our_search_stage5_2_{count}_"
    )
    db_path = os.path.join(workspace, "state.sqlite3")

    try:
        store = URLStateStore(db_path)

        frontier = CrawlFrontier(
            default_delay=0,
            max_retries=0,
            storage_path=db_path,
            state_store=store,
        )

        start = time.perf_counter()

        inserted = 0

        for i in range(count):
            url = f"https://host-{i % 10000}.example.test/page/{i}"

            if frontier.add(
                url,
                priority=(i % 100) + 1,
                available_at=0,
            ):
                inserted += 1

        insert_elapsed = time.perf_counter() - start

        start = time.perf_counter()

        claimed = 0
        completed = 0

        while True:
            url = frontier.get_next()

            if url is None:
                break

            claimed += 1
            frontier.complete(url)
            completed += 1

        process_elapsed = time.perf_counter() - start

        counts = store.counts()

        total_elapsed = insert_elapsed + process_elapsed

        insert_rate = (
            inserted / insert_elapsed
            if insert_elapsed
            else 0
        )

        process_rate = (
            completed / process_elapsed
            if process_elapsed
            else 0
        )

        total_rate = (
            completed / total_elapsed
            if total_elapsed
            else 0
        )

        assert inserted == count
        assert claimed == count
        assert completed == count
        assert counts.get("crawled", 0) == count

        print(
            f"{count:>7,} URLs | "
            f"insert {insert_rate:>9.2f}/s | "
            f"claim+complete {process_rate:>9.2f}/s | "
            f"total {total_rate:>9.2f}/s | "
            f"DB {os.path.getsize(db_path):>10,} bytes | PASS"
        )

        store.close()

    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def main():
    print("=" * 72)
    print("STAGE 5.2 — MASSIVE FRONTIER CAPACITY BASELINE")
    print("=" * 72)
    print()
    print(
        "This benchmark measures the current frontier before "
        "web-scale redesign."
    )
    print()

    for count in DATASETS:
        run_case(count)

    print()
    print("=" * 72)
    print("RESULT: PASS")
    print("BASELINE ESTABLISHED — PROCEED TO FRONTIER ARCHITECTURE")
    print("=" * 72)


if __name__ == "__main__":
    main()
