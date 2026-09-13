import multiprocessing as mp
import os
import shutil
import tempfile
import time
from urllib.parse import urlsplit

from crawler_system.partition_router import PartitionRouter
from crawler_system.url_state import URLStateStore
from crawler_system.frontier import CrawlFrontier
from crawler_system.document_identity import DocumentIdentity


PARTITIONS = 8
WORKERS = 8
TOTAL_URLS = 5000


def build_urls():
    urls = []

    for i in range(TOTAL_URLS):
        host_id = i % 400
        path_id = i // 400
        urls.append(
            f"https://host-{host_id}.example.test/page-{path_id}-{i}"
        )

    return urls


def worker_main(worker_id, workspace, urls):
    router = PartitionRouter(
        partition_count=PARTITIONS,
        database_root=workspace,
    )

    owned = [
        url
        for url in urls
        if router.partition_for(url) == worker_id
    ]

    partition_path = router.partition_path(worker_id)

    os.makedirs(os.path.dirname(partition_path), exist_ok=True)

    store = URLStateStore(partition_path)

    frontier = CrawlFrontier(
        default_delay=0,
        max_retries=0,
        storage_path=partition_path,
        state_store=store,
    )

    inserted = 0
    completed = 0

    for url in owned:
        if frontier.add(url, priority=50, available_at=0):
            inserted += 1

    while True:
        url = frontier.get_next()

        if url is None:
            break

        frontier.complete(url)
        completed += 1

    counts = store.counts()

    store.close()

    return {
        "worker": worker_id,
        "assigned": len(owned),
        "inserted": inserted,
        "completed": completed,
        "counts": counts,
    }


def main():
    print("=" * 72)
    print("STAGE 5.1.2 — PARTITION CONCURRENCY STRESS GATE")
    print("=" * 72)

    workspace = tempfile.mkdtemp(
        prefix="our_search_stage5_1_2_concurrency_"
    )

    print(f"Workspace: {workspace}")
    print(f"Partitions: {PARTITIONS}")
    print(f"Workers:   {WORKERS}")
    print(f"URLs:      {TOTAL_URLS}")
    print()

    try:
        urls = build_urls()

        router = PartitionRouter(
            partition_count=PARTITIONS,
            database_root=workspace,
        )

        # Prove deterministic ownership before concurrency.
        first = {
            url: router.partition_for(url)
            for url in urls
        }

        second = {
            url: router.partition_for(url)
            for url in urls
        }

        assert first == second
        print("PASS — deterministic routing")

        distribution = {}

        for url in urls:
            pid = router.partition_for(url)
            distribution[pid] = distribution.get(pid, 0) + 1

        assert len(distribution) == PARTITIONS
        print(
            "PASS — URLs distributed across "
            f"{len(distribution)}/{PARTITIONS} partitions"
        )

        start = time.perf_counter()

        ctx = mp.get_context("spawn")

        with ctx.Pool(WORKERS) as pool:
            results = pool.starmap(
                worker_main,
                [
                    (worker_id, workspace, urls)
                    for worker_id in range(WORKERS)
                ],
            )

        elapsed = time.perf_counter() - start

        total_assigned = sum(r["assigned"] for r in results)
        total_inserted = sum(r["inserted"] for r in results)
        total_completed = sum(r["completed"] for r in results)

        assert total_assigned == TOTAL_URLS
        print(
            f"PASS — ownership coverage "
            f"{total_assigned}/{TOTAL_URLS}"
        )

        assert total_inserted == TOTAL_URLS
        print(
            f"PASS — concurrent insertion "
            f"{total_inserted}/{TOTAL_URLS}"
        )

        assert total_completed == TOTAL_URLS
        print(
            f"PASS — concurrent completion "
            f"{total_completed}/{TOTAL_URLS}"
        )

        # Verify every partition database exists.
        database_count = 0

        for partition_id in range(PARTITIONS):
            path = router.partition_path(partition_id)

            if os.path.exists(path):
                database_count += 1

        assert database_count == PARTITIONS
        print(
            f"PASS — independent databases "
            f"{database_count}/{PARTITIONS}"
        )

        # Reopen every database and verify persistence.
        persisted = 0

        for partition_id in range(PARTITIONS):
            path = router.partition_path(partition_id)

            store = URLStateStore(path)
            counts = store.counts()
            store.close()

            persisted += counts.get("crawled", 0)

        assert persisted == TOTAL_URLS
        print(
            f"PASS — restart persistence "
            f"{persisted}/{TOTAL_URLS}"
        )

        throughput = TOTAL_URLS / elapsed if elapsed > 0 else 0

        print()
        print(f"Elapsed:   {elapsed:.3f}s")
        print(f"Throughput: {throughput:.2f} URLs/s")
        print()

        print("=" * 72)
        print("RESULT: PASS")
        print("STAGE 5.1.2 PARTITION CONCURRENCY: PASS")
        print("=" * 72)

    except Exception:
        print()
        print("=" * 72)
        print("RESULT: FAIL")
        print("=" * 72)
        raise

    finally:
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()
