"""
Stage 5.1.2 — Partition architecture gate.
"""

from __future__ import annotations

import os
import shutil
import tempfile

from crawler_system.partition_router import PartitionRouter
from crawler_system.partitioned_frontier import PartitionedFrontier


def main():
    workspace = tempfile.mkdtemp(
        prefix="our_search_stage5_1_2_"
    )

    print("=" * 72)
    print("STAGE 5.1.2 — PARTITION ARCHITECTURE GATE")
    print("=" * 72)
    print(f"Workspace: {workspace}")

    try:
        storage_root = os.path.join(
            workspace,
            "partitions",
        )

        router = PartitionRouter(
            partition_count=4,
            database_root=storage_root,
        )

        urls = [
            "https://example.com/",
            "https://example.com/about",
            "https://example.com/contact",
            "https://iana.org/",
            "https://iana.org/domains/",
            "https://wikipedia.org/",
            "https://python.org/",
            "https://github.com/",
        ]

        # ----------------------------------------------------------
        # 1. Deterministic routing
        # ----------------------------------------------------------

        for url in urls:
            first = router.partition_for(url)
            second = router.partition_for(url)

            assert first == second
            assert 0 <= first < 4

        print("PASS — deterministic routing")

        # ----------------------------------------------------------
        # 2. Host affinity
        # ----------------------------------------------------------

        hosts = [
            "example.com",
            "iana.org",
            "wikipedia.org",
            "python.org",
            "github.com",
        ]

        for host in hosts:
            first = router.partition_for(
                f"https://{host}/a"
            )
            second = router.partition_for(
                f"https://{host}/b"
            )

            assert first == second, (
                f"host affinity violated: {host}"
            )

        print("PASS — host affinity")

        # ----------------------------------------------------------
        # 3. Create partitioned frontier
        # ----------------------------------------------------------

        frontier = PartitionedFrontier(
            partition_count=4,
            default_delay=0,
            max_retries=3,
            storage_root=storage_root,
        )

        # ----------------------------------------------------------
        # 4. Insert URLs
        # ----------------------------------------------------------

        inserted = 0

        for index, url in enumerate(urls):
            if frontier.add(
                url,
                priority=100 - index,
                source="stage5_1_2_test",
            ):
                inserted += 1

        assert inserted == len(urls), (
            f"insert mismatch: "
            f"expected {len(urls)}, got {inserted}"
        )

        print(
            f"PASS — inserted {inserted} URLs"
        )

        # ----------------------------------------------------------
        # 5. Single partition ownership
        # ----------------------------------------------------------

        ownership = {}

        for url in urls:
            partition_id = frontier.partition_for(url)

            assert 0 <= partition_id < 4
            ownership[url] = partition_id

        print("PASS — single partition ownership")

        # ----------------------------------------------------------
        # 6. Independent databases
        # ----------------------------------------------------------

        database_paths = []

        for partition_id in range(4):
            database_path = router.partition_path(
                partition_id
            )

            assert os.path.exists(database_path), (
                f"missing database: {database_path}"
            )

            database_paths.append(database_path)

        assert len(set(database_paths)) == 4

        print(
            "PASS — independent partition databases"
        )

        # ----------------------------------------------------------
        # 7. Claim and complete all URLs
        # ----------------------------------------------------------

        completed = 0

        for _ in range(len(urls) * 10):
            url = frontier.get_next()

            if url is None:
                break

            assert url in ownership

            result = frontier.complete(url)

            assert result is True

            completed += 1

        assert completed == len(urls), (
            f"completion mismatch: "
            f"expected {len(urls)}, got {completed}"
        )

        counts = frontier.counts()

        assert counts["crawled"] == len(urls)

        print(
            f"PASS — completed {completed} URLs"
        )

        # ----------------------------------------------------------
        # 8. Restart persistence
        # ----------------------------------------------------------

        frontier.close()

        restarted = PartitionedFrontier(
            partition_count=4,
            default_delay=0,
            max_retries=3,
            storage_root=storage_root,
        )

        restarted_counts = restarted.counts()

        assert (
            restarted_counts["crawled"]
            == len(urls)
        )

        print("PASS — restart persistence")

        restarted.close()

        print("=" * 72)
        print("RESULT: PASS")
        print("STAGE 5.1.2 PARTITION ARCHITECTURE: PASS")
        print("=" * 72)

    except Exception:
        print("=" * 72)
        print("RESULT: FAIL")
        print("=" * 72)
        raise

    finally:
        shutil.rmtree(
            workspace,
            ignore_errors=True,
        )


if __name__ == "__main__":
    main()
