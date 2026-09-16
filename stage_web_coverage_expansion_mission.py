import os
import shutil
import tempfile
import threading
import time

from crawler_system.discovery_fabric import (
    DiscoveryFabric,
    DiscoveryRecord,
)


def check(condition, message):
    if not condition:
        raise AssertionError(message)

    print(f"PASS — {message}")


def main():
    root = tempfile.mkdtemp(
        prefix="our_search_web_coverage_"
    )

    try:
        # ---------------------------------------------------------
        # 1. Large partition fabric
        # ---------------------------------------------------------

        fabric = DiscoveryFabric(
            storage_root=root,
            shard_count=64,
            lease_timeout=1.0,
        )

        check(
            fabric.shard_count == 64,
            "64 durable discovery shards initialized",
        )

        # ---------------------------------------------------------
        # 2. Deterministic partitioning
        # ---------------------------------------------------------

        samples = [
            "example.com",
            "openai.com",
            "youtube.com",
            "wikipedia.org",
            "stanford.edu",
        ]

        for hostname in samples:
            first = fabric.shard_for_hostname(hostname)
            second = fabric.shard_for_hostname(hostname)

            check(
                first == second,
                f"deterministic partitioning for {hostname}",
            )

        # ---------------------------------------------------------
        # 3. Massive synthetic workload
        # ---------------------------------------------------------

        total = 50000

        records = []

        for index in range(total):
            hostname = f"site-{index}.example"

            url = f"https://{hostname}/"

            records.append(
                DiscoveryRecord(
                    key=f"url-{index}",
                    url=url,
                    hostname=hostname,
                    source="synthetic",
                    source_url="https://seed.example/",
                    priority=float(index % 100),
                    metadata={
                        "workload": "web_coverage_expansion",
                        "sequence": index,
                    },
                )
            )

        result = fabric.enqueue_many(records)

        check(
            result["inserted"] == total,
            f"{total:,} unique discoveries durably inserted",
        )

        check(
            result["duplicates"] == 0,
            "initial workload contains no duplicates",
        )

        # ---------------------------------------------------------
        # 4. Duplicate protection
        # ---------------------------------------------------------

        duplicate_result = fabric.enqueue_many(records[:10000])

        check(
            duplicate_result["inserted"] == 0,
            "duplicate discoveries are not inserted again",
        )

        check(
            duplicate_result["duplicates"] == 10000,
            "10,000 duplicate discoveries detected",
        )

        # ---------------------------------------------------------
        # 5. Partition distribution
        # ---------------------------------------------------------

        distribution = fabric.distribution()

        active_shards = sum(
            1 for value in distribution if value > 0
        )

        check(
            active_shards >= 40,
            "large workload distributes across many shards",
        )

        # ---------------------------------------------------------
        # 6. Concurrent shard claims
        # ---------------------------------------------------------

        claimed_keys = set()
        claimed_lock = threading.Lock()
        errors = []

        def worker(worker_id):
            try:
                for shard_id in range(
                    worker_id,
                    fabric.shard_count,
                    4,
                ):
                    while True:
                        batch = fabric.claim_many(
                            shard_id=shard_id,
                            limit=100,
                            lease_owner=f"worker-{worker_id}",
                        )

                        if not batch:
                            break

                        with claimed_lock:
                            for item in batch:
                                if item.key in claimed_keys:
                                    raise AssertionError(
                                        "duplicate claim detected"
                                    )

                                claimed_keys.add(item.key)

                        for item in batch:
                            fabric.mark_complete(
                                shard_id=item.shard_id,
                                key=item.key,
                                lease_owner=f"worker-{worker_id}",
                            )

            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(
                target=worker,
                args=(worker_id,),
            )
            for worker_id in range(4)
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        check(
            not errors,
            "concurrent shard workers complete without claim collisions",
        )

        check(
            len(claimed_keys) == total,
            f"all {total:,} discoveries claimed exactly once",
        )

        stats = fabric.global_stats()

        check(
            stats["complete"] == total,
            "all claimed discoveries reach durable complete state",
        )

        check(
            stats["queued"] == 0,
            "no queued work remains after processing",
        )

        # ---------------------------------------------------------
        # 7. Lease recovery
        # ---------------------------------------------------------

        recovery_fabric = DiscoveryFabric(
            storage_root=os.path.join(
                root,
                "recovery",
            ),
            shard_count=8,
            lease_timeout=0.2,
        )

        recovery_fabric.enqueue(
            url="https://expired.example/",
            hostname="expired.example",
            source="synthetic",
        )

        shard_id = recovery_fabric.shard_for_hostname(
            "expired.example"
        )

        claimed = recovery_fabric.claim_many(
            shard_id=shard_id,
            limit=1,
            lease_owner="crashed-worker",
        )

        check(
            len(claimed) == 1,
            "test discovery enters processing state",
        )

        time.sleep(0.3)

        recovered = recovery_fabric.recover_expired_leases()

        check(
            recovered == 1,
            "expired discovery lease is recovered",
        )

        check(
            recovery_fabric.global_stats()["queued"] == 1,
            "recovered discovery returns to queued state",
        )

        # ---------------------------------------------------------
        # 8. Restart durability
        # ---------------------------------------------------------

        restart_root = os.path.join(
            root,
            "restart",
        )

        first_instance = DiscoveryFabric(
            storage_root=restart_root,
            shard_count=16,
        )

        first_instance.enqueue(
            url="https://persistent.example/",
            hostname="persistent.example",
            source="restart-test",
        )

        before = first_instance.global_stats()

        del first_instance

        second_instance = DiscoveryFabric(
            storage_root=restart_root,
            shard_count=16,
        )

        after = second_instance.global_stats()

        check(
            before["total"] == after["total"],
            "discovery state survives fabric restart",
        )

        check(
            after["queued"] == 1,
            "queued discovery survives restart",
        )

        # ---------------------------------------------------------
        # FINAL GATE
        # ---------------------------------------------------------

        print()
        print("=" * 62)
        print("WEB COVERAGE EXPANSION — MISSION RESULT: PASS")
        print("=" * 62)

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
