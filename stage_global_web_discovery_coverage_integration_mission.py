from __future__ import annotations

import shutil
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path

from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
)
from crawler_system.domain_discovery_fabric import DomainDiscoveryFabric
from crawler_system.domain_candidate_pipeline import DomainCandidatePipeline
from crawler_system.global_web_discovery_integration import (
    GlobalWebDiscoveryIntegration,
)
from crawler_system.whole_web_crawler import WholeWebCrawler


class SyntheticGlobalDomainSource:
    name = "synthetic_global_test"

    def __init__(self, count: int):
        self.count = int(count)

    def can_discover(self, context: DomainDiscoveryContext) -> bool:
        return True

    def discover(
        self,
        context: DomainDiscoveryContext,
    ) -> set[DomainCandidate]:
        return {
            DomainCandidate(
                hostname=f"site-{i:06d}.example",
                url=f"https://site-{i:06d}.example/",
                source=self.name,
                evidence=f"synthetic-{i}",
            )
            for i in range(self.count)
        }


class ConcurrentSyntheticSource:
    def __init__(self, start: int, count: int):
        self.start = int(start)
        self.count = int(count)
        self.name = f"synthetic_concurrent_test_{self.start}"

    def can_discover(self, context: DomainDiscoveryContext) -> bool:
        return True

    def discover(
        self,
        context: DomainDiscoveryContext,
    ) -> set[DomainCandidate]:
        return {
            DomainCandidate(
                hostname=f"parallel-{i:06d}.example",
                url=f"https://parallel-{i:06d}.example/",
                source=self.name,
                evidence=f"parallel-{i}",
            )
            for i in range(
                self.start,
                self.start + self.count,
            )
        }


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"PASS — {message}")


def main() -> None:
    root = Path(
        tempfile.mkdtemp(
            prefix="our_search_global_integration_"
        )
    )

    try:
        # =========================================================
        # 1. Production crawler construction
        # =========================================================

        crawler = WholeWebCrawler(
            worker_count=2,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=1,
            storage_root=str(root),
        )

        check(
            isinstance(
                crawler.global_web_discovery,
                GlobalWebDiscoveryIntegration,
            ),
            "production crawler contains global discovery integration",
        )

        check(
            crawler.global_web_discovery.fabric.shard_count == 64,
            "global discovery fabric has 64 durable shards",
        )

        check(
            "certificate_transparency"
            in crawler.global_web_discovery.registry.names(),
            "certificate-transparency source is registered",
        )

        # =========================================================
        # 2. Replace/add deterministic test source
        # =========================================================

        registry = crawler.global_web_discovery.registry

        registry.register(
            SyntheticGlobalDomainSource(5000)
        )

        # =========================================================
        # 3. Global discovery cycle
        # =========================================================

        result = crawler.global_web_discovery.cycle(
            force=True
        )

        check(
            isinstance(result, dict),
            "global discovery cycle completed",
        )

        fabric_stats = (
            crawler.global_web_discovery.fabric.stats()
        )

        check(
            fabric_stats["total"] >= 5000,
            "5,000 independent domain discoveries entered durable fabric",
        )

        check(
            fabric_stats["total"] >= 5000
            and (
                fabric_stats["queued"]
                + fabric_stats["processing"]
                + fabric_stats["complete"]
                + fabric_stats["failed"]
            ) >= 5000,
            "discovered domains are durably represented in the global fabric",
        )

        # =========================================================
        # 4. Duplicate protection
        # =========================================================

        result_duplicate = crawler.global_web_discovery.cycle(
            force=True
        )

        fabric_stats_duplicate = (
            crawler.global_web_discovery.fabric.stats()
        )

        check(
            fabric_stats_duplicate["total"] == fabric_stats["total"],
            "duplicate global discoveries do not increase durable workload",
        )

        # =========================================================
        # 5. Durable claim/process/activation
        # =========================================================

        processed = crawler.global_web_discovery.process_once(
            max_rounds=20
        )

        check(
            isinstance(processed, dict),
            "global durable processing completed",
        )

        after_processing = (
            crawler.global_web_discovery.fabric.stats()
        )

        check(
            after_processing["processing"] == 0,
            "no domain remains stuck in processing state",
        )

        check(
            after_processing["complete"] > 0,
            "domain discoveries reach durable complete state",
        )

        # =========================================================
        # 6. Production crawler ingestion
        # =========================================================

        check(
            crawler.stats["accepted_urls"] > 0
            or crawler.url_state.count() > 0,
            "global domain activation reaches production URL state",
        )

        # =========================================================
        # 7. Concurrent independent source execution
        # =========================================================

        concurrent_registry = (
            DomainDiscoverySourceRegistry()
        )

        concurrent_registry.register(
            ConcurrentSyntheticSource(0, 2500)
        )

        concurrent_registry.register(
            ConcurrentSyntheticSource(2500, 2500)
        )

        contexts = [
            DomainDiscoveryContext(
                metadata={"worker": i}
            )
            for i in range(8)
        ]

        concurrent_results = []

        def discover_worker():
            concurrent_results.append(
                concurrent_registry.discover(
                    contexts[0]
                )
            )

        threads = [
            threading.Thread(
                target=discover_worker
            )
            for _ in range(8)
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        check(
            len(concurrent_results) == 8,
            "concurrent discovery workers complete successfully",
        )

        check(
            all(
                isinstance(item, set)
                for item in concurrent_results
            ),
            "concurrent source results remain valid",
        )

        # =========================================================
        # 8. Direct large durable fabric workload
        # =========================================================

        stress_root = root / "stress_fabric"

        fabric = DomainDiscoveryFabric(
            storage_root=str(stress_root),
            shard_count=64,
            lease_timeout=1.0,
        )

        records = []

        for i in range(50000):
            records.append(
                __import__(
                    "crawler_system.domain_discovery_fabric",
                    fromlist=["DomainDiscoveryRecord"],
                ).DomainDiscoveryRecord(
                    hostname=f"stress-{i:06d}.example",
                    url=f"https://stress-{i:06d}.example/",
                    source="stress",
                    evidence=f"stress-{i}",
                )
            )

        inserted = fabric.enqueue_many(records)

        check(
            inserted["inserted"] == 50000,
            "50,000 global-domain records durably inserted",
        )

        check(
            inserted["duplicates"] == 0,
            "50,000-record stress workload starts duplicate-free",
        )

        # =========================================================
        # 9. Concurrent claims across all shards
        # =========================================================

        claimed = []
        claim_lock = threading.Lock()

        def claim_worker(worker_id: int):
            local = []

            for shard_id in range(
                worker_id,
                fabric.shard_count,
                8,
            ):
                items = fabric.claim_many(
                    shard_id=shard_id,
                    limit=2000,
                    lease_owner=f"stress-worker-{worker_id}",
                )
                local.extend(
                    (item, f"stress-worker-{worker_id}")
                    for item in items
                )

            with claim_lock:
                claimed.extend(local)

        workers = [
            threading.Thread(
                target=claim_worker,
                args=(i,),
            )
            for i in range(8)
        ]

        for worker in workers:
            worker.start()

        for worker in workers:
            worker.join()

        check(
            len(claimed) == 50000,
            "all 50,000 global-domain records claimed exactly once",
        )

        check(
            len({
                (item.shard_id, item.hostname)
                for item, _lease_owner in claimed
            }) == 50000,
            "no concurrent claim collisions occurred",
        )

        # =========================================================
        # 10. Complete all claimed records
        # =========================================================

        completed = 0

        for item, lease_owner in claimed:
            if fabric.mark_complete(
                item,
                lease_owner=lease_owner,
            ):
                completed += 1

        check(
            completed == 50000,
            "all 50,000 claimed records reach durable complete state",
        )

        final_stress_stats = fabric.stats()

        check(
            final_stress_stats["queued"] == 0,
            "stress fabric has no queued work remaining",
        )

        check(
            final_stress_stats["processing"] == 0,
            "stress fabric has no processing work remaining",
        )

        check(
            final_stress_stats["complete"] == 50000,
            "stress fabric durably records 50,000 completed domains",
        )

        # =========================================================
        # 11. Restart persistence
        # =========================================================

        restart_root = root / "restart_fabric"

        fabric_a = DomainDiscoveryFabric(
            storage_root=str(restart_root),
            shard_count=64,
            lease_timeout=1.0,
        )

        records_restart = []

        from crawler_system.domain_discovery_fabric import (
            DomainDiscoveryRecord,
        )

        for i in range(1000):
            records_restart.append(
                DomainDiscoveryRecord(
                    hostname=f"restart-{i:04d}.example",
                    url=f"https://restart-{i:04d}.example/",
                    source="restart_test",
                    evidence=str(i),
                )
            )

        fabric_a.enqueue_many(records_restart)

        before_restart = fabric_a.stats()["queued"]

        del fabric_a

        fabric_b = DomainDiscoveryFabric(
            storage_root=str(restart_root),
            shard_count=64,
            lease_timeout=1.0,
        )

        after_restart = fabric_b.stats()["queued"]

        check(
            before_restart == 1000,
            "restart test queues 1,000 domains",
        )

        check(
            after_restart == 1000,
            "1,000 queued domains survive fabric restart",
        )

        # =========================================================
        # 12. Lease recovery
        # =========================================================

        claimed_restart = None

        for shard_id in range(64):
            items = fabric_b.claim_many(
                shard_id=shard_id,
                limit=1,
                lease_owner="lease-test-worker",
            )
            if items:
                claimed_restart = items[0]
                break

        check(
            claimed_restart is not None,
            "restart workload produces a durable lease",
        )

        import time

        time.sleep(1.2)

        recovered = fabric_b.recover_expired_leases()

        check(
            recovered >= 1,
            "expired global-domain lease is recovered",
        )

        # =========================================================
        # FINAL GATE
        # =========================================================

        print()
        print(
            "GLOBAL WEB DISCOVERY & COVERAGE "
            "INTEGRATION — MISSION RESULT: PASS"
        )

    finally:
        shutil.rmtree(
            root,
            ignore_errors=True,
        )


if __name__ == "__main__":
    main()
