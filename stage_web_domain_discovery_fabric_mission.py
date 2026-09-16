from __future__ import annotations

import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from crawler_system.domain_discovery_fabric import (
    DomainDiscoveryFabric,
)
from crawler_system.domain_discovery_fabric_controller import (
    DomainDiscoveryFabricController,
)
from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
)


class SyntheticDomainSource:
    def __init__(
        self,
        name: str,
        prefix: str,
        count: int,
        delay: float = 0.001,
    ):
        self.name = name
        self.prefix = prefix
        self.count = count
        self.delay = delay

        self._active = 0
        self._max_active = 0
        self._lock = threading.Lock()

    @property
    def max_active(self) -> int:
        with self._lock:
            return self._max_active

    def can_discover(
        self,
        context: DomainDiscoveryContext,
    ) -> bool:
        return True

    def discover(
        self,
        context: DomainDiscoveryContext,
    ) -> set[DomainCandidate]:
        with self._lock:
            self._active += 1
            self._max_active = max(
                self._max_active,
                self._active,
            )

        try:
            time.sleep(self.delay)

            context_name = (
                context.seed_domain
                or "context"
            )

            results = set()

            for index in range(self.count):
                hostname = (
                    f"{self.prefix}-"
                    f"{context_name.replace('.', '-')}-"
                    f"{index}.example"
                )

                results.add(
                    DomainCandidate(
                        hostname=hostname,
                        url=f"https://{hostname}/",
                        source=self.name,
                        evidence=(
                            f"synthetic:{context_name}"
                        ),
                        metadata={
                            "test_source": self.name,
                            "context": context_name,
                        },
                    )
                )

            return results

        finally:
            with self._lock:
                self._active -= 1


class FailingActivation:
    def __init__(self, failures: int = 25):
        self.remaining_failures = failures
        self.lock = threading.Lock()
        self.attempts = 0
        self.successes = 0
        self.failures = 0

    def __call__(
        self,
        hostname: str,
        url: str,
        priority: int,
        source: str | None,
    ) -> bool:
        with self.lock:
            self.attempts += 1

            if self.remaining_failures > 0:
                self.remaining_failures -= 1
                self.failures += 1
                return False

            self.successes += 1
            return True


def assert_true(
    condition: bool,
    message: str,
) -> None:
    if not condition:
        raise AssertionError(message)

    print(f"PASS — {message}")


def main() -> None:
    root = Path(
        tempfile.mkdtemp(
            prefix="our_search_domain_fabric_"
        )
    )

    try:
        fabric_root = (
            root / "domain_discovery_fabric"
        )

        fabric = DomainDiscoveryFabric(
            storage_root=str(fabric_root),
            shard_count=64,
            lease_timeout=1.0,
        )

        assert_true(
            fabric.shard_count == 64,
            "64 durable domain discovery shards initialized",
        )

        registry = DomainDiscoverySourceRegistry()

        source_a = SyntheticDomainSource(
            name="synthetic_a",
            prefix="alpha",
            count=25,
        )

        source_b = SyntheticDomainSource(
            name="synthetic_b",
            prefix="beta",
            count=25,
        )

        registry.register(source_a)
        registry.register(source_b)

        controller = DomainDiscoveryFabricController(
            registry=registry,
            fabric=fabric,
            discovery_workers=16,
            domain_workers=32,
            claim_batch_size=50,
            lease_recovery_interval=0.1,
            cycle_interval=0.0,
        )

        contexts = [
            DomainDiscoveryContext(
                seed_domain=f"seed-{index}"
            )
            for index in range(100)
        ]

        discovery_result = controller.discover_contexts(
            contexts
        )

        expected_unique = 100 * 25 * 2

        assert_true(
            discovery_result["contexts"] == 100,
            "100 discovery contexts processed",
        )

        assert_true(
            discovery_result["accepted"]
            == expected_unique,
            "all synthetic domain candidates accepted",
        )

        assert_true(
            discovery_result["inserted"]
            == expected_unique,
            "all unique domain discoveries inserted",
        )

        assert_true(
            source_a.max_active == 1,
            "same discovery source executions are serialized safely",
        )

        assert_true(
            source_b.max_active == 1,
            "second discovery source executions are serialized safely",
        )

        duplicate_context = [
            DomainDiscoveryContext(
                seed_domain=f"seed-{index}"
            )
            for index in range(100)
        ]

        duplicate_result = controller.discover_contexts(
            duplicate_context
        )

        assert_true(
            duplicate_result["duplicates"]
            == expected_unique,
            "duplicate domain discoveries are suppressed durably",
        )

        stats = fabric.stats()

        assert_true(
            stats["total"] == expected_unique,
            "fabric contains exactly the unique domain workload",
        )

        distribution = stats["shards"]

        active_shards = sum(
            1
            for shard in distribution
            if shard["total"] > 0
        )

        assert_true(
            active_shards >= 40,
            "domain workload distributes across many durable shards",
        )

        activation = FailingActivation(
            failures=25
        )

        controller.activation_callback = activation

        processing = controller.process_available(
            max_rounds=10
        )

        assert_true(
            processing["claimed"]
            >= expected_unique,
            "all domain discoveries are claimed by concurrent workers",
        )

        assert_true(
            activation.failures == 25,
            "activation failure injection executed",
        )

        assert_true(
            activation.successes
            == expected_unique,
            "all domains eventually activate successfully",
        )

        assert_true(
            processing["retried"] >= 25,
            "activation failures return to durable retry flow",
        )

        final_stats = fabric.stats()

        assert_true(
            final_stats["queued"] == 0,
            "no queued domain discovery work remains",
        )

        assert_true(
            final_stats["processing"] == 0,
            "no processing domain discovery work remains",
        )

        assert_true(
            final_stats["complete"]
            == expected_unique,
            "all unique domains reach durable complete state",
        )

        lease_owner = "lease-test-worker"

        lease_target = (
            "lease-recovery.example"
        )

        fabric.enqueue(
            hostname=lease_target,
            url=f"https://{lease_target}/",
            source="lease_test",
            priority=50,
        )

        claimed = fabric.claim_many(
            shard_id=fabric.shard_for_hostname(
                lease_target
            ),
            limit=1,
            lease_owner=lease_owner,
        )

        assert_true(
            len(claimed) == 1,
            "test domain enters leased processing state",
        )

        time.sleep(1.2)

        recovered = controller.recover_expired_leases(
            force=True
        )

        assert_true(
            recovered >= 1,
            "expired domain discovery lease is recovered",
        )

        recovered_claim = fabric.claim_many(
            shard_id=fabric.shard_for_hostname(
                lease_target
            ),
            limit=1,
            lease_owner="recovery-worker",
        )

        assert_true(
            len(recovered_claim) == 1,
            "recovered domain returns to durable processing",
        )

        fabric.mark_complete(
            lease_target,
            lease_owner="recovery-worker",
        )

        restart_fabric = DomainDiscoveryFabric(
            storage_root=str(fabric_root),
            shard_count=64,
            lease_timeout=1.0,
        )

        restart_stats = restart_fabric.stats()

        assert_true(
            restart_stats["complete"]
            == expected_unique + 1,
            "domain discovery completion state survives fabric restart",
        )

        restart_fabric.close()

        registry_metrics = registry.metrics()

        assert_true(
            "synthetic_a" in registry_metrics,
            "source A metrics remain available",
        )

        assert_true(
            "synthetic_b" in registry_metrics,
            "source B metrics remain available",
        )

        print(
            "\n"
            "==============================================================\n"
            "DOMAIN DISCOVERY FABRIC — INTEGRATION MISSION: PASS\n"
            "=============================================================="
        )

    finally:
        shutil.rmtree(
            root,
            ignore_errors=True,
        )


if __name__ == "__main__":
    main()
