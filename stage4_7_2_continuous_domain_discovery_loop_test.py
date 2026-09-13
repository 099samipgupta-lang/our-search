import os
import tempfile

from crawler_system.continuous_domain_discovery_loop import (
    ContinuousDomainDiscoveryLoop,
)
from crawler_system.domain_candidate_activation import (
    DomainCandidateActivator,
)
from crawler_system.domain_candidate_pipeline import (
    DomainCandidatePipeline,
)
from crawler_system.domain_candidate_store import (
    DomainCandidateStore,
)
from crawler_system.domain_discovery_pipeline import (
    DomainDiscoveryPipeline,
)
from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
)
from crawler_system.expansion_queue import ExpansionQueue
from crawler_system.expansion_store import ExpansionCandidateStore


class FakeDiscoverySource:
    name = "fake-domain-source"

    def __init__(self):
        self.calls = []

    def can_discover(self, context):
        return True

    def discover(self, context):
        self.calls.append(context)

        return {
            DomainCandidate(
                hostname="example.com",
                url="https://example.com/",
                source="certificate_transparency",
                evidence="ct",
                metadata={"test": True},
            ),
            DomainCandidate(
                hostname="example.org",
                url="https://example.org/",
                source="registry",
                evidence="registry",
                metadata={"test": True},
            ),
        }


class FakeDiscoveryRegistry(DomainDiscoverySourceRegistry):
    def __init__(self):
        super().__init__()
        self.source = FakeDiscoverySource()
        self.register(self.source)

    @property
    def calls(self):
        return self.source.calls


class FailureDiscoverySource:
    name = "failure-domain-source"

    def __init__(self):
        self.calls = []

    def can_discover(self, context):
        return True

    def discover(self, context):
        self.calls.append(context)

        if (
            context is not None
            and getattr(context, "seed_domain", None)
            == "bad.example"
        ):
            raise RuntimeError(
                "simulated discovery failure"
            )

        return {
            DomainCandidate(
                hostname="good.example",
                url="https://good.example/",
                source="registry",
                evidence="test",
                metadata={"healthy": True},
            )
        }


class FailureDiscoveryRegistry(DomainDiscoverySourceRegistry):
    def __init__(self):
        super().__init__()
        self.source = FailureDiscoverySource()
        self.register(self.source)

    @property
    def calls(self):
        return self.source.calls


class FailingActivator:
    def activate(self, hostname):
        if hostname == "bad-activation.example":
            raise RuntimeError(
                "simulated activation failure"
            )

        return {
            "success": True,
            "status": "activated",
            "hostname": hostname,
        }


def build_components(temp_dir, registry):
    domain_db = os.path.join(
        temp_dir,
        "domain_candidates.db",
    )

    expansion_db = os.path.join(
        temp_dir,
        "expansion_candidates.db",
    )

    queue_db = os.path.join(
        temp_dir,
        "expansion_queue.db",
    )

    domain_store = DomainCandidateStore(
        database_path=domain_db,
    )

    expansion_store = ExpansionCandidateStore(
        database_path=expansion_db,
    )

    expansion_queue = ExpansionQueue(
        database_path=queue_db,
    )

    candidate_pipeline = DomainCandidatePipeline()

    discovery_pipeline = DomainDiscoveryPipeline(
        registry=registry,
        store=domain_store,
        candidate_pipeline=candidate_pipeline,
    )

    activator = DomainCandidateActivator(
        domain_store=domain_store,
        expansion_store=expansion_store,
        expansion_queue=expansion_queue,
    )

    return (
        domain_store,
        expansion_store,
        expansion_queue,
        discovery_pipeline,
        activator,
    )


def main():
    temp_dir = tempfile.mkdtemp(
        prefix="stage4_7_2_"
    )

    # ============================================================
    # BASIC CONTINUOUS DISCOVERY FLOW
    # ============================================================

    registry = FakeDiscoveryRegistry()

    (
        domain_store,
        expansion_store,
        expansion_queue,
        discovery_pipeline,
        activator,
    ) = build_components(
        temp_dir,
        registry,
    )

    loop = ContinuousDomainDiscoveryLoop(
        discovery_pipeline=discovery_pipeline,
        activator=activator,
        contexts=[
            DomainDiscoveryContext(
                seed_domain="seed-one.example"
            ),
            DomainDiscoveryContext(
                seed_domain="seed-two.example"
            ),
        ],
        cycle_interval=0.1,
        max_contexts_per_cycle=10,
        activation_batch_size=100,
    )

    assert loop.status()["running"] is False
    print("LOOP INITIALIZED: PASS")

    result = loop.run_cycle()

    assert result["discovery"]["contexts"] == 2
    print("MULTI-CONTEXT DISCOVERY: PASS")

    assert result["discovery"]["discovered"] == 4
    print("DISCOVERY RESULT COUNT: PASS")

    assert result["discovery"]["accepted"] == 2
    print("PIPELINE ACCEPTANCE: PASS")

    assert result["discovery"]["stored"] == 2
    print("DURABLE STORAGE: PASS")

    assert result["activation"]["requested"] == 2
    print("ACTIVATION REQUEST COUNT: PASS")

    assert result["activation"]["activated"] == 2
    print("DOMAIN ACTIVATION: PASS")

    assert domain_store.count() == 2
    print("DOMAIN STORE COUNT: PASS")

    assert (
        expansion_queue.count("queued") == 2
    )
    print("EXPANSION QUEUE PROPAGATION: PASS")

    assert len(registry.calls) == 2
    print("REGISTRY EXECUTION: PASS")

    # ============================================================
    # SECOND CYCLE / DEDUPLICATION
    # ============================================================

    second = loop.run_cycle()

    assert second["discovery"]["accepted"] == 0
    print("CROSS-CYCLE PIPELINE DEDUPLICATION: PASS")

    assert domain_store.count() == 2
    print("NO DUPLICATE DOMAIN STORAGE: PASS")

    assert (
        expansion_queue.count("queued") == 2
    )
    print("NO DUPLICATE EXPANSION QUEUING: PASS")

    # ============================================================
    # LIFECYCLE
    # ============================================================

    assert loop.start() is True
    print("START: PASS")

    assert loop.start() is False
    print("START IDEMPOTENCY: PASS")

    assert loop.stop() is True
    print("STOP: PASS")

    assert loop.stop() is False
    print("STOP IDEMPOTENCY: PASS")

    # ============================================================
    # DURABLE RECOVERY / ACTIVATION AFTER RESTART
    # ============================================================

    recovery_dir = tempfile.mkdtemp(
        prefix="stage4_7_2_recovery_"
    )

    recovery_registry = FakeDiscoveryRegistry()

    (
        recovery_domain_store,
        recovery_expansion_store,
        recovery_queue,
        recovery_pipeline,
        recovery_activator,
    ) = build_components(
        recovery_dir,
        recovery_registry,
    )

    recovery_domain_store.add(
        DomainCandidate(
            hostname="recovered.example",
            url="https://recovered.example/",
            source="registry",
            evidence="restart-test",
        )
    )

    recovery_loop = ContinuousDomainDiscoveryLoop(
        discovery_pipeline=recovery_pipeline,
        activator=recovery_activator,
        context_provider=lambda: [],
        cycle_interval=0.1,
        activation_batch_size=100,
    )

    recovery_result = recovery_loop.run_cycle()

    assert (
        recovery_result["activation"]["requested"] == 1
    )
    print("DURABLE UNACTIVATED CANDIDATE RECOVERED: PASS")

    assert (
        recovery_result["activation"]["activated"] == 1
    )
    print("RECOVERED CANDIDATE ACTIVATED: PASS")

    assert (
        recovery_domain_store.get(
            "recovered.example"
        ).get("status")
        == "activated"
    )
    print("RECOVERED STATUS PERSISTED: PASS")

    assert (
        recovery_queue.count("queued") == 1
    )
    print("RECOVERED CANDIDATE REACHES QUEUE: PASS")

    # ============================================================
    # DISCOVERY FAILURE ISOLATION
    # ============================================================

    failure_dir = tempfile.mkdtemp(
        prefix="stage4_7_2_failure_"
    )

    failure_registry = FailureDiscoveryRegistry()

    (
        failure_domain_store,
        failure_expansion_store,
        failure_queue,
        failure_pipeline,
        failure_activator,
    ) = build_components(
        failure_dir,
        failure_registry,
    )

    failure_loop = ContinuousDomainDiscoveryLoop(
        discovery_pipeline=failure_pipeline,
        activator=failure_activator,
        contexts=[
            DomainDiscoveryContext(
                seed_domain="bad.example"
            ),
            DomainDiscoveryContext(
                seed_domain="good.example"
            ),
        ],
        cycle_interval=0.1,
        max_contexts_per_cycle=10,
        activation_batch_size=100,
    )

    failure_result = failure_loop.run_cycle()

    assert (
        failure_result["discovery"]["failures"] == 0
    )
    assert (
        failure_registry.execution_metrics[
            "failure-domain-source"
        ]["failures"] == 1
    )
    print("DISCOVERY FAILURE ISOLATION: PASS")

    assert (
        failure_result["discovery"]["accepted"] == 1
    )
    print("HEALTHY CONTEXT CONTINUES: PASS")

    assert (
        failure_domain_store.count() == 1
    )
    print("HEALTHY CANDIDATE STORED: PASS")

    assert (
        failure_queue.count("queued") == 1
    )
    print("HEALTHY CANDIDATE REACHES QUEUE: PASS")

    assert (
        failure_loop.stats["discovery_failures"] == 0
    )
    print("LOOP DISCOVERY FAILURE METRIC: PASS")

    # ============================================================
    # ACTIVATION FAILURE ISOLATION
    # ============================================================

    activation_dir = tempfile.mkdtemp(
        prefix="stage4_7_2_activation_"
    )

    activation_registry = FakeDiscoveryRegistry()

    (
        activation_domain_store,
        activation_expansion_store,
        activation_queue,
        activation_pipeline,
        _,
    ) = build_components(
        activation_dir,
        activation_registry,
    )

    activation_domain_store.add(
        DomainCandidate(
            hostname="bad-activation.example",
            url="https://bad-activation.example/",
            source="registry",
            evidence="activation-failure",
        )
    )

    activation_domain_store.add(
        DomainCandidate(
            hostname="good-activation.example",
            url="https://good-activation.example/",
            source="registry",
            evidence="activation-success",
        )
    )

    failing_activator = FailingActivator()

    activation_loop = ContinuousDomainDiscoveryLoop(
        discovery_pipeline=activation_pipeline,
        activator=failing_activator,
        context_provider=lambda: [],
        cycle_interval=0.1,
        activation_batch_size=100,
    )

    activation_result = activation_loop.run_cycle()

    assert (
        activation_result["activation"]["requested"] == 2
    )
    print("ACTIVATION BATCH BOUNDARY: PASS")

    assert (
        activation_result["activation"]["activated"] == 1
    )
    print("HEALTHY ACTIVATION CONTINUES: PASS")

    assert (
        activation_result["activation"]["failed"] == 1
    )
    print("ACTIVATION FAILURE ISOLATION: PASS")

    assert (
        activation_loop.stats["activation_failed"] == 1
    )
    print("ACTIVATION FAILURE METRIC: PASS")

    assert (
        activation_domain_store.get(
            "good-activation.example"
        ).get("status")
        == "discovered"
    )
    print("FAKE ACTIVATOR DOES NOT MUTATE STORE: PASS")

    # ============================================================
    # BATCH BOUND
    # ============================================================

    batch_dir = tempfile.mkdtemp(
        prefix="stage4_7_2_batch_"
    )

    batch_registry = FakeDiscoveryRegistry()

    (
        batch_domain_store,
        batch_expansion_store,
        batch_queue,
        batch_pipeline,
        batch_activator,
    ) = build_components(
        batch_dir,
        batch_registry,
    )

    for index in range(5):
        batch_domain_store.add(
            DomainCandidate(
                hostname=f"batch-{index}.example",
                url=(
                    f"https://batch-{index}.example/"
                ),
                source="registry",
                evidence="batch-test",
            )
        )

    batch_loop = ContinuousDomainDiscoveryLoop(
        discovery_pipeline=batch_pipeline,
        activator=batch_activator,
        context_provider=lambda: [],
        cycle_interval=0.1,
        activation_batch_size=2,
    )

    batch_first = batch_loop.run_cycle()

    assert (
        batch_first["activation"]["requested"] == 2
    )
    print("ACTIVATION BATCH LIMIT: PASS")

    assert (
        batch_domain_store.count(
            status="discovered"
        ) == 3
    )
    print("REMAINING CANDIDATES PRESERVED: PASS")

    batch_second = batch_loop.run_cycle()

    assert (
        batch_second["activation"]["requested"] == 2
    )
    print("SECOND BATCH LIMIT: PASS")

    batch_third = batch_loop.run_cycle()

    assert (
        batch_third["activation"]["requested"] == 1
    )
    print("FINAL PARTIAL BATCH: PASS")

    assert (
        batch_domain_store.count(
            status="discovered"
        ) == 0
    )
    print("BATCH QUEUE FULLY DRAINED: PASS")

    # ============================================================
    # METRICS
    # ============================================================

    status = loop.status()

    assert status["stats"]["cycles"] == 2
    print("CYCLE METRICS: PASS")

    assert status["stats"]["discovered"] == 8
    print("DISCOVERED METRICS: PASS")

    assert status["stats"]["accepted"] == 2
    print("ACCEPTED METRICS: PASS")

    assert status["stats"]["stored"] == 2
    print("STORED METRICS: PASS")

    assert status["stats"]["activated"] == 2
    print("ACTIVATION METRICS: PASS")

    assert status["stats"]["activation_failed"] == 0
    print("NO UNEXPECTED ACTIVATION FAILURES: PASS")

    print("RESULT: PASS")


if __name__ == "__main__":
    main()
