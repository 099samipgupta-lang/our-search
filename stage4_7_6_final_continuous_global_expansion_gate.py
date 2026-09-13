import os
import sqlite3
import tempfile

from crawler_system.whole_web_crawler import WholeWebCrawler
from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
)
from crawler_system.domain_candidate_pipeline import DomainCandidatePipeline
from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_discovery_pipeline import DomainDiscoveryPipeline
from crawler_system.domain_candidate_activation import DomainCandidateActivator
from crawler_system.continuous_domain_discovery_loop import (
    ContinuousDomainDiscoveryLoop,
)
from crawler_system.global_expansion_feedback import GlobalExpansionFeedback
from crawler_system.global_expansion_feedback_controller import (
    GlobalExpansionFeedbackController,
)


class FinalGateDiscoverySource:
    name = "final_gate_source"

    def __init__(self):
        self.calls = []

    def can_discover(self, context):
        return True

    def discover(self, context):
        seed = (
            context.seed_domain
            if context is not None
            else "default.example"
        )

        self.calls.append(seed)

        return {
            DomainCandidate(
                hostname=f"alpha-{seed}",
                url=f"https://alpha-{seed}/",
                source=self.name,
                evidence=f"final-gate:{seed}",
            ),
            DomainCandidate(
                hostname=f"beta-{seed}",
                url=f"https://beta-{seed}/",
                source=self.name,
                evidence=f"final-gate:{seed}",
            ),
        }


class FinalGateCrawler(WholeWebCrawler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.added_urls = []

    def _add_url(
        self,
        url,
        source="unknown",
        depth=0,
        seed=False,
        source_url=None,
    ):
        self.added_urls.append(
            {
                "url": url,
                "source": source,
                "depth": depth,
                "seed": seed,
                "source_url": source_url,
            }
        )

        return True


class FailureGateCrawler(WholeWebCrawler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attempts = {}

    def _add_url(
        self,
        url,
        source="unknown",
        depth=0,
        seed=False,
        source_url=None,
    ):
        hostname = url.split("//", 1)[-1].split("/", 1)[0]

        self.attempts[hostname] = (
            self.attempts.get(hostname, 0) + 1
        )

        if hostname.startswith("fail-"):
            return False

        return True


def build_domain_components(temp_dir):
    database_path = os.path.join(
        temp_dir,
        "domain_candidates.db",
    )

    domain_store = DomainCandidateStore(
        database_path=database_path
    )

    registry = DomainDiscoverySourceRegistry()

    source = FinalGateDiscoverySource()
    registry.register(source)

    candidate_pipeline = DomainCandidatePipeline()

    discovery_pipeline = DomainDiscoveryPipeline(
        registry=registry,
        store=domain_store,
        candidate_pipeline=candidate_pipeline,
    )

    return (
        domain_store,
        registry,
        source,
        discovery_pipeline,
    )


def assert_sqlite_integrity(database_path):
    connection = sqlite3.connect(database_path)
    try:
        result = connection.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]
    finally:
        connection.close()

    assert result == "ok"


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        (
            domain_store,
            registry,
            source,
            discovery_pipeline,
        ) = build_domain_components(temp_dir)

        crawler_storage = os.path.join(
            temp_dir,
            "crawler",
        )

        feedback_database = os.path.join(
            temp_dir,
            "global_feedback.db",
        )

        crawler = FinalGateCrawler(
            worker_count=2,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=3,
            storage_root=crawler_storage,
        )

        activator = DomainCandidateActivator(
            domain_store=domain_store,
            expansion_store=crawler.expansion_store,
            expansion_queue=crawler.expansion_queue,
        )

        feedback = GlobalExpansionFeedback(
            database_path=feedback_database
        )

        feedback_controller = GlobalExpansionFeedbackController(
            domain_store=domain_store,
            expansion_queue=crawler.expansion_queue,
            expansion_store=crawler.expansion_store,
            feedback=feedback,
        )

        discovery_loop = ContinuousDomainDiscoveryLoop(
            discovery_pipeline=discovery_pipeline,
            activator=activator,
            contexts=[
                DomainDiscoveryContext(
                    seed_domain="one.example"
                ),
                DomainDiscoveryContext(
                    seed_domain="two.example"
                ),
            ],
            cycle_interval=0.0,
            max_contexts_per_cycle=2,
            activation_batch_size=100,
            feedback_controller=feedback_controller,
        )

        # ---------------------------------------------------------
        # 1. CONTINUOUS DISCOVERY
        # ---------------------------------------------------------

        first_cycle = discovery_loop.run_cycle()

        assert first_cycle["discovery"]["contexts"] == 2
        assert first_cycle["discovery"]["discovered"] == 4
        assert first_cycle["discovery"]["accepted"] == 4
        assert first_cycle["discovery"]["stored"] == 4
        assert first_cycle["discovery"]["failures"] == 0

        print("CONTINUOUS DOMAIN DISCOVERY: PASS")

        # ---------------------------------------------------------
        # 2. DURABLE DOMAIN STORAGE
        # ---------------------------------------------------------

        assert domain_store.count(
            status="discovered"
        ) == 0

        assert domain_store.count(
            status="activated"
        ) == 4

        print("DOMAIN CANDIDATE DURABLE STORAGE: PASS")
        print("DOMAIN ACTIVATION: PASS")

        # ---------------------------------------------------------
        # 3. EXPANSION STORE + QUEUE
        # ---------------------------------------------------------

        assert crawler.expansion_store.count(
            status="discovered"
        ) == 4

        expansion_candidates = (
            crawler.expansion_store.list_candidates(
                status="discovered",
                limit=100,
            )
        )

        assert len(expansion_candidates) == 4

        expected_urls = {
            candidate["first_url"]
            for candidate in expansion_candidates
        }

        assert len(expected_urls) == 4

        assert crawler.expansion_queue.count(
            "queued"
        ) == 4

        print("ACTIVATION → EXPANSION STORE: PASS")
        print("ACTIVATION → EXPANSION QUEUE: PASS")

        # ---------------------------------------------------------
        # 4. REAL CONTINUOUS EXPANSION PROCESSOR
        # ---------------------------------------------------------

        controller = crawler.expansion_controller

        assert controller is not None

        controller_result = controller.run_cycle()

        assert controller_result["processed"] == 4

        assert len(crawler.added_urls) == 4

        assert crawler.expansion_queue.count(
            "queued"
        ) == 0

        assert crawler.expansion_queue.count(
            "processing"
        ) == 0

        assert crawler.expansion_queue.count(
            "complete"
        ) == 4

        print("CONTINUOUS EXPANSION CONTROLLER: PASS")
        print("REAL EXPANSION PROCESSING: PASS")
        print("EXPANSION QUEUE DRAIN: PASS")

        # ---------------------------------------------------------
        # 5. CRAWLER URL PROPAGATION
        # ---------------------------------------------------------

        added_urls = {
            item["url"]
            for item in crawler.added_urls
        }

        assert added_urls == expected_urls

        assert all(
            item["source"] == "expansion"
            for item in crawler.added_urls
        )

        assert all(
            item["depth"] == 0
            for item in crawler.added_urls
        )

        assert all(
            item["seed"] is False
            for item in crawler.added_urls
        )

        print("EXPANSION → CRAWLER PROPAGATION: PASS")

        # ---------------------------------------------------------
        # 6. FEEDBACK OBSERVATION AFTER SUCCESS
        # ---------------------------------------------------------

        feedback_result = feedback_controller.process(
            limit=100
        )

        assert feedback_result["processed"] >= 4

        stats = feedback.source_stats(
            source.name
        )

        assert stats is not None
        assert stats["successes"] >= 1
        assert stats["total_events"] >= 1

        multiplier = (
            feedback_controller.adaptation_multiplier(
                source.name
            )
        )

        assert 0.1 <= multiplier <= 2.0

        adapted_priority = (
            feedback_controller.adapt_candidate_priority(
                {
                    "hostname": "alpha-one.example",
                    "source": source.name,
                    "priority": 50.0,
                }
            )
        )

        assert 0.0 <= adapted_priority <= 100.0

        print("GLOBAL EXPANSION FEEDBACK: PASS")
        print("SOURCE EFFECTIVENESS TRACKING: PASS")
        print("ADAPTIVE PRIORITY: PASS")

        # ---------------------------------------------------------
        # 7. CROSS-CYCLE DEDUPLICATION
        # ---------------------------------------------------------

        second_cycle = discovery_loop.run_cycle()

        assert second_cycle["discovery"]["contexts"] == 2
        assert second_cycle["discovery"]["discovered"] == 4
        assert second_cycle["discovery"]["accepted"] == 0
        assert second_cycle["discovery"]["stored"] == 0

        assert second_cycle["activation"]["requested"] == 0
        assert second_cycle["activation"]["activated"] == 0
        assert second_cycle["activation"]["failed"] == 0

        assert domain_store.count(
            status="activated"
        ) == 4

        assert crawler.expansion_queue.count(
            "queued"
        ) == 0

        assert crawler.expansion_queue.count(
            "processing"
        ) == 0

        assert crawler.expansion_queue.count(
            "complete"
        ) == 4

        print("CROSS-CYCLE DEDUPLICATION: PASS")
        print("NO DUPLICATE ACTIVATION: PASS")
        print("NO DUPLICATE QUEUING: PASS")

        # ---------------------------------------------------------
        # 8. LEASE RECOVERY
        # ---------------------------------------------------------

        lease_url = "https://lease-final.example/"
        crawler.expansion_store.add(lease_url)

        lease_candidate = crawler.expansion_store.get(
            "lease-final.example"
        )

        assert lease_candidate is not None

        crawler.expansion_queue.enqueue(
            lease_candidate,
            priority=50.0,
        )

        claimed = crawler.expansion_queue.claim_next()

        assert claimed is not None
        assert claimed["hostname"] == "lease-final.example"

        recovered = (
            crawler.expansion_queue.recover_expired_leases(
                now=claimed["processing_started_at"] + 100000
            )
        )

        assert recovered == 1

        assert crawler.expansion_queue.count(
            "queued"
        ) == 1

        print("PROCESSING LEASE CREATION: PASS")
        print("EXPIRED LEASE RECOVERY: PASS")
        print("RECOVERED WORK RETURNS TO QUEUE: PASS")

        lease_result = controller.run_cycle()

        assert lease_result["processed"] == 1

        assert crawler.expansion_queue.count(
            "processing"
        ) == 0

        assert crawler.expansion_queue.count(
            "complete"
        ) == 5

        print("RECOVERED WORK COMPLETES: PASS")

        # ---------------------------------------------------------
        # 9. FAILURE RECOVERY — REAL CRAWLER PATH
        # ---------------------------------------------------------

        failure_storage = os.path.join(
            temp_dir,
            "failure_crawler",
        )

        failure_crawler = FailureGateCrawler(
            worker_count=1,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=3,
            storage_root=failure_storage,
        )

        failure_url = "https://fail-final.example/"

        failure_crawler.expansion_store.add(
            failure_url
        )

        failure_candidate = (
            failure_crawler.expansion_store.get(
                "fail-final.example"
            )
        )

        assert failure_candidate is not None

        failure_crawler.expansion_queue.enqueue(
            failure_candidate,
            priority=50.0,
        )

        for _ in range(3):
            failure_crawler.expansion_controller.run_cycle()

        assert (
            failure_crawler.attempts[
                "fail-final.example"
            ] == 3
        )

        assert failure_crawler.expansion_queue.count(
            "queued"
        ) == 0

        assert failure_crawler.expansion_queue.count(
            "processing"
        ) == 0

        assert failure_crawler.expansion_queue.count(
            "failed"
        ) == 1

        recovery_status = (
            failure_crawler.expansion_failure_recovery.status()
        )

        assert recovery_status[
            "exhausted_failures"
        ] == 1

        print("REAL CRAWLER FAILURE PATH: PASS")
        print("BOUNDED FAILURE RETRIES: PASS")
        print("FAILURE BUDGET EXHAUSTION: PASS")
        print("PERMANENT FAILURE ISOLATION: PASS")

        # ---------------------------------------------------------
        # 10. FAILURE RESTART PERSISTENCE
        # ---------------------------------------------------------

        failure_crawler.stop()

        restarted_failure_crawler = FailureGateCrawler(
            worker_count=1,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=3,
            storage_root=failure_storage,
        )

        restarted_recovery = (
            restarted_failure_crawler
            .expansion_failure_recovery
            .status()
        )

        assert restarted_recovery[
            "exhausted_failures"
        ] == 1

        restarted_failure_crawler.stop()

        print("FAILURE STATE RESTART PERSISTENCE: PASS")
        print("EXHAUSTED STATE SURVIVES RESTART: PASS")

        # ---------------------------------------------------------
        # 11. QUEUED WORK RESTART PERSISTENCE
        # ---------------------------------------------------------

        restart_storage = os.path.join(
            temp_dir,
            "restart_crawler",
        )

        restart_crawler = FinalGateCrawler(
            worker_count=1,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=3,
            storage_root=restart_storage,
        )

        restart_url = "https://restart-final.example/"

        restart_crawler.expansion_store.add(
            restart_url
        )

        restart_candidate = (
            restart_crawler.expansion_store.get(
                "restart-final.example"
            )
        )

        assert restart_candidate is not None

        restart_crawler.expansion_queue.enqueue(
            restart_candidate,
            priority=50.0,
        )

        assert restart_crawler.expansion_queue.count(
            "queued"
        ) == 1

        restart_crawler.stop()

        restarted = FinalGateCrawler(
            worker_count=1,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=3,
            storage_root=restart_storage,
        )

        assert restarted.expansion_queue.count(
            "queued"
        ) == 1

        print("QUEUED WORK SURVIVES RESTART: PASS")

        restart_result = (
            restarted.expansion_controller.run_cycle()
        )

        assert restart_result["processed"] == 1

        assert restarted.expansion_queue.count(
            "queued"
        ) == 0

        assert restarted.expansion_queue.count(
            "processing"
        ) == 0

        assert restarted.expansion_queue.count(
            "complete"
        ) == 1

        print("RESTARTED WORK COMPLETES: PASS")

        # ---------------------------------------------------------
        # 12. CONTROLLER LIFECYCLE
        # ---------------------------------------------------------

        assert controller.start() is True
        assert controller.start() is False
        assert controller.stop() is True
        assert controller.stop() is False

        print("CONTROLLER START: PASS")
        print("CONTROLLER START IDEMPOTENCY: PASS")
        print("CONTROLLER STOP: PASS")
        print("CONTROLLER STOP IDEMPOTENCY: PASS")

        # ---------------------------------------------------------
        # 13. DISCOVERY LOOP LIFECYCLE
        # ---------------------------------------------------------

        assert discovery_loop.start() is True
        assert discovery_loop.start() is False
        assert discovery_loop.stop() is True
        assert discovery_loop.stop() is False

        print("DISCOVERY LOOP START: PASS")
        print("DISCOVERY LOOP START IDEMPOTENCY: PASS")
        print("DISCOVERY LOOP STOP: PASS")
        print("DISCOVERY LOOP STOP IDEMPOTENCY: PASS")

        # ---------------------------------------------------------
        # 14. FINAL STATUS SURFACES
        # ---------------------------------------------------------

        crawler_status = crawler.status()

        assert "expansion_controller" in crawler_status
        assert "expansion_failure_recovery" in crawler_status

        controller_status = (
            crawler_status["expansion_controller"]
        )

        assert "stats" in controller_status
        assert controller_status["stats"]["cycles"] >= 1

        recovery_status = (
            crawler_status[
                "expansion_failure_recovery"
            ]
        )

        assert "stats" in recovery_status
        assert "exhausted_failures" in recovery_status

        print("CRAWLER STATUS: PASS")
        print("CONTROLLER STATUS: PASS")
        print("RECOVERY STATUS: PASS")

        # ---------------------------------------------------------
        # 15. DISCOVERY LOOP METRICS
        # ---------------------------------------------------------

        loop_status = discovery_loop.status()

        assert loop_status["running"] is False
        assert loop_status["contexts"] == 2

        loop_stats = loop_status["stats"]

        assert loop_stats["cycles"] == 2
        assert loop_stats["discovery_runs"] == 4
        assert loop_stats["discovery_failures"] == 0
        assert loop_stats["discovered"] == 8
        assert loop_stats["accepted"] == 4
        assert loop_stats["stored"] == 4
        assert loop_stats["activation_requested"] == 4
        assert loop_stats["activated"] == 4
        assert loop_stats["activation_failed"] == 0

        print("DISCOVERY LOOP METRICS: PASS")

        # ---------------------------------------------------------
        # 16. SOURCE EXECUTION
        # ---------------------------------------------------------

        assert source.calls == [
            "one.example",
            "two.example",
            "one.example",
            "two.example",
        ]

        print("DISCOVERY SOURCE EXECUTION: PASS")

        # ---------------------------------------------------------
        # 17. DATABASE INTEGRITY
        # ---------------------------------------------------------

        assert_sqlite_integrity(
            os.path.join(
                temp_dir,
                "domain_candidates.db",
            )
        )

        assert_sqlite_integrity(
            os.path.join(
                crawler_storage,
                "expansion_store.db",
            )
        )

        assert_sqlite_integrity(
            os.path.join(
                crawler_storage,
                "expansion_queue.db",
            )
        )

        assert_sqlite_integrity(
            os.path.join(
                crawler_storage,
                "expansion_failure_recovery.db",
            )
        )

        assert_sqlite_integrity(
            feedback_database
        )

        print("DOMAIN DATABASE INTEGRITY: PASS")
        print("EXPANSION STORE DATABASE INTEGRITY: PASS")
        print("EXPANSION QUEUE DATABASE INTEGRITY: PASS")
        print("FAILURE RECOVERY DATABASE INTEGRITY: PASS")
        print("GLOBAL FEEDBACK DATABASE INTEGRITY: PASS")

        # ---------------------------------------------------------
        # 18. FINAL GLOBAL STATE
        # ---------------------------------------------------------

        assert domain_store.count(
            status="activated"
        ) == 4

        assert domain_store.count(
            status="discovered"
        ) == 0

        assert crawler.expansion_queue.count(
            "queued"
        ) == 0

        assert crawler.expansion_queue.count(
            "processing"
        ) == 0

        assert crawler.expansion_queue.count(
            "complete"
        ) == 5

        assert failure_crawler.expansion_queue.count(
            "failed"
        ) == 1

        assert feedback.feedback_count() >= 1

        print("FINAL DOMAIN STATE: PASS")
        print("FINAL EXPANSION STATE: PASS")
        print("FINAL FAILURE STATE: PASS")
        print("FINAL FEEDBACK STATE: PASS")

        # ---------------------------------------------------------
        # 19. CLEAN LIFECYCLE
        # ---------------------------------------------------------

        crawler.stop()
        feedback.close()

        assert crawler.expansion_queue.count(
            "processing"
        ) == 0

        print("CLEAN SHUTDOWN: PASS")

        # ---------------------------------------------------------
        # FINAL GATE
        # ---------------------------------------------------------

        print("========================================")
        print("STAGE 4.7.6 FINAL CONTINUOUS GLOBAL")
        print("EXPANSION GATE")
        print("========================================")
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
