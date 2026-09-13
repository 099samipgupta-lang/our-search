import os
import tempfile

from crawler_system.whole_web_crawler import WholeWebCrawler
from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
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
from crawler_system.domain_candidate_activation import (
    DomainCandidateActivator,
)
from crawler_system.continuous_domain_discovery_loop import (
    ContinuousDomainDiscoveryLoop,
)


class IntegrationDiscoverySource:
    name = "integration_source"

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
                evidence=f"integration:{seed}",
            ),
            DomainCandidate(
                hostname=f"beta-{seed}",
                url=f"https://beta-{seed}/",
                source=self.name,
                evidence=f"integration:{seed}",
            ),
        }


class IntegrationCrawler(WholeWebCrawler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.integration_added_urls = []

    def _add_url(
        self,
        url,
        source="unknown",
        depth=0,
        seed=False,
        source_url=None,
    ):
        self.integration_added_urls.append(
            {
                "url": url,
                "source": source,
                "depth": depth,
                "seed": seed,
                "source_url": source_url,
            }
        )

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

    source = IntegrationDiscoverySource()

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

        crawler = IntegrationCrawler(
            worker_count=1,
            frontier_delay=0,
            task_timeout=10,
            max_attempts=1,
            storage_root=crawler_storage,
        )

        activator = DomainCandidateActivator(
            domain_store=domain_store,
            expansion_store=crawler.expansion_store,
            expansion_queue=crawler.expansion_queue,
        )

        # ---------------------------------------------------------
        # 1. REAL CONTINUOUS DOMAIN DISCOVERY LOOP
        # ---------------------------------------------------------

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
            cycle_interval=0.1,
            max_contexts_per_cycle=2,
            activation_batch_size=100,
        )

        first_cycle = discovery_loop.run_cycle()

        assert first_cycle["discovery"]["contexts"] == 2
        assert first_cycle["discovery"]["discovered"] == 4
        assert first_cycle["discovery"]["accepted"] == 4
        assert first_cycle["discovery"]["stored"] == 4
        assert first_cycle["discovery"]["duplicates"] == 0
        assert first_cycle["discovery"]["failures"] == 0

        print("CONTINUOUS DISCOVERY LOOP: PASS")
        print("DISCOVERY → PIPELINE: PASS")
        print("PIPELINE → DURABLE STORE: PASS")

        # ---------------------------------------------------------
        # 2. REAL DOMAIN ACTIVATION
        # ---------------------------------------------------------

        assert first_cycle["activation"]["requested"] == 4
        assert first_cycle["activation"]["activated"] == 4
        assert first_cycle["activation"]["failed"] == 0

        assert domain_store.count(
            status="activated"
        ) == 4

        print("STORE → ACTIVATION: PASS")
        print("DOMAIN ACTIVATION: PASS")
        print("ACTIVATED DOMAIN PERSISTENCE: PASS")

        # ---------------------------------------------------------
        # 3. ACTIVATION → EXPANSION CANDIDATE STORE
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

        assert all(
            candidate.get("hostname")
            for candidate in expansion_candidates
        )

        assert all(
            candidate.get("first_url")
            for candidate in expansion_candidates
        )

        print("ACTIVATION → EXPANSION STORE: PASS")

        # ---------------------------------------------------------
        # 4. ACTIVATION → REAL EXPANSION QUEUE
        # ---------------------------------------------------------

        assert crawler.expansion_queue.count(
            "queued"
        ) == 4

        print("ACTIVATION → EXPANSION QUEUE: PASS")

        # ---------------------------------------------------------
        # 5. VERIFY EXPANSION CANDIDATE DATA
        # ---------------------------------------------------------
        #
        # ExpansionCandidateStore intentionally uses:
        #
        #     hostname
        #     first_url
        #     source_hostname
        #     discovered_at
        #     status
        #
        # It does NOT have a "source" field.
        #
        # Therefore source propagation is verified through the
        # actual domain discovery source and resulting URLs rather
        # than by reading a nonexistent expansion-store field.
        # ---------------------------------------------------------

        assert source.calls == [
            "one.example",
            "two.example",
        ]

        assert all(
            candidate["hostname"].startswith(
                ("alpha-", "beta-")
            )
            for candidate in expansion_candidates
        )

        print("DISCOVERY SOURCE PROPAGATION: PASS")

        # ---------------------------------------------------------
        # 6. REAL WHOLEWEBCRAWLER EXPANSION PROCESSOR
        # ---------------------------------------------------------
        #
        # This uses the exact production processor through the
        # production ContinuousExpansionController.
        #
        # No fake queue.
        # No injected expansion queue.
        # No injected expansion store.
        # ---------------------------------------------------------

        controller = crawler.expansion_controller

        assert controller is not None

        controller_result = controller.run_cycle()

        assert controller_result["processed"] == 4

        assert len(
            crawler.integration_added_urls
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

        print("CONTINUOUS EXPANSION CONTROLLER: PASS")
        print("EXPANSION PROCESSOR: PASS")
        print("EXPANSION QUEUE DRAIN: PASS")

        # ---------------------------------------------------------
        # 7. VERIFY REAL CRAWLER URL PROPAGATION
        # ---------------------------------------------------------

        added_urls = {
            item["url"]
            for item in crawler.integration_added_urls
        }

        expected_urls = {
            candidate["first_url"]
            for candidate in expansion_candidates
        }

        assert added_urls == expected_urls

        assert all(
            item["source"] == "expansion"
            for item in crawler.integration_added_urls
        )

        assert all(
            item["depth"] == 0
            for item in crawler.integration_added_urls
        )

        assert all(
            item["seed"] is False
            for item in crawler.integration_added_urls
        )

        print("EXPANSION → CRAWLER URL PROPAGATION: PASS")

        # ---------------------------------------------------------
        # 8. SECOND CONTINUOUS DISCOVERY CYCLE
        # ---------------------------------------------------------
        #
        # The same discovery contexts are executed again.
        #
        # The source produces the same four logical candidates.
        # The candidate pipeline must deduplicate them.
        # No new domain records should be stored.
        # No new activation should occur.
        # No duplicate expansion queue entries should appear.
        # ---------------------------------------------------------

        second_cycle = discovery_loop.run_cycle()

        assert second_cycle["discovery"]["contexts"] == 2
        assert second_cycle["discovery"]["discovered"] == 4
        assert second_cycle["discovery"]["accepted"] == 0
        assert second_cycle["discovery"]["stored"] == 0
        assert second_cycle["discovery"]["duplicates"] == 0
        assert second_cycle["discovery"]["failures"] == 0

        print("CROSS-CYCLE DOMAIN DEDUPLICATION: PASS")
        print("NO DUPLICATE DOMAIN STORAGE: PASS")

        # ---------------------------------------------------------
        # 9. NO DUPLICATE ACTIVATION
        # ---------------------------------------------------------

        assert second_cycle["activation"]["requested"] == 0
        assert second_cycle["activation"]["activated"] == 0
        assert second_cycle["activation"]["failed"] == 0

        assert domain_store.count(
            status="discovered"
        ) == 0

        assert domain_store.count(
            status="activated"
        ) == 4

        print("NO DUPLICATE ACTIVATION: PASS")

        # ---------------------------------------------------------
        # 10. NO DUPLICATE EXPANSION QUEUING
        # ---------------------------------------------------------

        assert crawler.expansion_queue.count(
            "queued"
        ) == 0

        assert crawler.expansion_queue.count(
            "processing"
        ) == 0

        assert crawler.expansion_queue.count(
            "complete"
        ) == 4

        assert len(
            crawler.integration_added_urls
        ) == 4

        print("NO DUPLICATE EXPANSION QUEUING: PASS")

        # ---------------------------------------------------------
        # 11. CONTINUOUS DISCOVERY LOOP LIFECYCLE
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
        # 13. DISCOVERY LOOP STATUS / METRICS
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
        assert loop_stats["duplicates"] == 0
        assert loop_stats["activation_batches"] == 1
        assert loop_stats["activation_requested"] == 4
        assert loop_stats["activated"] == 4
        assert loop_stats["activation_failed"] == 0
        assert loop_stats["already_active"] == 0

        print("DISCOVERY LOOP METRICS: PASS")

        # ---------------------------------------------------------
        # 14. EXPANSION CONTROLLER STATUS / METRICS
        # ---------------------------------------------------------

        controller_status = controller.status()

        assert "stats" in controller_status

        controller_stats = controller_status["stats"]

        assert controller_stats["cycles"] >= 1
        assert controller_stats["last_processed"] == 4

        print("EXPANSION CONTROLLER METRICS: PASS")

        # ---------------------------------------------------------
        # 15. SOURCE EXECUTION
        # ---------------------------------------------------------

        assert source.calls == [
            "one.example",
            "two.example",
            "one.example",
            "two.example",
        ]

        print("DISCOVERY SOURCE EXECUTION: PASS")

        # ---------------------------------------------------------
        # 16. FINAL DOMAIN STATE
        # ---------------------------------------------------------

        assert domain_store.count(
            status="activated"
        ) == 4

        assert domain_store.count(
            status="discovered"
        ) == 0

        print("FINAL DOMAIN STATE: PASS")

        # ---------------------------------------------------------
        # 17. FINAL EXPANSION STATE
        # ---------------------------------------------------------

        assert crawler.expansion_store.count(
            status="discovered"
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

        assert len(
            crawler.integration_added_urls
        ) == 4

        print("FINAL EXPANSION STATE: PASS")

        # ---------------------------------------------------------
        # 18. FINAL RESULT
        # ---------------------------------------------------------

        print("RESULT: PASS")


if __name__ == "__main__":
    main()
