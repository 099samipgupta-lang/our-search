import os
import tempfile

from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
)
from crawler_system.domain_candidate_store import (
    DomainCandidateStore,
)
from crawler_system.domain_discovery_pipeline import (
    DomainDiscoveryPipeline,
)


class TestSource:
    name = "test_source"

    def can_discover(self, context):
        return True

    def discover(self, context):
        return {
            DomainCandidate(
                hostname="Example.COM.",
                url="https://Example.COM/",
                source="certificate_transparency",
                evidence="ct-example",
                discovered_at=100.0,
                metadata={"test": "one"},
            ),
            DomainCandidate(
                hostname="SECOND.Example.ORG.",
                url="https://SECOND.Example.ORG/",
                source="registry",
                evidence="registry-example",
                discovered_at=101.0,
                metadata={"test": "two"},
            ),
            DomainCandidate(
                hostname="example.com",
                url="https://example.com/",
                source="another_source",
                evidence="duplicate",
                discovered_at=102.0,
            ),
        }


def check(name, condition):
    if not condition:
        raise AssertionError(name)
    print(f"{name}: PASS")


with tempfile.TemporaryDirectory() as directory:
    database_path = os.path.join(
        directory,
        "domain_candidates.db",
    )

    registry = DomainDiscoverySourceRegistry()
    registry.register(TestSource())

    store = DomainCandidateStore(database_path)

    # ---------------------------------------------------------
    # 1. Pure discovery + pipeline processing
    # ---------------------------------------------------------

    discovery_pipeline = DomainDiscoveryPipeline(
        registry=registry,
        store=store,
    )

    candidates = discovery_pipeline.discover(
        DomainDiscoveryContext(
            seed_domain="example.com",
        )
    )

    check(
        "DISCOVERY EXECUTED",
        len(candidates) == 2,
    )

    hostnames = {
        candidate.hostname
        for candidate in candidates
    }

    check(
        "NORMALIZED HOSTNAME ONE",
        "example.com" in hostnames,
    )

    check(
        "NORMALIZED HOSTNAME TWO",
        "second.example.org" in hostnames,
    )


    # ---------------------------------------------------------
    # 2. Discovery provenance
    # ---------------------------------------------------------

    example_candidate = next(
        candidate
        for candidate in candidates
        if candidate.hostname == "example.com"
    )

    check(
        "PROVENANCE SOURCE PRESERVED",
        example_candidate.source
        == "certificate_transparency",
    )

    check(
        "PROVENANCE EVIDENCE PRESERVED",
        example_candidate.evidence
        == "ct-example",
    )

    check(
        "PROVENANCE METADATA PRESERVED",
        example_candidate.metadata["test"] == "one",
    )


    # ---------------------------------------------------------
    # 3. Fresh pipeline for discovery + storage
    # ---------------------------------------------------------

    storage_pipeline = DomainDiscoveryPipeline(
        registry=registry,
        store=store,
    )

    result = storage_pipeline.discover_and_store(
        DomainDiscoveryContext(
            seed_domain="example.com",
        )
    )

    check(
        "DISCOVERED COUNT",
        result["discovered"] == 3,
    )

    check(
        "ACCEPTED UNIQUE COUNT",
        result["accepted"] == 2,
    )

    check(
        "STORED COUNT",
        result["stored"] == 2,
    )

    check(
        "DUPLICATE STORE COUNT",
        result["duplicates"] == 0,
    )

    check(
        "DURABLE STORE COUNT",
        store.count() == 2,
    )


    # ---------------------------------------------------------
    # 4. Stored records contain priority
    # ---------------------------------------------------------

    stored = storage_pipeline.stored_candidates(
        status="discovered",
        limit=10,
    )

    check(
        "STORED CANDIDATE LIST",
        len(stored) == 2,
    )

    check(
        "PRIORITY PERSISTED",
        all(
            candidate["priority"] > 0
            for candidate in stored
        ),
    )

    check(
        "PRIORITY ORDERING",
        stored[0]["priority"]
        >= stored[1]["priority"],
    )


    # ---------------------------------------------------------
    # 5. Repeated discovery is deduplicated by pipeline state
    # ---------------------------------------------------------

    repeated = storage_pipeline.discover_and_store(
        DomainDiscoveryContext(
            seed_domain="example.com",
        )
    )

    check(
        "REPEATED DISCOVERY COUNT",
        repeated["discovered"] == 3,
    )

    check(
        "REPEATED DISCOVERY ACCEPTED ZERO",
        repeated["accepted"] == 0,
    )

    check(
        "REPEATED DISCOVERY STORED ZERO",
        repeated["stored"] == 0,
    )

    check(
        "STORE STILL TWO",
        store.count() == 2,
    )


    # ---------------------------------------------------------
    # 6. External candidate processing
    # ---------------------------------------------------------

    external_pipeline = DomainDiscoveryPipeline(
        registry=registry,
        store=store,
    )

    external = [
        DomainCandidate(
            hostname="Third.Example.NET.",
            url="https://Third.Example.NET/",
            source="external_registry",
            evidence="external-record",
        ),
        DomainCandidate(
            hostname="third.example.net",
            url="https://third.example.net/",
            source="duplicate_source",
            evidence="duplicate-record",
        ),
    ]

    processed = external_pipeline.process_candidates(
        external
    )

    check(
        "EXTERNAL CANDIDATE NORMALIZATION",
        len(processed) == 1,
    )

    check(
        "EXTERNAL HOSTNAME",
        next(iter(processed)).hostname
        == "third.example.net",
    )


    # ---------------------------------------------------------
    # 7. External processing + storage
    # ---------------------------------------------------------

    external_result = external_pipeline.process_and_store(
        external
    )

    check(
        "EXTERNAL INPUT COUNT",
        external_result["input"] == 2,
    )

    check(
        "EXTERNAL ACCEPTED COUNT",
        external_result["accepted"] == 0,
    )

    check(
        "EXTERNAL STORED COUNT",
        external_result["stored"] == 0,
    )

    # The previous process_candidates() call already accepted
    # the external hostname in this pipeline instance.
    check(
        "FINAL STORE COUNT BEFORE EXTERNAL INSERT",
        external_pipeline.stored_count() == 2,
    )


    # ---------------------------------------------------------
    # 8. Fresh external pipeline stores candidate
    # ---------------------------------------------------------

    fresh_external_pipeline = DomainDiscoveryPipeline(
        registry=registry,
        store=store,
    )

    fresh_external_result = (
        fresh_external_pipeline.process_and_store(
            external
        )
    )

    check(
        "FRESH EXTERNAL ACCEPTED COUNT",
        fresh_external_result["accepted"] == 1,
    )

    check(
        "FRESH EXTERNAL STORED COUNT",
        fresh_external_result["stored"] == 1,
    )

    check(
        "FINAL STORE COUNT",
        fresh_external_pipeline.stored_count() == 3,
    )


    # ---------------------------------------------------------
    # 9. Persistence through a new store instance
    # ---------------------------------------------------------

    reopened = DomainCandidateStore(
        database_path
    )

    check(
        "PERSISTENT PIPELINE DATA",
        reopened.count() == 3,
    )

    persisted = reopened.get(
        "third.example.net"
    )

    check(
        "PERSISTENT EXTERNAL CANDIDATE",
        persisted is not None,
    )

    check(
        "PERSISTENT PRIORITY",
        persisted["priority"] > 0,
    )


print("RESULT: PASS")
