import os
import tempfile

from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.domain_candidate_provenance import (
    DomainCandidateProvenance,
)
from crawler_system.domain_candidate_store import (
    DomainCandidateStore,
)


def check(name, condition):
    if not condition:
        raise AssertionError(name)
    print(f"{name}: PASS")


with tempfile.TemporaryDirectory() as directory:
    database_path = os.path.join(
        directory,
        "domain_candidates.db",
    )

    store = DomainCandidateStore(database_path)

    candidate = DomainCandidate(
        hostname="Example.COM.",
        url="https://Example.COM/",
        source="certificate_transparency",
        evidence="ct-response",
        discovered_at=123.0,
        metadata={
            "discovery_method": "certificate_transparency",
            "endpoint": "test-endpoint",
        },
    )

    # ---------------------------------------------------------
    # 1. Candidate insertion
    # ---------------------------------------------------------

    check(
        "CANDIDATE INSERT",
        store.add(candidate),
    )

    check(
        "TOTAL COUNT",
        store.count() == 1,
    )

    check(
        "DISCOVERED COUNT",
        store.count("discovered") == 1,
    )


    # ---------------------------------------------------------
    # 2. Duplicate protection
    # ---------------------------------------------------------

    duplicate = DomainCandidate(
        hostname="example.com",
        url="https://example.com/",
        source="another_source",
        evidence="another-evidence",
    )

    check(
        "DUPLICATE REJECTED",
        not store.add(duplicate),
    )

    check(
        "COUNT AFTER DUPLICATE",
        store.count() == 1,
    )


    # ---------------------------------------------------------
    # 3. Retrieval
    # ---------------------------------------------------------

    stored = store.get("EXAMPLE.COM.")

    check(
        "CANDIDATE RETRIEVAL",
        stored is not None,
    )

    check(
        "HOSTNAME NORMALIZATION",
        stored["hostname"] == "example.com",
    )

    check(
        "URL PRESERVED",
        stored["url"] == "https://Example.COM/",
    )

    check(
        "SOURCE PRESERVED",
        stored["source"] == "certificate_transparency",
    )

    check(
        "EVIDENCE PRESERVED",
        stored["evidence"] == "ct-response",
    )

    check(
        "TIMESTAMP PRESERVED",
        stored["discovered_at"] == 123.0,
    )

    check(
        "METADATA PRESERVED",
        stored["metadata"]["discovery_method"]
        == "certificate_transparency",
    )


    # ---------------------------------------------------------
    # 4. Candidate listing
    # ---------------------------------------------------------

    candidates = store.list_candidates(
        status="discovered",
        limit=10,
    )

    check(
        "LIST CANDIDATES",
        len(candidates) == 1,
    )

    check(
        "LIST HOSTNAME",
        candidates[0]["hostname"] == "example.com",
    )


    # ---------------------------------------------------------
    # 5. Status update
    # ---------------------------------------------------------

    check(
        "STATUS UPDATE",
        store.mark_status(
            "EXAMPLE.COM.",
            "queued",
        ),
    )

    check(
        "QUEUED COUNT",
        store.count("queued") == 1,
    )

    check(
        "DISCOVERED COUNT AFTER STATUS",
        store.count("discovered") == 0,
    )


    # ---------------------------------------------------------
    # 6. Separate provenance insertion
    # ---------------------------------------------------------

    provenance = DomainCandidateProvenance(
        source="test_registry",
        evidence="registry-record",
        discovered_at=456.0,
        metadata={
            "registry": "test",
        },
    )

    check(
        "PROVENANCE INSERT",
        store.add_provenance(
            "Second.Example.",
            "https://second.example/",
            provenance,
        ),
    )

    check(
        "TOTAL COUNT AFTER PROVENANCE",
        store.count() == 2,
    )

    second = store.get("second.example")

    check(
        "PROVENANCE RETRIEVAL",
        second is not None,
    )

    check(
        "PROVENANCE SOURCE",
        second["source"] == "test_registry",
    )

    check(
        "PROVENANCE EVIDENCE",
        second["evidence"] == "registry-record",
    )

    check(
        "PROVENANCE METADATA",
        second["metadata"]["registry"] == "test",
    )


    # ---------------------------------------------------------
    # 7. Persistence across store instances
    # ---------------------------------------------------------

    reopened = DomainCandidateStore(database_path)

    check(
        "PERSISTENT TOTAL COUNT",
        reopened.count() == 2,
    )

    persisted = reopened.get("example.com")

    check(
        "PERSISTENT RETRIEVAL",
        persisted is not None,
    )

    check(
        "PERSISTENT SOURCE",
        persisted["source"]
        == "certificate_transparency",
    )

    check(
        "PERSISTENT METADATA",
        persisted["metadata"]["endpoint"]
        == "test-endpoint",
    )

    check(
        "PERSISTENT STATUS",
        persisted["status"] == "queued",
    )


    # ---------------------------------------------------------
    # 8. Invalid input rejection
    # ---------------------------------------------------------

    check(
        "INVALID CANDIDATE REJECTED",
        not reopened.add(None),
    )

    check(
        "INVALID PROVENANCE HOST REJECTED",
        not reopened.add_provenance(
            "",
            "https://invalid.example/",
            provenance,
        ),
    )


    # ---------------------------------------------------------
    # 9. Clear
    # ---------------------------------------------------------

    reopened.clear()

    check(
        "STORE CLEAR",
        reopened.count() == 0,
    )


print("RESULT: PASS")
