import tempfile
from pathlib import Path

from crawler_system.domain_candidate_activation import (
    DomainCandidateActivator,
)
from crawler_system.domain_candidate_store import (
    DomainCandidateStore,
)
from crawler_system.domain_discovery_sources import (
    DomainCandidate,
)
from crawler_system.expansion_store import (
    ExpansionCandidateStore,
)
from crawler_system.expansion_queue import (
    ExpansionQueue,
)


def check(name, condition):
    if not condition:
        raise AssertionError(name)
    print(f"{name}: PASS")


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)

    domain_store = DomainCandidateStore(
        root / "domain_candidates.db"
    )

    expansion_store = ExpansionCandidateStore(
        root / "expansion_candidates.db"
    )

    expansion_queue = ExpansionQueue(
        root / "expansion_queue.db"
    )

    candidate = DomainCandidate(
        hostname="example.com",
        url="https://example.com/",
        source="certificate_transparency",
        evidence="https://crt.example/test",
        discovered_at=1000.0,
        metadata={
            "discovery_method": "certificate_transparency"
        },
    )

    inserted = domain_store.add(candidate)

    check(
        "DOMAIN CANDIDATE STORED",
        inserted is True,
    )

    stored = domain_store.get("example.com")

    check(
        "DOMAIN CANDIDATE RETRIEVED",
        stored is not None,
    )

    check(
        "INITIAL STATUS DISCOVERED",
        stored["status"] == "discovered",
    )

    check(
        "DOMAIN PRIORITY STORED",
        0.0 <= stored["priority"] <= 100.0,
    )

    activator = DomainCandidateActivator(
        domain_store=domain_store,
        expansion_store=expansion_store,
        expansion_queue=expansion_queue,
    )

    result = activator.activate(
        "example.com"
    )

    check(
        "ACTIVATION SUCCESS",
        result["success"] is True,
    )

    check(
        "ACTIVATION STATUS",
        result["status"] == "activated",
    )

    check(
        "ACTIVATION QUEUED",
        result["queued"] is True,
    )

    domain_after = domain_store.get(
        "example.com"
    )

    check(
        "DOMAIN STATUS ACTIVATED",
        domain_after["status"] == "activated",
    )

    expansion_candidate = expansion_store.get(
        "example.com"
    )

    check(
        "EXPANSION CANDIDATE CREATED",
        expansion_candidate is not None,
    )

    check(
        "EXPANSION URL PRESERVED",
        expansion_candidate["first_url"]
        == "https://example.com/",
    )

    queued = expansion_queue.next()

    check(
        "EXPANSION QUEUE ENTRY CREATED",
        queued is not None,
    )

    check(
        "QUEUE HOSTNAME",
        queued["hostname"] == "example.com",
    )

    check(
        "QUEUE URL",
        queued["first_url"]
        == "https://example.com/",
    )

    check(
        "QUEUE PRIORITY PRESERVED",
        queued["priority"]
        == domain_after["priority"],
    )

    repeated = activator.activate(
        "example.com"
    )

    check(
        "REPEATED ACTIVATION SAFE",
        repeated["success"] is True,
    )

    check(
        "REPEATED ACTIVATION DETECTED",
        repeated["status"] == "already_activated",
    )

    check(
        "QUEUE STILL ONE ENTRY",
        expansion_queue.count() == 1,
    )

    missing = activator.activate(
        "missing.example"
    )

    check(
        "MISSING CANDIDATE REJECTED",
        missing["success"] is False,
    )

    check(
        "MISSING CANDIDATE STATUS",
        missing["status"] == "not_found",
    )

    invalid = activator.activate(
        ""
    )

    check(
        "INVALID HOSTNAME REJECTED",
        invalid["success"] is False,
    )

    check(
        "INVALID HOSTNAME STATUS",
        invalid["status"] == "invalid_hostname",
    )

print("RESULT: PASS")
