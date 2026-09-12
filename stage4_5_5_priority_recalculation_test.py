import os
import tempfile

from crawler_system.domain_candidate_priority import DomainCandidatePriority
from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.discovery_priority_recalculator import (
    DiscoveryPriorityRecalculator,
)


def check(label, condition):
    print(f"{label}: {'PASS' if condition else 'FAIL'}")
    if not condition:
        raise AssertionError(label)


with tempfile.TemporaryDirectory() as tmp:
    database_path = os.path.join(
        tmp,
        "domain_candidates.db",
    )

    store = DomainCandidateStore(
        database_path=database_path,
    )

    candidate = DomainCandidate(
        hostname="recalculate.example",
        url="https://recalculate.example",
        source="link",
        evidence="https://source.example/page",
        discovered_at=100.0,
    )

    check(
        "CANDIDATE STORED",
        store.add(candidate),
    )

    recalculator = DiscoveryPriorityRecalculator()

    original = store.get(
        "recalculate.example",
    )

    original_priority = original["priority"]

    check(
        "ORIGINAL PRIORITY EXISTS",
        original_priority > 0,
    )

    check(
        "RECALCULATION SUCCESS",
        recalculator.recalculate_store_candidate(
            store,
            "recalculate.example",
        ),
    )

    recalculated = store.get(
        "recalculate.example",
    )

    expected = DomainCandidatePriority().score(
        DomainCandidate(
            hostname=recalculated["hostname"],
            url=recalculated["url"],
            source=recalculated["source"],
            evidence=recalculated["evidence"],
            discovered_at=recalculated["discovered_at"],
            metadata=recalculated["metadata"],
        )
    )

    check(
        "RECALCULATED PRIORITY MATCHES ENGINE",
        recalculated["priority"] == expected,
    )

    # Recalculation must survive reopening the durable store.
    reopened = DomainCandidateStore(
        database_path=database_path,
    )

    persisted = reopened.get(
        "recalculate.example",
    )

    check(
        "RECALCULATED PRIORITY PERSISTED",
        persisted["priority"] == expected,
    )

    check(
        "MISSING CANDIDATE SAFE",
        recalculator.recalculate_store_candidate(
            reopened,
            "missing.example",
        ) is False,
    )

print("RESULT: PASS")
