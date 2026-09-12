import os
import tempfile

from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.discovery_priority_update import (
    DiscoveryPriorityUpdater,
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

    updater = DiscoveryPriorityUpdater(store)

    candidate = DomainCandidate(
        hostname="dynamic.example",
        url="https://dynamic.example",
        source="link",
        evidence=None,
        discovered_at=100.0,
    )

    check(
        "INITIAL CANDIDATE STORED",
        store.add(candidate),
    )

    before = store.get("dynamic.example")
    before_priority = before["priority"]

    enriched = DomainCandidate(
        hostname="dynamic.example",
        url="https://dynamic.example",
        source="certificate_transparency",
        evidence="certificate transparency evidence",
        discovered_at=100.0,
    )

    check(
        "DYNAMIC UPDATE SUCCESS",
        updater.update_candidate(enriched),
    )

    after = store.get("dynamic.example")

    check(
        "UPDATED CANDIDATE EXISTS",
        after is not None,
    )

    check(
        "SOURCE UPDATED",
        after["source"] == "certificate_transparency",
    )

    check(
        "EVIDENCE UPDATED",
        after["evidence"] == "certificate transparency evidence",
    )

    check(
        "PRIORITY REFRESHED",
        after["priority"] != before_priority,
    )

    check(
        "UPDATED PRIORITY HIGHER",
        after["priority"] > before_priority,
    )

    check(
        "DIRECT HOSTNAME UPDATE SUCCESS",
        updater.update_hostname("dynamic.example"),
    )

    reopened = DomainCandidateStore(
        database_path=database_path,
    )

    persisted = reopened.get("dynamic.example")

    check(
        "UPDATED PRIORITY PERSISTED",
        persisted["priority"] == after["priority"],
    )

print("RESULT: PASS")
