import os
import tempfile

from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_discovery_sources import DomainCandidate


def check(label, condition):
    print(f"{label}: {'PASS' if condition else 'FAIL'}")
    if not condition:
        raise AssertionError(label)


with tempfile.TemporaryDirectory() as tmp:
    database_path = os.path.join(tmp, "domain_candidates.db")

    store = DomainCandidateStore(database_path=database_path)

    low = DomainCandidate(
        hostname="low.example",
        url="https://low.example",
        source="link",
        evidence="https://source.example/page",
        discovered_at=100.0,
    )

    high = DomainCandidate(
        hostname="high.example",
        url="https://high.example",
        source="certificate_transparency",
        evidence="https://crt.sh/?q=%25&output=json",
        discovered_at=200.0,
    )

    check("LOW CANDIDATE STORED", store.add(low))
    check("HIGH CANDIDATE STORED", store.add(high))

    ordered = store.list_candidates(
        status="discovered",
        limit=10,
    )

    check("TWO CANDIDATES RETURNED", len(ordered) == 2)
    check("HIGH PRIORITY FIRST", ordered[0]["hostname"] == "high.example")
    check("LOW PRIORITY SECOND", ordered[1]["hostname"] == "low.example")
    check("HIGH PRIORITY GREATER", ordered[0]["priority"] > ordered[1]["priority"])

    first_priority = ordered[0]["priority"]

    # Explicit priority update must immediately change durable ordering.
    check(
        "PRIORITY UPDATE SUCCESS",
        store.update_priority("low.example", 99.0),
    )

    reordered = store.list_candidates(
        status="discovered",
        limit=10,
    )

    check(
        "UPDATED HIGH PRIORITY FIRST",
        reordered[0]["hostname"] == "low.example",
    )

    check(
        "UPDATED PRIORITY PERSISTED",
        reordered[0]["priority"] == 99.0,
    )

    # Re-open the database to prove the ordering survives a new process/store.
    reopened = DomainCandidateStore(database_path=database_path)

    persisted = reopened.list_candidates(
        status="discovered",
        limit=10,
    )

    check(
        "PRIORITY SURVIVES REOPEN",
        persisted[0]["hostname"] == "low.example",
    )

    check(
        "PRIORITY VALUE SURVIVES REOPEN",
        persisted[0]["priority"] == 99.0,
    )

    # Equal priority must fall back deterministically to discovery time.
    check(
        "SECOND PRIORITY UPDATE",
        reopened.update_priority("high.example", 99.0),
    )

    tie_order = reopened.list_candidates(
        status="discovered",
        limit=10,
    )

    check(
        "TIE BREAK BY DISCOVERY TIME",
        tie_order[0]["hostname"] == "low.example",
    )

    check(
        "ORIGINAL PRIORITY CAPTURED",
        first_priority > 0,
    )

print("RESULT: PASS")
