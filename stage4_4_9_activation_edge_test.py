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
        root / "domain.db"
    )

    expansion_store = ExpansionCandidateStore(
        root / "expansion.db"
    )

    expansion_queue = ExpansionQueue(
        root / "queue.db"
    )

    domain_store.add(
        DomainCandidate(
            hostname="pending.example",
            url="https://pending.example/",
            source="certificate_transparency",
            evidence="ct:test",
            discovered_at=1000.0,
        )
    )

    domain_store.mark_status(
        "pending.example",
        "review",
    )

    activator = DomainCandidateActivator(
        domain_store,
        expansion_store,
        expansion_queue,
    )

    result = activator.activate(
        "pending.example"
    )

    check(
        "NON-ELIGIBLE REJECTED",
        result["success"] is False,
    )

    check(
        "NON-ELIGIBLE STATUS",
        result["status"] == "not_eligible",
    )

    check(
        "NON-ELIGIBLE REMAINS REVIEW",
        domain_store.get("pending.example")["status"]
        == "review",
    )

    check(
        "NON-ELIGIBLE NOT IN EXPANSION STORE",
        expansion_store.get("pending.example")
        is None,
    )

    check(
        "NON-ELIGIBLE NOT QUEUED",
        expansion_queue.count() == 0,
    )

    domain_store.add(
        DomainCandidate(
            hostname="approved.example",
            url="https://approved.example/",
            source="certificate_transparency",
            evidence="ct:test",
            discovered_at=2000.0,
        )
    )

    domain_store.mark_status(
        "approved.example",
        "approved",
    )

    result = activator.activate(
        "approved.example",
        priority=85.0,
    )

    check(
        "APPROVED CANDIDATE ACTIVATES",
        result["success"] is True,
    )

    check(
        "APPROVED ACTIVATION STATUS",
        result["status"] == "activated",
    )

    approved_domain = domain_store.get(
        "approved.example"
    )

    check(
        "APPROVED STATUS ACTIVATED",
        approved_domain["status"] == "activated",
    )

    approved_expansion = expansion_store.get(
        "approved.example"
    )

    check(
        "APPROVED EXPANSION CREATED",
        approved_expansion is not None,
    )

    approved_queue = expansion_queue.next()

    check(
        "APPROVED QUEUED",
        approved_queue is not None,
    )

    check(
        "APPROVED PRIORITY",
        approved_queue["priority"] == 85.0,
    )

    domain_store.add(
        DomainCandidate(
            hostname="invalid-priority.example",
            url="https://invalid-priority.example/",
            source="certificate_transparency",
            evidence="ct:test",
            discovered_at=3000.0,
        )
    )

    result = activator.activate(
        "invalid-priority.example",
        priority=101.0,
    )

    check(
        "INVALID PRIORITY REJECTED",
        result["success"] is False,
    )

    check(
        "INVALID PRIORITY STATUS",
        result["status"] == "invalid_priority",
    )

    check(
        "INVALID PRIORITY NOT ACTIVATED",
        domain_store.get(
            "invalid-priority.example"
        )["status"] == "discovered",
    )

    check(
        "INVALID PRIORITY NOT QUEUED",
        expansion_store.get(
            "invalid-priority.example"
        ) is None,
    )

    check(
        "INVALID PRIORITY QUEUE UNCHANGED",
        expansion_queue.count() == 1,
    )

print("RESULT: PASS")
