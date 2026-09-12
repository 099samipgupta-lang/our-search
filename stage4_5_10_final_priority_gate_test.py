import os
import tempfile

from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.discovery_priority import DiscoveryPriority
from crawler_system.discovery_priority_feedback import (
    DiscoveryPriorityFeedback,
)
from crawler_system.discovery_priority_feedback_policy import (
    DiscoveryPriorityFeedbackPolicy,
)
from crawler_system.discovery_priority_feedback_store import (
    DiscoveryPriorityFeedbackStore,
)
from crawler_system.discovery_priority_recalculator import (
    DiscoveryPriorityRecalculator,
)
from crawler_system.discovery_priority_update import (
    DiscoveryPriorityUpdater,
)
from crawler_system.discovery_priority_aging import (
    DiscoveryPriorityAging,
)
from crawler_system.discovery_starvation import (
    DiscoveryStarvationPolicy,
)


def check(label, condition):
    print(f"{label}: {'PASS' if condition else 'FAIL'}")
    if not condition:
        raise AssertionError(label)


with tempfile.TemporaryDirectory() as tmp:
    candidate_db = os.path.join(
        tmp,
        "domain_candidates.db",
    )

    feedback_db = os.path.join(
        tmp,
        "feedback.db",
    )

    # Core priority engine.
    priority = DiscoveryPriority()

    candidate = DomainCandidate(
        hostname="final.example",
        url="https://final.example",
        source="certificate_transparency",
        evidence="certificate transparency evidence",
        discovered_at=100.0,
    )

    decision = priority.score_domain_candidate(candidate)

    check(
        "CORE PRIORITY DECISION",
        decision.score > 0,
    )

    check(
        "CORE PRIORITY CATEGORY",
        decision.category == "domain_candidate",
    )

    # Durable candidate store.
    store = DomainCandidateStore(candidate_db)

    check(
        "CANDIDATE STORED",
        store.add(candidate),
    )

    stored = store.get("final.example")

    check(
        "CANDIDATE PRIORITY DURABLE",
        stored["priority"] > 0,
    )

    # Recalculation.
    recalculator = DiscoveryPriorityRecalculator(
        priority_engine=priority.domain_priority,
    )

    check(
        "PRIORITY RECALCULATION",
        recalculator.recalculate_store_candidate(
            store,
            "final.example",
        ),
    )

    # Dynamic update.
    updater = DiscoveryPriorityUpdater(
        store,
        priority_engine=priority.domain_priority,
    )

    updated_candidate = DomainCandidate(
        hostname="final.example",
        url="https://final.example/new",
        source="certificate_transparency",
        evidence="updated evidence",
        discovered_at=100.0,
    )

    check(
        "DYNAMIC PRIORITY UPDATE",
        updater.update_candidate(updated_candidate),
    )

    updated = store.get("final.example")

    check(
        "UPDATED SOURCE",
        updated["source"] == "certificate_transparency",
    )

    check(
        "UPDATED EVIDENCE",
        updated["evidence"] == "updated evidence",
    )

    # Durable feedback.
    feedback_store = DiscoveryPriorityFeedbackStore(
        feedback_db,
    )

    check(
        "FEEDBACK RECORDED",
        feedback_store.record(
            "certificate_transparency",
            success=True,
        ),
    )

    check(
        "FEEDBACK DURABLE",
        feedback_store.score(
            "certificate_transparency"
        ) == 1.0,
    )

    # Feedback -> priority.
    feedback_policy = DiscoveryPriorityFeedbackPolicy(
        feedback=feedback_store.feedback,
        multiplier=1.0,
        max_adjustment=5.0,
    )

    adjusted_priority = feedback_policy.apply(
        updated["priority"],
        "certificate_transparency",
    )

    check(
        "FEEDBACK AFFECTS PRIORITY",
        adjusted_priority > updated["priority"],
    )

    # Aging.
    aging = DiscoveryPriorityAging(
        age_unit_seconds=100.0,
        bonus_per_unit=2.0,
        max_bonus=20.0,
    )

    aged_priority = aging.apply(
        adjusted_priority,
        0.0,
        now=100.0,
    )

    check(
        "AGING AFFECTS PRIORITY",
        aged_priority > adjusted_priority,
    )

    # Starvation prevention.
    starvation = DiscoveryStarvationPolicy(
        aging=aging,
    )

    check(
        "STARVATION PREVENTION",
        starvation.should_overtake(
            waiting_base_priority=54.0,
            waiting_discovered_at=0.0,
            new_base_priority=55.0,
            new_discovered_at=100.0,
            now=100.0,
        ),
    )

    # Deterministic ranking.
    decisions = [
        priority.score_domain_candidate(candidate),
        priority.score_domain_candidate(
            DomainCandidate(
                hostname="low.example",
                url="http://low.example/path?x=1",
                source="link",
                evidence=None,
                discovered_at=101.0,
            )
        ),
    ]

    ranked = DiscoveryPriority.rank(decisions)

    check(
        "GLOBAL RANKING",
        len(ranked) == 2,
    )

    check(
        "RANKING ORDER DETERMINISTIC",
        ranked[0].score >= ranked[1].score,
    )

    # Restart verification.
    reopened_store = DomainCandidateStore(candidate_db)
    reopened_feedback = DiscoveryPriorityFeedbackStore(
        feedback_db,
    )

    check(
        "CANDIDATE SURVIVES RESTART",
        reopened_store.get("final.example") is not None,
    )

    check(
        "FEEDBACK SURVIVES RESTART",
        reopened_feedback.score(
            "certificate_transparency"
        ) == 1.0,
    )

    print("RESULT: PASS")
