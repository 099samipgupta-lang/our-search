import os
import tempfile

from crawler_system.discovery_priority_feedback_store import (
    DiscoveryPriorityFeedbackStore,
)


def check(label, condition):
    print(f"{label}: {'PASS' if condition else 'FAIL'}")
    if not condition:
        raise AssertionError(label)


with tempfile.TemporaryDirectory() as tmp:
    database_path = os.path.join(
        tmp,
        "feedback.db",
    )

    store = DiscoveryPriorityFeedbackStore(
        database_path=database_path,
    )

    check(
        "INITIAL SCORE ZERO",
        store.score("certificate_transparency") == 0.0,
    )

    check(
        "POSITIVE FEEDBACK STORED",
        store.record(
            "certificate_transparency",
            success=True,
        ),
    )

    check(
        "POSITIVE SCORE CORRECT",
        store.score("certificate_transparency") == 1.0,
    )

    check(
        "SECOND SOURCE STORED",
        store.record(
            "sitemap",
            success=True,
        ),
    )

    check(
        "SOURCES INDEPENDENT",
        store.score("sitemap") == 1.0
        and store.score("certificate_transparency") == 1.0,
    )

    check(
        "NEGATIVE FEEDBACK STORED",
        store.record(
            "certificate_transparency",
            success=False,
        ),
    )

    check(
        "NEGATIVE FEEDBACK APPLIED",
        store.score("certificate_transparency") == 0.0,
    )

    reopened = DiscoveryPriorityFeedbackStore(
        database_path=database_path,
    )

    check(
        "FEEDBACK SURVIVES REOPEN",
        reopened.score("certificate_transparency") == 0.0,
    )

    check(
        "SECOND SOURCE SURVIVES REOPEN",
        reopened.score("sitemap") == 1.0,
    )

    for _ in range(100):
        reopened.record(
            "certificate_transparency",
            success=True,
        )

    check(
        "UPPER BOUND SURVIVES",
        reopened.score("certificate_transparency") <= 20.0,
    )

    snapshot = reopened.snapshot()

    check(
        "SNAPSHOT CONTAINS SOURCES",
        "certificate_transparency" in snapshot
        and "sitemap" in snapshot,
    )

print("RESULT: PASS")
