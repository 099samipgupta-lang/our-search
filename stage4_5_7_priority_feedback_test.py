from crawler_system.discovery_priority_feedback import (
    DiscoveryPriorityFeedback,
)


def check(label, condition):
    print(f"{label}: {'PASS' if condition else 'FAIL'}")
    if not condition:
        raise AssertionError(label)


feedback = DiscoveryPriorityFeedback(
    positive_weight=2.0,
    negative_weight=1.0,
    max_feedback=5.0,
)

check(
    "INITIAL FEEDBACK ZERO",
    feedback.score("certificate_transparency") == 0.0,
)

check(
    "POSITIVE OUTCOME RECORDED",
    feedback.record(
        "certificate_transparency",
        success=True,
    ),
)

check(
    "POSITIVE FEEDBACK INCREASED",
    feedback.score("certificate_transparency") == 2.0,
)

check(
    "SECOND POSITIVE OUTCOME RECORDED",
    feedback.record(
        "certificate_transparency",
        success=True,
    ),
)

check(
    "POSITIVE FEEDBACK ACCUMULATED",
    feedback.score("certificate_transparency") == 4.0,
)

check(
    "NEGATIVE OUTCOME RECORDED",
    feedback.record(
        "certificate_transparency",
        success=False,
    ),
)

check(
    "NEGATIVE FEEDBACK REDUCES SCORE",
    feedback.score("certificate_transparency") == 3.0,
)

for _ in range(10):
    feedback.record(
        "certificate_transparency",
        success=True,
    )

check(
    "FEEDBACK UPPER BOUND",
    feedback.score("certificate_transparency") == 5.0,
)

for _ in range(20):
    feedback.record(
        "certificate_transparency",
        success=False,
    )

check(
    "FEEDBACK LOWER BOUND",
    feedback.score("certificate_transparency") == -5.0,
)

check(
    "INVALID SOURCE SAFE",
    feedback.record(
        "",
        success=True,
    ) is False,
)

check(
    "INVALID SUCCESS SAFE",
    feedback.record(
        "certificate_transparency",
        success=1,
    ) is False,
)

snapshot = feedback.snapshot()

check(
    "SNAPSHOT RETURNS DATA",
    snapshot["certificate_transparency"] == -5.0,
)

check(
    "RESET SOURCE SUCCESS",
    feedback.reset("certificate_transparency"),
)

check(
    "RESET SOURCE CLEARS SCORE",
    feedback.score("certificate_transparency") == 0.0,
)

check(
    "RESET ALL SUCCESS",
    feedback.reset(),
)

print("RESULT: PASS")
