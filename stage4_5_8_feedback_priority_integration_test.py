from crawler_system.discovery_priority_feedback import (
    DiscoveryPriorityFeedback,
)
from crawler_system.discovery_priority_feedback_policy import (
    DiscoveryPriorityFeedbackPolicy,
)


def check(label, condition):
    print(f"{label}: {'PASS' if condition else 'FAIL'}")
    if not condition:
        raise AssertionError(label)


feedback = DiscoveryPriorityFeedback(
    positive_weight=2.0,
    negative_weight=1.0,
    max_feedback=10.0,
)

policy = DiscoveryPriorityFeedbackPolicy(
    feedback=feedback,
    multiplier=1.0,
    max_adjustment=5.0,
)

base = 50.0

check(
    "INITIAL ADJUSTMENT ZERO",
    policy.adjustment("link") == 0.0,
)

check(
    "INITIAL PRIORITY UNCHANGED",
    policy.apply(base, "link") == 50.0,
)

feedback.record("link", success=True)

check(
    "POSITIVE FEEDBACK ADJUSTMENT",
    policy.adjustment("link") == 2.0,
)

check(
    "POSITIVE FEEDBACK RAISES PRIORITY",
    policy.apply(base, "link") == 52.0,
)

feedback.record("link", success=False)

check(
    "NEGATIVE FEEDBACK RECORDED",
    policy.adjustment("link") == 1.0,
)

check(
    "NEGATIVE OUTCOME REDUCES BENEFIT",
    policy.apply(base, "link") == 51.0,
)

for _ in range(20):
    feedback.record("link", success=True)

check(
    "ADJUSTMENT UPPER BOUNDED",
    policy.adjustment("link") == 5.0,
)

for _ in range(50):
    feedback.record("link", success=False)

check(
    "ADJUSTMENT LOWER BOUNDED",
    policy.adjustment("link") == -5.0,
)

check(
    "FINAL PRIORITY LOWER BOUNDED",
    policy.apply(base, "link") == 45.0,
)

check(
    "PRIORITY UPPER BOUNDED",
    policy.apply(99.0, "unknown") <= 100.0,
)

check(
    "INVALID BASE SAFE",
    policy.apply("invalid", "link") == 0.0,
)

print("RESULT: PASS")
