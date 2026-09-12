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


aging = DiscoveryPriorityAging(
    age_unit_seconds=100.0,
    bonus_per_unit=5.0,
    max_bonus=20.0,
)

policy = DiscoveryStarvationPolicy(aging=aging)

now = 1000.0

# Candidate waiting for a long time.
waiting_priority = policy.effective_priority(
    base_priority=50.0,
    discovered_at=0.0,
    now=now,
)

check(
    "WAITING CANDIDATE GAINS PRIORITY",
    waiting_priority == 70.0,
)

# A newly discovered candidate has a higher raw score.
new_priority = policy.effective_priority(
    base_priority=65.0,
    discovered_at=1000.0,
    now=now,
)

check(
    "NEW CANDIDATE STARTS HIGHER",
    new_priority == 65.0,
)

check(
    "WAITING CANDIDATE OVERTAKES",
    policy.should_overtake(
        waiting_base_priority=50.0,
        waiting_discovered_at=0.0,
        new_base_priority=65.0,
        new_discovered_at=1000.0,
        now=now,
    ),
)

# A fresh candidate that is substantially stronger should still win.
check(
    "STRONG NEW CANDIDATE REMAINS AHEAD",
    not policy.should_overtake(
        waiting_base_priority=50.0,
        waiting_discovered_at=1000.0,
        new_base_priority=90.0,
        new_discovered_at=1000.0,
        now=now,
    ),
)

# Aging must be bounded, preventing unlimited priority inflation.
very_old = policy.effective_priority(
    base_priority=50.0,
    discovered_at=-100000.0,
    now=now,
)

check(
    "STARVATION BONUS BOUNDED",
    very_old == 70.0,
)

# The policy must remain deterministic for identical inputs.
first = policy.effective_priority(
    base_priority=55.0,
    discovered_at=400.0,
    now=now,
)

second = policy.effective_priority(
    base_priority=55.0,
    discovered_at=400.0,
    now=now,
)

check(
    "STARVATION POLICY DETERMINISTIC",
    first == second,
)

print("RESULT: PASS")
