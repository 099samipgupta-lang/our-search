from crawler_system.discovery_priority_aging import (
    DiscoveryPriorityAging,
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

now = 1000.0

fresh_bonus = aging.bonus(
    discovered_at=1000.0,
    now=now,
)

check(
    "FRESH CANDIDATE NO BONUS",
    fresh_bonus == 0.0,
)

one_unit_bonus = aging.bonus(
    discovered_at=900.0,
    now=now,
)

check(
    "AGING BONUS APPLIED",
    one_unit_bonus == 5.0,
)

older_bonus = aging.bonus(
    discovered_at=500.0,
    now=now,
)

check(
    "OLDER CANDIDATE HIGHER BONUS",
    older_bonus > one_unit_bonus,
)

capped_bonus = aging.bonus(
    discovered_at=0.0,
    now=now,
)

check(
    "AGING BONUS CAPPED",
    capped_bonus == 20.0,
)

future_bonus = aging.bonus(
    discovered_at=1100.0,
    now=now,
)

check(
    "FUTURE TIMESTAMP SAFE",
    future_bonus == 0.0,
)

aged_priority = aging.apply(
    base_priority=50.0,
    discovered_at=900.0,
    now=now,
)

check(
    "AGED PRIORITY CALCULATED",
    aged_priority == 55.0,
)

check(
    "BASE PRIORITY PRESERVED",
    aging.apply(
        base_priority=50.0,
        discovered_at=1000.0,
        now=now,
    ) == 50.0,
)

check(
    "INVALID BASE SAFE",
    aging.apply(
        base_priority="invalid",
        discovered_at=900.0,
        now=now,
    ) == 0.0,
)

print("RESULT: PASS")
