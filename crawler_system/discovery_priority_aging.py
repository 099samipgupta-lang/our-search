import time


class DiscoveryPriorityAging:
    """
    Prevents long-waiting discovery candidates from being
    permanently buried by newer candidates.

    Aging is deterministic and bounded.
    """

    def __init__(
        self,
        age_unit_seconds=3600.0,
        bonus_per_unit=2.0,
        max_bonus=20.0,
    ):
        self.age_unit_seconds = float(age_unit_seconds)
        self.bonus_per_unit = float(bonus_per_unit)
        self.max_bonus = float(max_bonus)

        if self.age_unit_seconds <= 0:
            raise ValueError("age_unit_seconds must be positive")

        if self.bonus_per_unit < 0:
            raise ValueError("bonus_per_unit must be non-negative")

        if self.max_bonus < 0:
            raise ValueError("max_bonus must be non-negative")

    def bonus(self, discovered_at, now=None):
        if not isinstance(discovered_at, (int, float)):
            return 0.0

        if now is None:
            now = time.time()

        try:
            age = max(float(now) - float(discovered_at), 0.0)
        except (TypeError, ValueError):
            return 0.0

        units = age / self.age_unit_seconds

        return min(
            units * self.bonus_per_unit,
            self.max_bonus,
        )

    def apply(self, base_priority, discovered_at, now=None):
        try:
            base = float(base_priority)
        except (TypeError, ValueError):
            return 0.0

        if base < 0:
            base = 0.0

        return base + self.bonus(
            discovered_at,
            now=now,
        )
