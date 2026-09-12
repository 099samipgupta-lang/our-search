from crawler_system.discovery_priority_aging import DiscoveryPriorityAging


class DiscoveryStarvationPolicy:
    """
    Ensures waiting discovery candidates gain bounded priority
    over time so they cannot remain permanently buried.
    """

    def __init__(self, aging=None):
        self.aging = (
            aging
            if aging is not None
            else DiscoveryPriorityAging()
        )

    def effective_priority(
        self,
        base_priority,
        discovered_at,
        now=None,
    ):
        return self.aging.apply(
            base_priority,
            discovered_at,
            now=now,
        )

    def should_overtake(
        self,
        waiting_base_priority,
        waiting_discovered_at,
        new_base_priority,
        new_discovered_at,
        now=None,
    ):
        waiting_priority = self.effective_priority(
            waiting_base_priority,
            waiting_discovered_at,
            now=now,
        )

        new_priority = self.effective_priority(
            new_base_priority,
            new_discovered_at,
            now=now,
        )

        return waiting_priority > new_priority
