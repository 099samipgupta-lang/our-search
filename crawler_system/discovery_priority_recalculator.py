from crawler_system.domain_candidate_priority import DomainCandidatePriority


class DiscoveryPriorityRecalculator:
    """
    Recalculates the durable priority of an independently
    discovered domain candidate from its current evidence.
    """

    def __init__(self, priority_engine=None):
        self.priority_engine = (
            priority_engine
            if priority_engine is not None
            else DomainCandidatePriority()
        )

    def calculate(self, candidate):
        return float(self.priority_engine.score(candidate))

    def recalculate_store_candidate(self, store, hostname):
        candidate_record = store.get(hostname)

        if candidate_record is None:
            return False

        from crawler_system.domain_discovery_sources import DomainCandidate

        candidate = DomainCandidate(
            hostname=candidate_record["hostname"],
            url=candidate_record["url"],
            source=candidate_record["source"],
            evidence=candidate_record.get("evidence"),
            discovered_at=candidate_record.get("discovered_at"),
            metadata=candidate_record.get("metadata"),
        )

        priority = self.calculate(candidate)

        return store.update_priority(
            hostname,
            priority,
        )
