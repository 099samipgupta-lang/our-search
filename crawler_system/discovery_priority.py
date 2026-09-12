from dataclasses import dataclass

from crawler_system.domain_candidate_priority import DomainCandidatePriority
from crawler_system.expansion_priority import ExpansionPriority


@dataclass(frozen=True)
class DiscoveryPriorityDecision:
    """
    Unified discovery-priority decision.

    This layer does not replace the specialized priority engines.
    It provides one common interface for large-scale discovery.
    """

    score: float
    category: str
    reason: str


class DiscoveryPriority:
    """
    Coordinates priority decisions across discovery layers.

    Domain candidates use DomainCandidatePriority.
    Web expansion URLs use ExpansionPriority.

    Higher scores mean earlier consideration.
    """

    def __init__(
        self,
        domain_priority=None,
        expansion_priority=None,
    ):
        self.domain_priority = (
            domain_priority
            if domain_priority is not None
            else DomainCandidatePriority()
        )

        self.expansion_priority = (
            expansion_priority
            if expansion_priority is not None
            else ExpansionPriority()
        )

    def score_domain_candidate(self, candidate):
        score = self.domain_priority.score(candidate)

        return DiscoveryPriorityDecision(
            score=score,
            category="domain_candidate",
            reason="domain candidate priority",
        )

    def score_expansion(
        self,
        url,
        *,
        source_url=None,
        source="link",
        discovery_depth=0,
    ):
        score = self.expansion_priority.score(
            url,
            source_url=source_url,
            source=source,
            discovery_depth=discovery_depth,
        )

        return DiscoveryPriorityDecision(
            score=score,
            category="expansion",
            reason="web expansion priority",
        )

    @staticmethod
    def rank(decisions):
        """
        Deterministically rank priority decisions from highest
        score to lowest score.
        """
        if not decisions:
            return []

        return sorted(
            decisions,
            key=lambda decision: (
                -float(decision.score),
                decision.category,
                decision.reason,
            ),
        )
