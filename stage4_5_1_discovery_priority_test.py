from crawler_system.discovery_priority import (
    DiscoveryPriority,
    DiscoveryPriorityDecision,
)
from crawler_system.domain_discovery_pipeline import DomainCandidate


def check(label, condition):
    print(f"{label}: {'PASS' if condition else 'FAIL'}")
    if not condition:
        raise AssertionError(label)


priority = DiscoveryPriority()

candidate = DomainCandidate(
    hostname="example.com",
    url="https://example.com",
    source="certificate_transparency",
    evidence="https://crt.sh/?q=%25&output=json",
    metadata={"discovery_method": "certificate_transparency"},
)

domain_decision = priority.score_domain_candidate(candidate)

check(
    "DOMAIN DECISION TYPE",
    isinstance(domain_decision, DiscoveryPriorityDecision),
)

check(
    "DOMAIN DECISION CATEGORY",
    domain_decision.category == "domain_candidate",
)

check(
    "DOMAIN PRIORITY POSITIVE",
    domain_decision.score > 0,
)

expansion_decision = priority.score_expansion(
    "https://example.org",
    source="link",
    discovery_depth=0,
)

check(
    "EXPANSION DECISION TYPE",
    isinstance(expansion_decision, DiscoveryPriorityDecision),
)

check(
    "EXPANSION DECISION CATEGORY",
    expansion_decision.category == "expansion",
)

check(
    "EXPANSION PRIORITY POSITIVE",
    expansion_decision.score > 0,
)

high = DiscoveryPriorityDecision(
    score=90.0,
    category="domain_candidate",
    reason="high",
)

low = DiscoveryPriorityDecision(
    score=20.0,
    category="expansion",
    reason="low",
)

ranked = priority.rank([low, high])

check(
    "RANKING RETURNS LIST",
    isinstance(ranked, list),
)

check(
    "HIGHER PRIORITY FIRST",
    ranked[0] is high,
)

check(
    "LOWER PRIORITY SECOND",
    ranked[1] is low,
)

check(
    "EMPTY RANKING SAFE",
    priority.rank([]) == [],
)

print("RESULT: PASS")
