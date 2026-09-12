from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.domain_candidate_priority import DomainCandidatePriority


priority = DomainCandidatePriority()


def check(name, condition):
    if not condition:
        raise AssertionError(name)
    print(f"{name}: PASS")


# ---------------------------------------------------------
# 1. Basic valid candidate
# ---------------------------------------------------------

candidate = DomainCandidate(
    hostname="example.com",
    url="https://example.com/",
    source="certificate_transparency",
    evidence="https://ct.example/query",
)

score = priority.score(candidate)

check(
    "BASIC SCORE POSITIVE",
    score > 0,
)

check(
    "BASIC SCORE BOUNDED",
    1.0 <= score <= 100.0,
)


# ---------------------------------------------------------
# 2. Certificate Transparency gets strong priority
# ---------------------------------------------------------

ct_candidate = DomainCandidate(
    hostname="example.com",
    url="https://example.com/",
    source="certificate_transparency",
    evidence="ct-record",
)

unknown_candidate = DomainCandidate(
    hostname="example.org",
    url="https://example.org/",
    source="unknown_source",
    evidence="record",
)

check(
    "CT SOURCE PRIORITY",
    priority.score(ct_candidate)
    > priority.score(unknown_candidate),
)


# ---------------------------------------------------------
# 3. Evidence increases priority
# ---------------------------------------------------------

with_evidence = DomainCandidate(
    hostname="example.net",
    url="https://example.net/",
    source="unknown_source",
    evidence="discovery-record",
)

without_evidence = DomainCandidate(
    hostname="example.net",
    url="https://example.net/",
    source="unknown_source",
    evidence=None,
)

check(
    "EVIDENCE PRIORITY",
    priority.score(with_evidence)
    > priority.score(without_evidence),
)


# ---------------------------------------------------------
# 4. HTTPS preference
# ---------------------------------------------------------

https_candidate = DomainCandidate(
    hostname="example.net",
    url="https://example.net/",
    source="unknown_source",
)

http_candidate = DomainCandidate(
    hostname="example.net",
    url="http://example.net/",
    source="unknown_source",
)

check(
    "HTTPS PRIORITY",
    priority.score(https_candidate)
    > priority.score(http_candidate),
)


# ---------------------------------------------------------
# 5. Root URL preference
# ---------------------------------------------------------

root_candidate = DomainCandidate(
    hostname="example.net",
    url="https://example.net/",
    source="unknown_source",
)

deep_candidate = DomainCandidate(
    hostname="example.net",
    url="https://example.net/path/to/resource",
    source="unknown_source",
)

check(
    "ROOT URL PRIORITY",
    priority.score(root_candidate)
    > priority.score(deep_candidate),
)


# ---------------------------------------------------------
# 6. Normal two-label hostname gets preference
# ---------------------------------------------------------

normal_domain = DomainCandidate(
    hostname="example.com",
    url="https://example.com/",
    source="unknown_source",
)

three_label_domain = DomainCandidate(
    hostname="api.example.com",
    url="https://api.example.com/",
    source="unknown_source",
)

check(
    "NORMAL DOMAIN PRIORITY",
    priority.score(normal_domain)
    > priority.score(three_label_domain),
)


# ---------------------------------------------------------
# 7. Deep subdomain penalty does not destroy candidate
# ---------------------------------------------------------

deep_domain = DomainCandidate(
    hostname="a.b.c.d.e.f.example.com",
    url="https://a.b.c.d.e.f.example.com/",
    source="certificate_transparency",
    evidence="ct-record",
)

deep_score = priority.score(deep_domain)

check(
    "DEEP DOMAIN STILL VALID",
    deep_score >= 1.0,
)

check(
    "DEEP DOMAIN BOUNDED",
    deep_score <= 100.0,
)


# ---------------------------------------------------------
# 8. Query URL receives lower score
# ---------------------------------------------------------

clean_url = DomainCandidate(
    hostname="example.com",
    url="https://example.com/",
    source="unknown_source",
)

query_url = DomainCandidate(
    hostname="example.com",
    url="https://example.com/?q=test",
    source="unknown_source",
)

check(
    "QUERY URL PENALTY",
    priority.score(clean_url)
    > priority.score(query_url),
)


# ---------------------------------------------------------
# 9. Invalid candidates
# ---------------------------------------------------------

check(
    "NONE REJECTED",
    priority.score(None) == 0.0,
)

check(
    "INVALID HOST REJECTED",
    priority.score(
        DomainCandidate(
            hostname="",
            url="https://example.com/",
            source="test",
        )
    ) == 0.0,
)

check(
    "INVALID URL TYPE REJECTED",
    priority.score(
        DomainCandidate(
            hostname="example.com",
            url=None,
            source="test",
        )
    ) == 0.0,
)

check(
    "INVALID SOURCE REJECTED",
    priority.score(
        DomainCandidate(
            hostname="example.com",
            url="https://example.com/",
            source="",
        )
    ) == 0.0,
)


# ---------------------------------------------------------
# 10. Score remains bounded
# ---------------------------------------------------------

maximum_candidate = DomainCandidate(
    hostname="example.com",
    url="https://example.com/",
    source="certificate_transparency",
    evidence="strong-evidence",
)

maximum_score = priority.score(maximum_candidate)

check(
    "MAXIMUM SCORE BOUNDED",
    maximum_score <= 100.0,
)

check(
    "MINIMUM SCORE BOUNDED",
    maximum_score >= 1.0,
)


print("RESULT: PASS")
