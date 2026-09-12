from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.domain_candidate_pipeline import (
    DomainCandidateNormalizer,
    DomainCandidateValidator,
    DomainCandidateDeduplicator,
    DomainCandidatePipeline,
)


def check(name, condition):
    if not condition:
        raise AssertionError(name)
    print(f"{name}: PASS")


# ---------------------------------------------------------
# 1. Hostname normalization
# ---------------------------------------------------------

normalizer = DomainCandidateNormalizer()

check(
    "LOWERCASE NORMALIZATION",
    normalizer.normalize_hostname("WWW.Example.COM") == "www.example.com",
)

check(
    "TRAILING DOT NORMALIZATION",
    normalizer.normalize_hostname("example.com.") == "example.com",
)

check(
    "WILDCARD NORMALIZATION",
    normalizer.normalize_hostname("*.Example.COM") == "example.com",
)

check(
    "URL REJECTION",
    normalizer.normalize_hostname("https://example.com") is None,
)


# ---------------------------------------------------------
# 2. Candidate normalization
# ---------------------------------------------------------

candidate = DomainCandidate(
    hostname="WWW.Example.COM.",
    url="https://WWW.Example.COM./some/path",
    source="test_source",
    evidence="test_evidence",
)

normalized = normalizer.normalize_candidate(candidate)

check(
    "CANDIDATE NORMALIZATION",
    normalized is not None,
)

check(
    "NORMALIZED HOSTNAME",
    normalized.hostname == "www.example.com",
)

check(
    "NORMALIZED ROOT URL",
    normalized.url == "https://www.example.com/",
)


# ---------------------------------------------------------
# 3. Validation
# ---------------------------------------------------------

validator = DomainCandidateValidator()

check(
    "VALID CANDIDATE",
    validator.validate(normalized),
)

invalid_hosts = [
    "localhost",
    "",
    "example",
    "-example.com",
    "example-.com",
    "example..com",
    "example.com/",
    "example.123",
]

for hostname in invalid_hosts:
    invalid = DomainCandidate(
        hostname=hostname,
        url=f"https://{hostname}/",
        source="test_source",
    )

    check(
        f"INVALID HOST REJECTION: {hostname or '<empty>'}",
        not validator.validate(invalid),
    )


# ---------------------------------------------------------
# 4. Deduplication
# ---------------------------------------------------------

deduplicator = DomainCandidateDeduplicator()

first = DomainCandidate(
    hostname="example.com",
    url="https://example.com/",
    source="source_a",
)

second = DomainCandidate(
    hostname="example.com",
    url="https://example.com/",
    source="source_b",
)

check(
    "FIRST CANDIDATE ACCEPTED",
    deduplicator.is_new(first),
)

check(
    "DUPLICATE HOST REJECTED",
    not deduplicator.is_new(second),
)

check(
    "DEDUPLICATION COUNT",
    deduplicator.count() == 1,
)


# ---------------------------------------------------------
# 5. Full pipeline
# ---------------------------------------------------------

pipeline = DomainCandidatePipeline()

candidates = {
    DomainCandidate(
        hostname="Example.COM.",
        url="https://Example.COM/path",
        source="source_a",
    ),
    DomainCandidate(
        hostname="example.com",
        url="https://example.com/",
        source="source_b",
    ),
    DomainCandidate(
        hostname="Another.org",
        url="https://Another.org/test",
        source="source_a",
    ),
    DomainCandidate(
        hostname="localhost",
        url="https://localhost/",
        source="source_a",
    ),
}

results = pipeline.process_many(candidates)

check(
    "PIPELINE RESULT COUNT",
    len(results) == 2,
)

hostnames = {candidate.hostname for candidate in results}

check(
    "PIPELINE NORMALIZED HOSTNAMES",
    hostnames == {"example.com", "another.org"},
)

check(
    "PIPELINE DEDUPLICATION",
    pipeline.count() == 2,
)


# ---------------------------------------------------------
# 6. Provenance preservation
# ---------------------------------------------------------

provenance_candidate = DomainCandidate(
    hostname="provenance.example",
    url="https://provenance.example/path",
    source="certificate_transparency",
    evidence="https://ct.example/query",
    metadata={"test": "value"},
)

processed = pipeline.process(provenance_candidate)

check(
    "PROVENANCE PRESERVED",
    processed.source == "certificate_transparency",
)

check(
    "EVIDENCE PRESERVED",
    processed.evidence == "https://ct.example/query",
)

check(
    "METADATA PRESERVED",
    processed.metadata == {"test": "value"},
)


print("RESULT: PASS")
