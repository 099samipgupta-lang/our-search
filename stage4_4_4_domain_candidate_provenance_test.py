from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.domain_candidate_provenance import (
    DomainCandidateProvenance,
    DomainCandidateProvenanceBuilder,
    DomainCandidateProvenanceStore,
    DomainCandidateProvenanceValidator,
)


def check(name, condition):
    if not condition:
        raise AssertionError(name)
    print(f"{name}: PASS")


# ---------------------------------------------------------
# 1. Valid provenance
# ---------------------------------------------------------

provenance = DomainCandidateProvenance(
    source="certificate_transparency",
    evidence="https://crt.sh/?q=%25&output=json",
    discovered_at=1234567890.0,
    metadata={
        "discovery_method": "certificate_transparency",
    },
)

check(
    "VALID PROVENANCE",
    DomainCandidateProvenanceValidator.validate(provenance),
)


# ---------------------------------------------------------
# 2. Invalid provenance rejection
# ---------------------------------------------------------

invalid_cases = [
    DomainCandidateProvenance(source=""),
    DomainCandidateProvenance(source=None),
    DomainCandidateProvenance(
        source="test",
        evidence="",
    ),
    DomainCandidateProvenance(
        source="test",
        evidence=123,
    ),
    DomainCandidateProvenance(
        source="test",
        discovered_at=-1,
    ),
    DomainCandidateProvenance(
        source="test",
        discovered_at="invalid",
    ),
    DomainCandidateProvenance(
        source="test",
        metadata=[],
    ),
]

for index, invalid in enumerate(invalid_cases, start=1):
    check(
        f"INVALID PROVENANCE REJECTION {index}",
        not DomainCandidateProvenanceValidator.validate(
            invalid
        ),
    )


# ---------------------------------------------------------
# 3. Dictionary serialization
# ---------------------------------------------------------

data = provenance.to_dict()

check(
    "PROVENANCE SERIALIZATION",
    data["source"] == "certificate_transparency",
)

check(
    "EVIDENCE SERIALIZATION",
    data["evidence"]
    == "https://crt.sh/?q=%25&output=json",
)

check(
    "METADATA SERIALIZATION",
    data["metadata"]["discovery_method"]
    == "certificate_transparency",
)


# ---------------------------------------------------------
# 4. Candidate → provenance
# ---------------------------------------------------------

candidate = DomainCandidate(
    hostname="example.com",
    url="https://example.com/",
    source="certificate_transparency",
    evidence="ct-response",
    discovered_at=123.0,
    metadata={
        "endpoint": "test-endpoint",
    },
)

candidate_provenance = (
    DomainCandidateProvenanceBuilder.from_candidate(
        candidate
    )
)

check(
    "CANDIDATE PROVENANCE BUILD",
    candidate_provenance is not None,
)

check(
    "CANDIDATE SOURCE PRESERVED",
    candidate_provenance.source
    == "certificate_transparency",
)

check(
    "CANDIDATE EVIDENCE PRESERVED",
    candidate_provenance.evidence
    == "ct-response",
)

check(
    "CANDIDATE TIMESTAMP PRESERVED",
    candidate_provenance.discovered_at == 123.0,
)

check(
    "CANDIDATE METADATA PRESERVED",
    candidate_provenance.metadata["endpoint"]
    == "test-endpoint",
)


# ---------------------------------------------------------
# 5. Automatic timestamp
# ---------------------------------------------------------

timestamp_candidate = DomainCandidate(
    hostname="timestamp.example",
    url="https://timestamp.example/",
    source="test_source",
)

timestamp_provenance = (
    DomainCandidateProvenanceBuilder.from_candidate(
        timestamp_candidate
    )
)

check(
    "AUTOMATIC DISCOVERY TIMESTAMP",
    isinstance(
        timestamp_provenance.discovered_at,
        (int, float),
    ),
)

check(
    "AUTOMATIC TIMESTAMP POSITIVE",
    timestamp_provenance.discovered_at > 0,
)


# ---------------------------------------------------------
# 6. Provenance store
# ---------------------------------------------------------

store = DomainCandidateProvenanceStore()

check(
    "STORE RECORD",
    store.record(
        "Example.COM.",
        provenance,
    ),
)

check(
    "STORE COUNT",
    store.count() == 1,
)

stored = store.get("example.com")

check(
    "STORE RETRIEVAL",
    stored == provenance,
)

check(
    "STORE CONTAINS",
    store.contains("EXAMPLE.COM"),
)

check(
    "DUPLICATE PROVENANCE REJECTED",
    not store.record(
        "example.com",
        provenance,
    ),
)

check(
    "STORE COUNT AFTER DUPLICATE",
    store.count() == 1,
)


# ---------------------------------------------------------
# 7. Invalid hostname rejected by store
# ---------------------------------------------------------

check(
    "INVALID HOSTNAME REJECTED",
    not store.record(
        "",
        provenance,
    ),
)


# ---------------------------------------------------------
# 8. Store serialization
# ---------------------------------------------------------

store_data = store.to_dict()

check(
    "STORE SERIALIZATION",
    "example.com" in store_data,
)

check(
    "STORE SERIALIZED SOURCE",
    store_data["example.com"]["source"]
    == "certificate_transparency",
)


# ---------------------------------------------------------
# 9. Clear
# ---------------------------------------------------------

store.clear()

check(
    "STORE CLEAR",
    store.count() == 0,
)

print("RESULT: PASS")
