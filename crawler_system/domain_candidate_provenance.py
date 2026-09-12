from dataclasses import dataclass
from time import time
from typing import Any


@dataclass(frozen=True)
class DomainCandidateProvenance:
    """
    Structured provenance for an independently discovered domain.

    This records where a candidate came from and the evidence
    associated with that discovery.
    """

    source: str
    evidence: str | None = None
    discovered_at: float | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "evidence": self.evidence,
            "discovered_at": self.discovered_at,
            "metadata": (
                dict(self.metadata)
                if self.metadata is not None
                else None
            ),
        }


class DomainCandidateProvenanceValidator:
    """Validate structured domain-candidate provenance."""

    @staticmethod
    def validate(
        provenance: DomainCandidateProvenance | None,
    ) -> bool:
        if provenance is None:
            return False

        source = provenance.source

        if not isinstance(source, str):
            return False

        if not source.strip():
            return False

        evidence = provenance.evidence

        if evidence is not None:
            if not isinstance(evidence, str):
                return False

            if not evidence.strip():
                return False

        discovered_at = provenance.discovered_at

        if discovered_at is not None:
            if not isinstance(
                discovered_at,
                (int, float),
            ):
                return False

            if discovered_at < 0:
                return False

        metadata = provenance.metadata

        if metadata is not None:
            if not isinstance(metadata, dict):
                return False

        return True


class DomainCandidateProvenanceBuilder:
    """
    Build structured provenance records from DomainCandidate objects.
    """

    @staticmethod
    def from_candidate(candidate):
        if candidate is None:
            return None

        source = getattr(
            candidate,
            "source",
            None,
        )

        evidence = getattr(
            candidate,
            "evidence",
            None,
        )

        discovered_at = getattr(
            candidate,
            "discovered_at",
            None,
        )

        metadata = getattr(
            candidate,
            "metadata",
            None,
        )

        if discovered_at is None:
            discovered_at = time()

        return DomainCandidateProvenance(
            source=source,
            evidence=evidence,
            discovered_at=discovered_at,
            metadata=(
                dict(metadata)
                if metadata is not None
                else None
            ),
        )


class DomainCandidateProvenanceStore:
    """
    In-memory provenance registry.

    This is intentionally not durable yet. Durable storage belongs
    to a later Stage 4.4 component.
    """

    def __init__(self):
        self._records: dict[str, DomainCandidateProvenance] = {}

    def record(
        self,
        hostname: str,
        provenance: DomainCandidateProvenance,
    ) -> bool:
        if not isinstance(hostname, str):
            return False

        hostname = hostname.strip().lower().rstrip(".")

        if not hostname:
            return False

        if not DomainCandidateProvenanceValidator.validate(
            provenance
        ):
            return False

        if hostname in self._records:
            return False

        self._records[hostname] = provenance
        return True

    def get(
        self,
        hostname: str,
    ) -> DomainCandidateProvenance | None:
        if not isinstance(hostname, str):
            return None

        return self._records.get(
            hostname.strip().lower()
        )

    def contains(
        self,
        hostname: str,
    ) -> bool:
        return self.get(hostname) is not None

    def count(self) -> int:
        return len(self._records)

    def clear(self) -> None:
        self._records.clear()

    def to_dict(self) -> dict[str, dict[str, Any]]:
        return {
            hostname: provenance.to_dict()
            for hostname, provenance in self._records.items()
        }
