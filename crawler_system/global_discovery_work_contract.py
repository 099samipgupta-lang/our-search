from __future__ import annotations

"""
OUR SEARCH — Global Discovery Work Contract

Version:
    global-discovery-work-contract.v1

Purpose:
    Provides the canonical work envelope shared by the enormous-scale
    discovery, routing, worker, recovery, crawler activation, storage,
    and indexing layers.

Design target:
    Billions of public-Web domains and an enormous URL universe.

This module is intentionally independent from any single queue, database,
crawler worker, or index implementation.
"""

from dataclasses import dataclass, field
from hashlib import sha256
from time import time
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit


CONTRACT_VERSION = "global-discovery-work-contract.v1"


@dataclass(frozen=True)
class DiscoveryWorkIdentity:
    """
    Stable identity for one logical discovery workload.

    The identity is deterministic and independent of worker assignment,
    physical storage placement, process ID, or execution attempt.
    """

    work_id: str
    hostname: str
    source: str

    @staticmethod
    def make(hostname: str, source: str) -> "DiscoveryWorkIdentity":
        normalized_hostname = normalize_hostname(hostname)
        normalized_source = str(source).strip().lower()

        if not normalized_hostname:
            raise ValueError("hostname is required")

        if not normalized_source:
            raise ValueError("source is required")

        digest = sha256(
            f"{normalized_hostname}\x00{normalized_source}".encode(
                "utf-8"
            )
        ).hexdigest()

        return DiscoveryWorkIdentity(
            work_id=digest,
            hostname=normalized_hostname,
            source=normalized_source,
        )


@dataclass(frozen=True)
class DiscoveryWorkContract:
    """
    Canonical representation of discovery work.

    Every downstream subsystem can consume this contract without knowing
    which upstream subsystem produced it.
    """

    work_id: str
    hostname: str
    url: str
    source: str

    priority: float = 50.0

    logical_partition: Optional[int] = None
    physical_bucket: Optional[int] = None

    discovered_at: float = field(default_factory=time)

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )

    worker_id: Optional[str] = None
    worker_generation: Optional[int] = None
    fencing_token: Optional[int] = None

    attempt: int = 0

    @classmethod
    def create(
        cls,
        hostname: str,
        url: Optional[str],
        source: str,
        priority: float = 50.0,
        logical_partition: Optional[int] = None,
        physical_bucket: Optional[int] = None,
        discovered_at: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> "DiscoveryWorkContract":

        normalized_hostname = normalize_hostname(hostname)

        if not normalized_hostname:
            raise ValueError("hostname is required")

        normalized_source = str(source).strip().lower()

        if not normalized_source:
            raise ValueError("source is required")

        canonical_url = canonical_url_for_hostname(
            normalized_hostname,
            url,
        )

        identity = DiscoveryWorkIdentity.make(
            normalized_hostname,
            normalized_source,
        )

        return cls(
            work_id=identity.work_id,
            hostname=normalized_hostname,
            url=canonical_url,
            source=normalized_source,
            priority=float(priority),
            logical_partition=logical_partition,
            physical_bucket=physical_bucket,
            discovered_at=(
                float(discovered_at)
                if discovered_at is not None
                else time()
            ),
            metadata=dict(metadata or {}),
        )

    def with_placement(
        self,
        logical_partition: int,
        physical_bucket: int,
    ) -> "DiscoveryWorkContract":
        return DiscoveryWorkContract(
            work_id=self.work_id,
            hostname=self.hostname,
            url=self.url,
            source=self.source,
            priority=self.priority,
            logical_partition=int(logical_partition),
            physical_bucket=int(physical_bucket),
            discovered_at=self.discovered_at,
            metadata=dict(self.metadata),
            worker_id=self.worker_id,
            worker_generation=self.worker_generation,
            fencing_token=self.fencing_token,
            attempt=self.attempt,
        )

    def with_lease(
        self,
        worker_id: str,
        worker_generation: int,
        fencing_token: int,
        attempt: Optional[int] = None,
    ) -> "DiscoveryWorkContract":
        return DiscoveryWorkContract(
            work_id=self.work_id,
            hostname=self.hostname,
            url=self.url,
            source=self.source,
            priority=self.priority,
            logical_partition=self.logical_partition,
            physical_bucket=self.physical_bucket,
            discovered_at=self.discovered_at,
            metadata=dict(self.metadata),
            worker_id=str(worker_id),
            worker_generation=int(worker_generation),
            fencing_token=int(fencing_token),
            attempt=(
                self.attempt
                if attempt is None
                else int(attempt)
            ),
        )

    def activation_payload(self) -> dict[str, Any]:
        """
        Stable payload consumed by crawler activation.
        """
        return {
            "work_id": self.work_id,
            "hostname": self.hostname,
            "url": self.url,
            "source": self.source,
            "priority": self.priority,
            "logical_partition": self.logical_partition,
            "physical_bucket": self.physical_bucket,
            "discovered_at": self.discovered_at,
            "metadata": dict(self.metadata),
            "worker_id": self.worker_id,
            "worker_generation": self.worker_generation,
            "fencing_token": self.fencing_token,
            "attempt": self.attempt,
            "contract_version": CONTRACT_VERSION,
        }

    def storage_payload(self) -> dict[str, Any]:
        """
        Stable payload consumed by distributed storage/index handoff.
        """
        return {
            "work_id": self.work_id,
            "hostname": self.hostname,
            "url": self.url,
            "source": self.source,
            "priority": self.priority,
            "logical_partition": self.logical_partition,
            "physical_bucket": self.physical_bucket,
            "discovered_at": self.discovered_at,
            "metadata": dict(self.metadata),
            "contract_version": CONTRACT_VERSION,
        }


def normalize_hostname(hostname: str) -> str:
    """
    Canonical hostname normalization.

    No network access is performed.
    """

    value = str(hostname).strip().lower()

    if not value:
        return ""

    if "://" in value:
        value = urlsplit(value).hostname or ""

    value = value.strip().rstrip(".")

    if value.startswith("*."):
        value = value[2:]

    return value


def canonical_url_for_hostname(
    hostname: str,
    url: Optional[str],
) -> str:
    """
    Produces one canonical activation URL.

    A supplied URL is preserved after basic normalization.
    If upstream work only contains a hostname, a deterministic HTTPS
    root URL is generated so every downstream component has a URL.
    """

    normalized_hostname = normalize_hostname(hostname)

    if not normalized_hostname:
        raise ValueError("hostname is required")

    if url:
        supplied = str(url).strip()

        if "://" not in supplied:
            supplied = "https://" + supplied

        parsed = urlsplit(supplied)

        if not parsed.hostname:
            raise ValueError(
                f"invalid discovery URL: {url!r}"
            )

        supplied_hostname = normalize_hostname(
            parsed.hostname
        )

        if supplied_hostname != normalized_hostname:
            raise ValueError(
                "URL hostname does not match discovery hostname"
            )

        return supplied

    return f"https://{normalized_hostname}/"


def contract_from_candidate(candidate: Any) -> DiscoveryWorkContract:
    """
    Converts a domain candidate, federated result item, or compatible
    object into the canonical contract.

    Accepted attributes:
        hostname
        url
        source
        priority
        discovered_at
        metadata
    """

    hostname = getattr(candidate, "hostname", None)

    if not hostname:
        raise ValueError(
            "candidate does not contain hostname"
        )

    return DiscoveryWorkContract.create(
        hostname=hostname,
        url=getattr(candidate, "url", None),
        source=getattr(candidate, "source", "unknown"),
        priority=getattr(candidate, "priority", 50.0),
        discovered_at=getattr(
            candidate,
            "discovered_at",
            None,
        ),
        metadata=getattr(
            candidate,
            "metadata",
            None,
        ),
    )


def contract_from_work_item(
    work_item: Any,
) -> DiscoveryWorkContract:
    """
    Converts durable queue/router work into the canonical contract.

    Critical compatibility behavior:
        Older queue records may contain hostname but no URL.
        In that case the contract deterministically supplies the
        HTTPS root URL for the hostname.

    This prevents the downstream crawler/storage/index layers from
    depending on optional URL fields in queue implementations.
    """

    hostname = getattr(work_item, "hostname", None)

    if not hostname:
        raise ValueError(
            "work item does not contain hostname"
        )

    return DiscoveryWorkContract.create(
        hostname=hostname,
        url=getattr(work_item, "url", None),
        source=getattr(work_item, "source", "unknown"),
        priority=getattr(
            work_item,
            "priority",
            50.0,
        ),
        logical_partition=getattr(
            work_item,
            "logical_partition",
            None,
        ),
        physical_bucket=getattr(
            work_item,
            "physical_bucket",
            None,
        ),
        discovered_at=getattr(
            work_item,
            "discovered_at",
            None,
        ),
        metadata=getattr(
            work_item,
            "metadata",
            None,
        ),
    )


def validate_contract(
    contract: DiscoveryWorkContract,
) -> None:
    """
    Strong invariant validation before activation or storage.
    """

    if not contract.work_id:
        raise ValueError("missing work_id")

    if not contract.hostname:
        raise ValueError("missing hostname")

    if not contract.url:
        raise ValueError("missing URL")

    if not contract.source:
        raise ValueError("missing source")

    normalized_hostname = normalize_hostname(
        contract.hostname
    )

    parsed = urlsplit(contract.url)

    if not parsed.hostname:
        raise ValueError("URL has no hostname")

    url_hostname = normalize_hostname(
        parsed.hostname
    )

    if normalized_hostname != url_hostname:
        raise ValueError(
            "contract hostname/URL hostname mismatch"
        )

    expected_identity = DiscoveryWorkIdentity.make(
        normalized_hostname,
        contract.source,
    )

    if contract.work_id != expected_identity.work_id:
        raise ValueError(
            "work_id does not match canonical identity"
        )

    if contract.attempt < 0:
        raise ValueError("attempt cannot be negative")

    if contract.logical_partition is not None:
        if contract.logical_partition < 0:
            raise ValueError(
                "logical_partition cannot be negative"
            )

    if contract.physical_bucket is not None:
        if contract.physical_bucket < 0:
            raise ValueError(
                "physical_bucket cannot be negative"
            )


__all__ = [
    "CONTRACT_VERSION",
    "DiscoveryWorkIdentity",
    "DiscoveryWorkContract",
    "normalize_hostname",
    "canonical_url_for_hostname",
    "contract_from_candidate",
    "contract_from_work_item",
    "validate_contract",
]
