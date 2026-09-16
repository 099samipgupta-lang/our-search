from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from crawler_system.domain_candidate_pipeline import DomainCandidatePipeline
from crawler_system.domain_discovery_work_queue import (
    DiscoveryWorkItem,
    DomainDiscoveryWorkQueue,
)
from crawler_system.domain_discovery_work_router import (
    DomainDiscoveryWorkRouter,
)
from crawler_system.global_source_federation import (
    FederatedDiscoveryResult,
)


@dataclass(frozen=True)
class ExpansionCandidate:
    hostname: str
    url: str
    source: str
    priority: float = 50.0
    evidence: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


@dataclass(frozen=True)
class ExpansionBatchResult:
    seen: int
    accepted: int
    duplicates: int
    invalid: int
    queued: int
    rejected_backpressure: int


class MassiveDomainURLExpansion:
    """
    Production-scale discovery expansion plane.

    Responsibilities:

    source federation
        ↓
    candidate conversion
        ↓
    normalization / validation
        ↓
    deterministic deduplication
        ↓
    priority assignment
        ↓
    durable work queue
        ↓
    placement-aware routing
        ↓
    crawler activation

    The architecture is intentionally independent of a fixed number of
    domains, URLs, workers, or physical machines.
    """

    VERSION = "massive-domain-url-expansion.v1"

    def __init__(
        self,
        storage_root: str,
        candidate_pipeline: Optional[
            DomainCandidatePipeline
        ] = None,
        work_queue: Optional[
            DomainDiscoveryWorkQueue
        ] = None,
        work_router: Optional[
            DomainDiscoveryWorkRouter
        ] = None,
        max_batch_size: int = 10000,
        admission_limit: Optional[int] = None,
    ):
        self.storage_root = storage_root

        self.candidate_pipeline = (
            candidate_pipeline
            or DomainCandidatePipeline(
                storage_path=f"{storage_root}/domain_candidate_pipeline.db"
            )
        )

        self.work_queue = (
            work_queue
            or DomainDiscoveryWorkQueue(
                storage_path=f"{storage_root}/discovery_work_queue.db"
            )
        )

        self.work_router = work_router

        if max_batch_size < 1:
            raise ValueError("max_batch_size must be >= 1")

        self.max_batch_size = max_batch_size
        self.admission_limit = admission_limit

        self._lock = threading.RLock()

        self._seen = 0
        self._accepted = 0
        self._duplicates = 0
        self._invalid = 0
        self._queued = 0
        self._rejected_backpressure = 0

    @staticmethod
    def _normalize_hostname(hostname: str) -> str:
        value = str(hostname).strip().lower().rstrip(".")
        return value

    @staticmethod
    def _stable_candidate_id(
        hostname: str,
        url: str,
        source: str,
    ) -> str:
        raw = (
            f"{hostname}\x00"
            f"{url}\x00"
            f"{source}"
        ).encode("utf-8")

        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _priority(candidate: ExpansionCandidate) -> float:
        priority = float(candidate.priority)

        if candidate.source == "certificate_transparency":
            priority += 100.0
        elif candidate.source in {
            "registry",
            "public_registry",
        }:
            priority += 90.0
        elif candidate.source == "feed":
            priority += 70.0
        elif candidate.source == "sitemap":
            priority += 60.0
        elif candidate.source == "link":
            priority += 50.0

        if candidate.evidence:
            priority += min(
                10.0,
                len(candidate.evidence) / 100.0,
            )

        return priority

    @classmethod
    def from_federated_result(
        cls,
        result: FederatedDiscoveryResult,
    ) -> list[ExpansionCandidate]:
        """
        Convert arbitrary source-federation output into the common
        expansion representation.

        Supports mappings and objects exposing the expected fields.
        """

        payload = getattr(
            result,
            "candidates",
            None,
        )

        if payload is None:
            payload = getattr(
                result,
                "results",
                None,
            )

        if payload is None:
            payload = []

        candidates: list[ExpansionCandidate] = []

        for item in payload:
            if isinstance(item, ExpansionCandidate):
                candidates.append(item)
                continue

            if isinstance(item, dict):
                hostname = item.get("hostname")
                url = item.get("url")
                source = item.get("source")

                if not hostname or not url or not source:
                    continue

                candidates.append(
                    ExpansionCandidate(
                        hostname=str(hostname),
                        url=str(url),
                        source=str(source),
                        priority=float(
                            item.get("priority", 50.0)
                        ),
                        evidence=item.get("evidence"),
                        metadata=item.get("metadata"),
                    )
                )
                continue

            hostname = getattr(item, "hostname", None)
            url = getattr(item, "url", None)
            source = getattr(item, "source", None)

            if not hostname or not url or not source:
                continue

            candidates.append(
                ExpansionCandidate(
                    hostname=str(hostname),
                    url=str(url),
                    source=str(source),
                    priority=float(
                        getattr(item, "priority", 50.0)
                    ),
                    evidence=getattr(
                        item,
                        "evidence",
                        None,
                    ),
                    metadata=getattr(
                        item,
                        "metadata",
                        None,
                    ),
                )
            )

        return candidates

    def _validate(
        self,
        candidate: ExpansionCandidate,
    ) -> bool:
        hostname = self._normalize_hostname(
            candidate.hostname
        )

        if not hostname:
            return False

        if len(hostname) > 253:
            return False

        if "/" in hostname or ":" in hostname:
            return False

        if not candidate.url:
            return False

        if not candidate.source:
            return False

        return True

    def _prepare(
        self,
        candidates: Iterable[ExpansionCandidate],
    ) -> tuple[
        list[ExpansionCandidate],
        int,
        int,
    ]:
        strongest: dict[str, ExpansionCandidate] = {}

        invalid = 0
        duplicates = 0

        for candidate in candidates:
            if not self._validate(candidate):
                invalid += 1
                continue

            hostname = self._normalize_hostname(
                candidate.hostname
            )

            normalized = ExpansionCandidate(
                hostname=hostname,
                url=candidate.url.strip(),
                source=candidate.source.strip(),
                priority=self._priority(candidate),
                evidence=candidate.evidence,
                metadata=candidate.metadata,
            )

            existing = strongest.get(hostname)

            if existing is None:
                strongest[hostname] = normalized
                continue

            duplicates += 1

            existing_key = (
                existing.priority,
                existing.source,
                existing.url,
            )

            candidate_key = (
                normalized.priority,
                normalized.source,
                normalized.url,
            )

            if candidate_key > existing_key:
                strongest[hostname] = normalized

        return (
            list(strongest.values()),
            duplicates,
            invalid,
        )

    def expand(
        self,
        candidates: Iterable[ExpansionCandidate],
    ) -> ExpansionBatchResult:
        """
        Admit one arbitrarily large upstream stream in bounded batches.

        No global materialization of the complete Web is required.
        """

        seen = 0
        accepted = 0
        duplicates = 0
        invalid = 0
        queued = 0
        rejected = 0

        batch: list[ExpansionCandidate] = []

        def process(
            current: list[ExpansionCandidate],
        ) -> None:
            nonlocal accepted
            nonlocal duplicates
            nonlocal invalid
            nonlocal queued
            nonlocal rejected

            prepared, dup_count, invalid_count = (
                self._prepare(current)
            )

            duplicates += dup_count
            invalid += invalid_count

            if not prepared:
                return

            for candidate in prepared:
                accepted += 1

                if (
                    self.admission_limit is not None
                    and queued >= self.admission_limit
                ):
                    rejected += 1
                    continue

                work_id = self._stable_candidate_id(
                    candidate.hostname,
                    candidate.url,
                    candidate.source,
                )

                item = DiscoveryWorkItem(
                    work_id=work_id,
                    logical_partition=0,
                    hostname=candidate.hostname,
                    priority=candidate.priority,
                    source=candidate.source,
                    available_at=time.time(),
                )

                try:
                    inserted = self.work_queue.enqueue(item)

                except Exception:
                    rejected += 1
                    continue

                if inserted:
                    queued += 1
                else:
                    duplicates += 1

        for candidate in candidates:
            seen += 1
            batch.append(candidate)

            if len(batch) >= self.max_batch_size:
                process(batch)
                batch.clear()

        if batch:
            process(batch)

        with self._lock:
            self._seen += seen
            self._accepted += accepted
            self._duplicates += duplicates
            self._invalid += invalid
            self._queued += queued
            self._rejected_backpressure += rejected

        return ExpansionBatchResult(
            seen=seen,
            accepted=accepted,
            duplicates=duplicates,
            invalid=invalid,
            queued=queued,
            rejected_backpressure=rejected,
        )

    def acquire_for_crawler(
        self,
        worker_id: str,
        limit: int = 100,
    ):
        """
        Acquire placement-aware discovery work.

        When a router exists, ownership is handled by the router.
        Otherwise callers can consume the durable queue directly.
        """

        if limit < 1:
            return []

        if self.work_router is not None:
            return self.work_router.acquire_from_any_partition(
                worker_id=worker_id,
                limit=limit,
            )

        return self.work_queue.claim(
            worker_id=worker_id,
            limit=limit,
        )

    def renew(
        self,
        work_id: str,
        worker_id: str,
        fencing_token: int,
    ) -> bool:
        if self.work_router is not None:
            return self.work_router.renew(
                work_id=work_id,
                worker_id=worker_id,
                fencing_token=fencing_token,
            )

        return self.work_queue.renew(
            work_id=work_id,
            worker_id=worker_id,
            fencing_token=fencing_token,
        )

    def complete(
        self,
        work_id: str,
        worker_id: str,
        fencing_token: int,
    ) -> bool:
        if self.work_router is not None:
            return self.work_router.complete(
                work_id=work_id,
                worker_id=worker_id,
                fencing_token=fencing_token,
            )

        return self.work_queue.complete(
            work_id=work_id,
            worker_id=worker_id,
            fencing_token=fencing_token,
        )

    def fail(
        self,
        work_id: str,
        worker_id: str,
        fencing_token: int,
        error: str,
    ) -> bool:
        if self.work_router is not None:
            return self.work_router.fail(
                work_id=work_id,
                worker_id=worker_id,
                fencing_token=fencing_token,
                error=error,
            )

        return self.work_queue.fail(
            work_id=work_id,
            worker_id=worker_id,
            fencing_token=fencing_token,
            error=error,
        )

    def recover(self) -> int:
        if self.work_router is not None:
            return self.work_router.recover_expired()

        return self.work_queue.recover_expired()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            local = {
                "version": self.VERSION,
                "seen": self._seen,
                "accepted": self._accepted,
                "duplicates": self._duplicates,
                "invalid": self._invalid,
                "queued": self._queued,
                "rejected_backpressure": (
                    self._rejected_backpressure
                ),
            }

        try:
            queue_stats = self.work_queue.stats()
        except Exception:
            queue_stats = {}

        local["queue"] = queue_stats

        if self.work_router is not None:
            try:
                local["router"] = self.work_router.stats()
            except Exception:
                local["router"] = {}

        return local
