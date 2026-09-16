from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from crawler_system.domain_candidate_pipeline import (
    DomainCandidatePipeline,
)
from crawler_system.domain_discovery_fabric import (
    ClaimedDomainDiscovery,
    DomainDiscoveryFabric,
    DomainDiscoveryRecord,
)
from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
)


@dataclass(frozen=True)
class DomainDiscoveryFabricMetrics:
    cycles: int
    source_discoveries: int
    candidates_seen: int
    candidates_accepted: int
    inserted: int
    duplicates: int
    invalid: int
    claimed: int
    completed: int
    failed: int
    retried: int
    recovered_leases: int
    activation_failures: int


@dataclass(frozen=True)
class DomainDiscoveryWorkerLease:
    worker_id: str
    shard_id: int
    lease_owner: str
    acquired_at: float


class DomainDiscoveryFabricController:
    """
    Placement-aware distributed worker control plane for the global
    domain-discovery fabric.

    The architecture intentionally separates:

        logical identity
              |
              v
        durable placement
              |
              v
        physical work
              |
              v
        worker acquisition
              |
              v
        record-level leases

    Workers are execution capacity.

    Physical buckets are storage placement.

    Logical partitions are stable identity.

    None of those quantities are required to have the same cardinality.

    The controller therefore never creates one worker per physical
    bucket and never assumes that the physical bucket count represents
    the size of the Web.

    The worker layer operates on currently visible work and acquires
    short-lived execution ownership through the existing durable
    record leases.
    """

    VERSION = "global-domain-discovery-controller.v4"

    def __init__(
        self,
        registry: DomainDiscoverySourceRegistry,
        fabric: DomainDiscoveryFabric,
        candidate_pipeline: Optional[
            DomainCandidatePipeline
        ] = None,
        activation_callback: Optional[
            Callable[
                [str, str, int, Optional[str]],
                bool,
            ]
        ] = None,
        discovery_workers: int = 8,
        domain_workers: int = 32,
        claim_batch_size: int = 100,
        lease_recovery_interval: float = 30.0,
        cycle_interval: float = 5.0,
        max_contexts_per_cycle: int = 100,
        worker_idle_sleep: float = 0.25,
    ):
        if not isinstance(
            registry,
            DomainDiscoverySourceRegistry,
        ):
            raise TypeError(
                "registry must be DomainDiscoverySourceRegistry"
            )

        if not isinstance(
            fabric,
            DomainDiscoveryFabric,
        ):
            raise TypeError(
                "fabric must be DomainDiscoveryFabric"
            )

        if candidate_pipeline is None:
            candidate_pipeline = (
                DomainCandidatePipeline()
            )

        if not isinstance(
            candidate_pipeline,
            DomainCandidatePipeline,
        ):
            raise TypeError(
                "candidate_pipeline must be DomainCandidatePipeline"
            )

        self.discovery_workers = max(
            1,
            int(discovery_workers),
        )

        self.domain_workers = max(
            1,
            int(domain_workers),
        )

        self.claim_batch_size = max(
            1,
            int(claim_batch_size),
        )

        self.lease_recovery_interval = float(
            lease_recovery_interval
        )

        self.cycle_interval = float(
            cycle_interval
        )

        self.max_contexts_per_cycle = max(
            1,
            int(max_contexts_per_cycle),
        )

        self.worker_idle_sleep = max(
            0.01,
            float(worker_idle_sleep),
        )

        if self.lease_recovery_interval <= 0:
            raise ValueError(
                "lease_recovery_interval must be positive"
            )

        if self.cycle_interval < 0:
            raise ValueError(
                "cycle_interval cannot be negative"
            )

        self.registry = registry
        self.fabric = fabric
        self.candidate_pipeline = (
            candidate_pipeline
        )
        self.activation_callback = (
            activation_callback
        )

        self._stop_event = threading.Event()

        self._metrics_lock = threading.RLock()

        self._metrics = {
            "cycles": 0,
            "source_discoveries": 0,
            "candidates_seen": 0,
            "candidates_accepted": 0,
            "inserted": 0,
            "duplicates": 0,
            "invalid": 0,
            "claimed": 0,
            "completed": 0,
            "failed": 0,
            "retried": 0,
            "recovered_leases": 0,
            "activation_failures": 0,
        }

        self._last_lease_recovery = 0.0

    # ============================================================
    # Metrics
    # ============================================================

    def _increment(
        self,
        name: str,
        value: int = 1,
    ) -> None:
        with self._metrics_lock:
            self._metrics[name] = (
                self._metrics.get(name, 0)
                + int(value)
            )

    def metrics(
        self,
    ) -> DomainDiscoveryFabricMetrics:
        with self._metrics_lock:
            values = dict(
                self._metrics
            )

        return DomainDiscoveryFabricMetrics(
            cycles=values["cycles"],
            source_discoveries=values[
                "source_discoveries"
            ],
            candidates_seen=values[
                "candidates_seen"
            ],
            candidates_accepted=values[
                "candidates_accepted"
            ],
            inserted=values["inserted"],
            duplicates=values["duplicates"],
            invalid=values["invalid"],
            claimed=values["claimed"],
            completed=values["completed"],
            failed=values["failed"],
            retried=values["retried"],
            recovered_leases=values[
                "recovered_leases"
            ],
            activation_failures=values[
                "activation_failures"
            ],
        )

    # ============================================================
    # Candidate conversion
    # ============================================================

    @staticmethod
    def _record_from_candidate(
        candidate: DomainCandidate,
    ) -> Optional[
        DomainDiscoveryRecord
    ]:
        if not isinstance(
            candidate,
            DomainCandidate,
        ):
            return None

        hostname = candidate.hostname
        url = candidate.url
        source = candidate.source

        if (
            not isinstance(hostname, str)
            or not hostname.strip()
        ):
            return None

        if (
            not isinstance(url, str)
            or not url.strip()
        ):
            return None

        if (
            not isinstance(source, str)
            or not source.strip()
        ):
            return None

        return DomainDiscoveryRecord(
            hostname=(
                hostname
                .strip()
                .lower()
                .rstrip(".")
            ),
            url=url.strip(),
            source=source.strip(),
            evidence=candidate.evidence,
            discovered_at=(
                candidate.discovered_at
            ),
            metadata=(
                dict(candidate.metadata)
                if isinstance(
                    candidate.metadata,
                    dict,
                )
                else {}
            ),
        )

    # ============================================================
    # Discovery ingestion
    # ============================================================

    def discover_context(
        self,
        context: DomainDiscoveryContext,
    ) -> dict[str, int]:
        if not isinstance(
            context,
            DomainDiscoveryContext,
        ):
            raise TypeError(
                "context must be DomainDiscoveryContext"
            )

        candidates = self.registry.discover(
            context
        )

        self._increment(
            "source_discoveries"
        )

        self._increment(
            "candidates_seen",
            len(candidates),
        )

        accepted = (
            self.candidate_pipeline
            .process_many_for_fabric(
                candidates
            )
        )

        self._increment(
            "candidates_accepted",
            len(accepted),
        )

        records = []

        for candidate in accepted:
            record = (
                self._record_from_candidate(
                    candidate
                )
            )

            if record is not None:
                records.append(record)

        result = self.fabric.enqueue_many(
            records
        )

        self._increment(
            "inserted",
            result["inserted"],
        )

        self._increment(
            "duplicates",
            result["duplicates"],
        )

        invalid_count = (
            result["invalid"]
            + (
                len(accepted)
                - len(records)
            )
        )

        self._increment(
            "invalid",
            invalid_count,
        )

        return {
            "discovered": len(candidates),
            "accepted": len(accepted),
            "records": len(records),
            "inserted": result[
                "inserted"
            ],
            "duplicates": result[
                "duplicates"
            ],
            "invalid": invalid_count,
        }

    def discover_contexts(
        self,
        contexts: Iterable[
            DomainDiscoveryContext
        ],
    ) -> dict[str, int]:
        context_list = list(
            contexts
        )

        if not context_list:
            return {
                "contexts": 0,
                "discovered": 0,
                "accepted": 0,
                "records": 0,
                "inserted": 0,
                "duplicates": 0,
                "invalid": 0,
                "failures": 0,
            }

        context_list = context_list[
            : self.max_contexts_per_cycle
        ]

        totals = {
            "contexts": len(context_list),
            "discovered": 0,
            "accepted": 0,
            "records": 0,
            "inserted": 0,
            "duplicates": 0,
            "invalid": 0,
            "failures": 0,
        }

        worker_count = min(
            self.discovery_workers,
            len(context_list),
        )

        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix=(
                "domain-discovery"
            ),
        ) as executor:
            futures = [
                executor.submit(
                    self.discover_context,
                    context,
                )
                for context in context_list
            ]

            for future in as_completed(
                futures
            ):
                try:
                    result = future.result()
                except Exception:
                    totals["failures"] += 1
                    continue

                for key in (
                    "discovered",
                    "accepted",
                    "records",
                    "inserted",
                    "duplicates",
                    "invalid",
                ):
                    totals[key] += result[
                        key
                    ]

        return totals

    # ============================================================
    # Activation
    # ============================================================

    def _activate(
        self,
        item: ClaimedDomainDiscovery,
    ) -> bool:
        if self.activation_callback is None:
            return True

        return bool(
            self.activation_callback(
                item.hostname,
                item.url,
                int(
                    round(
                        item.priority
                    )
                ),
                item.source,
            )
        )

    # ============================================================
    # Placement-aware work discovery
    # ============================================================

    def _discover_work_shards(
        self,
    ) -> list[int]:
        """
        Ask the durable fabric for physical buckets that currently
        contain executable work.

        No global physical-topology enumeration occurs here.
        """

        try:
            shards = (
                self.fabric.shards_with_work()
            )
        except Exception:
            return []

        result = []

        for shard_id in shards:
            try:
                shard_id = int(
                    shard_id
                )
            except (
                TypeError,
                ValueError,
            ):
                continue

            if (
                0
                <= shard_id
                < self.fabric.shard_count
            ):
                result.append(
                    shard_id
                )

        return sorted(
            set(result)
        )

    def _partition_work(
        self,
        shard_ids: list[int],
    ) -> list[list[int]]:
        """
        Divide currently visible work across execution workers.

        The worker pool is bounded by execution capacity, not by the
        total number of physical buckets.

        Deterministic ordering prevents repeatedly preferring one
        subset of physical buckets.
        """

        if not shard_ids:
            return []

        worker_count = min(
            self.domain_workers,
            len(shard_ids),
        )

        groups = [
            []
            for _ in range(worker_count)
        ]

        for index, shard_id in enumerate(
            shard_ids
        ):
            groups[
                index % worker_count
            ].append(
                shard_id
            )

        return [
            group
            for group in groups
            if group
        ]

    # ============================================================
    # Worker execution
    # ============================================================

    def _worker_id(
        self,
    ) -> str:
        return (
            "domain-worker-"
            f"{uuid.uuid4().hex}"
        )

    def _process_worker_assignment(
        self,
        shard_ids: list[int],
        worker_id: str,
    ) -> dict[str, int]:
        totals = {
            "claimed": 0,
            "processed": 0,
            "completed": 0,
            "failed": 0,
            "retried": 0,
        }

        for shard_id in shard_ids:
            if self._stop_event.is_set():
                break

            lease_owner = (
                f"{worker_id}:"
                f"{int(shard_id)}"
            )

            result = self.process_shard(
                shard_id=shard_id,
                lease_owner=lease_owner,
                stop_event=self._stop_event,
            )

            for key in totals:
                totals[key] += result.get(
                    key,
                    0,
                )

        return totals

    def process_shard(
        self,
        shard_id: int,
        lease_owner: str,
        stop_event: Optional[
            threading.Event
        ] = None,
    ) -> dict[str, int]:
        """
        Drain currently available records from one physical placement.

        Record-level leases remain authoritative. A worker does not
        receive permanent ownership of the physical bucket.
        """

        if stop_event is None:
            stop_event = self._stop_event

        claimed_count = 0
        processed = 0
        completed = 0
        failed = 0
        retried = 0

        while not stop_event.is_set():
            claimed = self.fabric.claim_many(
                shard_id=shard_id,
                limit=self.claim_batch_size,
                lease_owner=lease_owner,
            )

            if not claimed:
                break

            self._increment(
                "claimed",
                len(claimed),
            )

            claimed_count += len(
                claimed
            )

            for item in claimed:
                if stop_event.is_set():
                    return {
                        "claimed": claimed_count,
                        "processed": processed,
                        "completed": completed,
                        "failed": failed,
                        "retried": retried,
                    }

                try:
                    success = self._activate(
                        item
                    )

                    if success:
                        marked = (
                            self.fabric.mark_complete(
                                item,
                                lease_owner=lease_owner,
                            )
                        )

                        if marked:
                            completed += 1
                            self._increment(
                                "completed"
                            )
                        else:
                            failed += 1
                            self._increment(
                                "failed"
                            )
                    else:
                        self.fabric.mark_failed(
                            item,
                            "domain activation returned false",
                            retry=True,
                            lease_owner=lease_owner,
                        )

                        failed += 1
                        retried += 1

                        self._increment(
                            "failed"
                        )

                        self._increment(
                            "retried"
                        )

                        self._increment(
                            "activation_failures"
                        )

                except Exception as exc:
                    try:
                        self.fabric.mark_failed(
                            item,
                            repr(exc),
                            retry=True,
                            lease_owner=lease_owner,
                        )
                    except Exception:
                        pass

                    failed += 1
                    retried += 1

                    self._increment(
                        "failed"
                    )

                    self._increment(
                        "retried"
                    )

                    self._increment(
                        "activation_failures"
                    )

                processed += 1

        return {
            "claimed": claimed_count,
            "processed": processed,
            "completed": completed,
            "failed": failed,
            "retried": retried,
        }

    # ============================================================
    # Global work scheduler
    # ============================================================

    def process_available(
        self,
        max_rounds: int = 1,
    ) -> dict[str, int]:
        """
        Work-conserving placement-aware scheduler.

        It discovers only currently work-bearing physical placements,
        divides them among a bounded worker pool, and lets each worker
        acquire record-level leases.

        Worker count therefore remains independent of:
            - logical partition count
            - physical bucket count
            - total domain count
            - total URL count
        """

        max_rounds = max(
            1,
            int(max_rounds),
        )

        totals = {
            "claimed": 0,
            "processed": 0,
            "completed": 0,
            "failed": 0,
            "retried": 0,
        }

        for _ in range(max_rounds):
            if self._stop_event.is_set():
                break

            shard_ids = (
                self._discover_work_shards()
            )

            if not shard_ids:
                break

            assignments = (
                self._partition_work(
                    shard_ids
                )
            )

            if not assignments:
                break

            with ThreadPoolExecutor(
                max_workers=min(
                    self.domain_workers,
                    len(assignments),
                ),
                thread_name_prefix=(
                    "domain-fabric-worker"
                ),
            ) as executor:
                futures = []

                for assignment in assignments:
                    futures.append(
                        executor.submit(
                            self._process_worker_assignment,
                            assignment,
                            self._worker_id(),
                        )
                    )

                round_processed = 0

                for future in as_completed(
                    futures
                ):
                    try:
                        result = future.result()
                    except Exception:
                        continue

                    for key in totals:
                        totals[key] += result.get(
                            key,
                            0,
                        )

                    round_processed += (
                        result.get(
                            "processed",
                            0,
                        )
                    )

            if round_processed == 0:
                break

        return totals

    # ============================================================
    # Lease recovery
    # ============================================================

    def recover_expired_leases(
        self,
        force: bool = False,
    ) -> int:
        now = time.time()

        if (
            not force
            and (
                now
                - self._last_lease_recovery
                < self.lease_recovery_interval
            )
        ):
            return 0

        recovered = (
            self.fabric.recover_expired_leases()
        )

        self._last_lease_recovery = now

        if recovered:
            self._increment(
                "recovered_leases",
                recovered,
            )

        return recovered

    # ============================================================
    # Complete cycle
    # ============================================================

    def run_cycle(
        self,
        contexts: Optional[
            Iterable[
                DomainDiscoveryContext
            ]
        ] = None,
    ) -> dict[str, Any]:
        self._increment(
            "cycles"
        )

        recovered = (
            self.recover_expired_leases()
        )

        discovery_result = {
            "contexts": 0,
            "discovered": 0,
            "accepted": 0,
            "records": 0,
            "inserted": 0,
            "duplicates": 0,
            "invalid": 0,
            "failures": 0,
        }

        if contexts is not None:
            discovery_result = (
                self.discover_contexts(
                    contexts
                )
            )

        processing_result = (
            self.process_available(
                max_rounds=1
            )
        )

        return {
            "recovered_leases": recovered,
            "discovery": discovery_result,
            "processing": processing_result,
            "fabric": self.fabric.stats(),
            "metrics": self.metrics(),
        }

    # ============================================================
    # Continuous operation
    # ============================================================

    def run(
        self,
        contexts: Optional[
            Iterable[
                DomainDiscoveryContext
            ]
        ] = None,
        max_cycles: Optional[int] = None,
    ) -> None:
        cycles = 0

        while not self._stop_event.is_set():
            if (
                max_cycles is not None
                and cycles >= int(max_cycles)
            ):
                break

            self.run_cycle(
                contexts=contexts
            )

            cycles += 1

            if self.cycle_interval > 0:
                self._stop_event.wait(
                    self.cycle_interval
                )

    def stop(self) -> None:
        self._stop_event.set()

    def stopped(self) -> bool:
        return self._stop_event.is_set()
