from __future__ import annotations

"""
OUR SEARCH — Global Scale Bottleneck Elimination

Phase 7
Brick 7.5

Version:
    global-scale-bottleneck-elimination.v1

Mission:
    Prepare the discovery/crawler/storage/index architecture for an
    enormous public Web containing billions of websites and an enormous
    URL universe.

This module provides scale-safe coordination primitives without making
any single process, worker, database, queue, or coordinator the global
execution bottleneck.

Design principles:

    1. Partition work before execution.
    2. Route work deterministically.
    3. Keep state local to its ownership domain.
    4. Process in bounded batches.
    5. Never require global scans for normal operation.
    6. Avoid global locks.
    7. Support horizontal worker expansion.
    8. Preserve fairness across partitions and sources.
    9. Keep slow/failing partitions isolated.
   10. Make control-plane operations incremental.
   11. Allow capacity to expand without changing work identity.
   12. Keep recovery independent from normal processing.

This is an architectural coordination layer. It does not replace the
existing queue, router, scheduler, worker supervisor, crawler,
recovery, storage, or index systems.
"""

import hashlib
import threading
from dataclasses import dataclass
from time import monotonic, time
from typing import Any, Iterable, Mapping, Optional


BOTTLENECK_ELIMINATION_VERSION = (
    "global-scale-bottleneck-elimination.v1"
)


@dataclass(frozen=True)
class ScalePartition:
    partition_id: int
    namespace: str
    capacity: int
    active: bool = True


@dataclass(frozen=True)
class ScaleAssignment:
    work_id: str
    partition_id: int
    worker_id: str
    sequence: int


@dataclass(frozen=True)
class ScaleCapacity:
    partitions: int
    workers: int
    active_assignments: int
    pending_assignments: int
    isolated_partitions: int


@dataclass(frozen=True)
class PartitionHealth:
    partition_id: int
    active: bool
    isolated: bool
    failure_count: int
    backoff_until: float
    last_progress_at: float


class GlobalScaleBottleneckElimination:
    """
    Horizontal scale coordinator.

    The object intentionally keeps coordination state partitioned in
    memory. Durable ownership remains with the existing production
    control-plane components.

    No operation here requires a global database scan.
    """

    def __init__(
        self,
        partition_count: int = 1_048_576,
        worker_capacity: int = 10_000,
        assignment_batch_size: int = 1_000,
        failure_backoff: float = 30.0,
        max_failure_backoff: float = 3_600.0,
    ):
        if partition_count <= 0:
            raise ValueError(
                "partition_count must be positive"
            )

        if worker_capacity <= 0:
            raise ValueError(
                "worker_capacity must be positive"
            )

        if assignment_batch_size <= 0:
            raise ValueError(
                "assignment_batch_size must be positive"
            )

        if failure_backoff <= 0:
            raise ValueError(
                "failure_backoff must be positive"
            )

        if max_failure_backoff < failure_backoff:
            raise ValueError(
                "max_failure_backoff cannot be smaller "
                "than failure_backoff"
            )

        self.partition_count = int(
            partition_count
        )
        self.worker_capacity = int(
            worker_capacity
        )
        self.assignment_batch_size = int(
            assignment_batch_size
        )
        self.failure_backoff = float(
            failure_backoff
        )
        self.max_failure_backoff = float(
            max_failure_backoff
        )

        self._lock = threading.RLock()

        self._workers: dict[
            str,
            float,
        ] = {}

        self._worker_load: dict[
            str,
            int,
        ] = {}

        self._partition_health: dict[
            int,
            PartitionHealth,
        ] = {}

        self._partition_sequences: dict[
            int,
            int,
        ] = {}

        self._active_assignments: dict[
            str,
            ScaleAssignment,
        ] = {}

        self._pending_by_partition: dict[
            int,
            int,
        ] = {}

        self._isolated: set[int] = set()

        self._stats = {
            "registered_workers": 0,
            "assignments_created": 0,
            "assignments_released": 0,
            "partitions_isolated": 0,
            "partitions_recovered": 0,
            "worker_rebalances": 0,
            "partition_rebalances": 0,
            "backpressure_events": 0,
            "fairness_rotations": 0,
        }

        self._fairness_cursor = 0

    # ------------------------------------------------------------------
    # DETERMINISTIC PARTITIONING
    # ------------------------------------------------------------------

    def partition_for(
        self,
        work_id: str,
    ) -> int:
        """
        Stable work → logical partition mapping.

        The mapping depends only on work identity and partition count.
        It does not depend on worker count or process topology.
        """

        value = str(work_id).strip()

        if not value:
            raise ValueError(
                "work_id is required"
            )

        digest = hashlib.sha256(
            value.encode("utf-8")
        ).digest()

        number = int.from_bytes(
            digest[:8],
            "big",
        )

        return number % self.partition_count

    # ------------------------------------------------------------------
    # WORKER CAPACITY
    # ------------------------------------------------------------------

    def register_worker(
        self,
        worker_id: str,
    ) -> bool:
        worker_id = str(
            worker_id
        ).strip()

        if not worker_id:
            raise ValueError(
                "worker_id is required"
            )

        with self._lock:
            now = monotonic()

            if worker_id not in self._workers:
                self._workers[
                    worker_id
                ] = now

                self._worker_load[
                    worker_id
                ] = 0

                self._stats[
                    "registered_workers"
                ] += 1
            else:
                self._workers[
                    worker_id
                ] = now

            return True

    def heartbeat_worker(
        self,
        worker_id: str,
    ) -> bool:
        with self._lock:
            if worker_id not in self._workers:
                return False

            self._workers[
                worker_id
            ] = monotonic()

            return True

    def unregister_worker(
        self,
        worker_id: str,
    ) -> int:
        with self._lock:
            self._workers.pop(
                worker_id,
                None,
            )

            self._worker_load.pop(
                worker_id,
                None,
            )

            assignments = [
                assignment
                for assignment in
                self._active_assignments.values()
                if assignment.worker_id
                == worker_id
            ]

            for assignment in assignments:
                self._active_assignments.pop(
                    assignment.work_id,
                    None,
                )

                self._worker_load[
                    worker_id
                ] = max(
                    0,
                    self._worker_load.get(
                        worker_id,
                        0,
                    ) - 1,
                )

            self._stats[
                "worker_rebalances"
            ] += len(assignments)

            return len(assignments)

    # ------------------------------------------------------------------
    # PARTITION STATE
    # ------------------------------------------------------------------

    def _health_for(
        self,
        partition_id: int,
    ) -> PartitionHealth:
        health = self._partition_health.get(
            partition_id
        )

        if health is not None:
            return health

        health = PartitionHealth(
            partition_id=partition_id,
            active=True,
            isolated=False,
            failure_count=0,
            backoff_until=0.0,
            last_progress_at=time(),
        )

        self._partition_health[
            partition_id
        ] = health

        return health

    def partition_health(
        self,
        partition_id: int,
    ) -> PartitionHealth:
        with self._lock:
            return self._health_for(
                int(partition_id)
            )

    # ------------------------------------------------------------------
    # WORKER SELECTION
    # ------------------------------------------------------------------

    def _eligible_workers(
        self,
    ) -> list[str]:
        if not self._workers:
            return []

        return [
            worker_id
            for worker_id in self._workers
            if self._worker_load.get(
                worker_id,
                0,
            ) < self.worker_capacity
        ]

    def _select_worker(
        self,
        partition_id: int,
    ) -> Optional[str]:
        workers = self._eligible_workers()

        if not workers:
            return None

        workers.sort(
            key=lambda worker_id: (
                self._worker_load.get(
                    worker_id,
                    0,
                ),
                hashlib.sha256(
                    (
                        f"{partition_id}:"
                        f"{worker_id}"
                    ).encode("utf-8")
                ).hexdigest(),
            )
        )

        return workers[0]

    # ------------------------------------------------------------------
    # ASSIGNMENT
    # ------------------------------------------------------------------

    def assign(
        self,
        work_id: str,
        worker_id: Optional[str] = None,
    ) -> Optional[ScaleAssignment]:
        work_id = str(
            work_id
        ).strip()

        if not work_id:
            raise ValueError(
                "work_id is required"
            )

        partition_id = self.partition_for(
            work_id
        )

        with self._lock:
            health = self._health_for(
                partition_id
            )

            if (
                not health.active
                or health.isolated
                or health.backoff_until
                > time()
            ):
                self._pending_by_partition[
                    partition_id
                ] = (
                    self._pending_by_partition.get(
                        partition_id,
                        0,
                    ) + 1
                )

                return None

            existing = self._active_assignments.get(
                work_id
            )

            if existing is not None:
                return existing

            selected_worker = (
                worker_id
                if worker_id in self._workers
                else self._select_worker(
                    partition_id
                )
            )

            if selected_worker is None:
                self._pending_by_partition[
                    partition_id
                ] = (
                    self._pending_by_partition.get(
                        partition_id,
                        0,
                    ) + 1
                )

                self._stats[
                    "backpressure_events"
                ] += 1

                return None

            load = self._worker_load.get(
                selected_worker,
                0,
            )

            if load >= self.worker_capacity:
                self._stats[
                    "backpressure_events"
                ] += 1

                self._pending_by_partition[
                    partition_id
                ] = (
                    self._pending_by_partition.get(
                        partition_id,
                        0,
                    ) + 1
                )

                return None

            sequence = (
                self._partition_sequences.get(
                    partition_id,
                    0,
                ) + 1
            )

            self._partition_sequences[
                partition_id
            ] = sequence

            assignment = ScaleAssignment(
                work_id=work_id,
                partition_id=partition_id,
                worker_id=selected_worker,
                sequence=sequence,
            )

            self._active_assignments[
                work_id
            ] = assignment

            self._worker_load[
                selected_worker
            ] = load + 1

            self._pending_by_partition[
                partition_id
            ] = max(
                0,
                self._pending_by_partition.get(
                    partition_id,
                    0,
                ) - 1,
            )

            self._stats[
                "assignments_created"
            ] += 1

            return assignment

    def assign_many(
        self,
        work_ids: Iterable[str],
        worker_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[ScaleAssignment]:
        maximum = (
            self.assignment_batch_size
            if limit is None
            else min(
                self.assignment_batch_size,
                int(limit),
            )
        )

        if maximum <= 0:
            return []

        assignments: list[
            ScaleAssignment
        ] = []

        for work_id in work_ids:
            if len(assignments) >= maximum:
                break

            assignment = self.assign(
                work_id,
                worker_id=worker_id,
            )

            if assignment is not None:
                assignments.append(
                    assignment
                )

        return assignments

    # ------------------------------------------------------------------
    # FAIRNESS
    # ------------------------------------------------------------------

    def fair_partition_order(
        self,
        partitions: Iterable[int],
    ) -> list[int]:
        """
        Rotates scheduling order so repeatedly active partitions cannot
        permanently starve older or quieter partitions.
        """

        values = sorted(
            {
                int(partition)
                for partition in partitions
                if 0 <= int(partition)
                < self.partition_count
            }
        )

        if not values:
            return []

        cursor = (
            self._fairness_cursor
            % len(values)
        )

        ordered = (
            values[cursor:]
            + values[:cursor]
        )

        self._fairness_cursor = (
            self._fairness_cursor + 1
        )

        self._stats[
            "fairness_rotations"
        ] += 1

        return ordered

    # ------------------------------------------------------------------
    # RELEASE
    # ------------------------------------------------------------------

    def release(
        self,
        work_id: str,
    ) -> bool:
        with self._lock:
            assignment = (
                self._active_assignments.pop(
                    str(work_id),
                    None,
                )
            )

            if assignment is None:
                return False

            worker_id = assignment.worker_id

            self._worker_load[
                worker_id
            ] = max(
                0,
                self._worker_load.get(
                    worker_id,
                    0,
                ) - 1,
            )

            health = self._health_for(
                assignment.partition_id
            )

            self._partition_health[
                assignment.partition_id
            ] = PartitionHealth(
                partition_id=health.partition_id,
                active=health.active,
                isolated=health.isolated,
                failure_count=health.failure_count,
                backoff_until=health.backoff_until,
                last_progress_at=time(),
            )

            self._stats[
                "assignments_released"
            ] += 1

            return True

    # ------------------------------------------------------------------
    # FAILURE ISOLATION
    # ------------------------------------------------------------------

    def record_partition_failure(
        self,
        partition_id: int,
        error: Optional[str] = None,
    ) -> PartitionHealth:
        partition_id = int(
            partition_id
        )

        with self._lock:
            health = self._health_for(
                partition_id
            )

            failures = (
                health.failure_count + 1
            )

            backoff = min(
                self.max_failure_backoff,
                self.failure_backoff
                * (
                    2
                    ** max(
                        0,
                        failures - 1,
                    )
                ),
            )

            isolated = (
                failures >= 3
            )

            updated = PartitionHealth(
                partition_id=partition_id,
                active=not isolated,
                isolated=isolated,
                failure_count=failures,
                backoff_until=(
                    time() + backoff
                ),
                last_progress_at=(
                    health.last_progress_at
                ),
            )

            self._partition_health[
                partition_id
            ] = updated

            if isolated:
                self._isolated.add(
                    partition_id
                )

                self._stats[
                    "partitions_isolated"
                ] += 1

            return updated

    def recover_partition(
        self,
        partition_id: int,
    ) -> bool:
        partition_id = int(
            partition_id
        )

        with self._lock:
            health = self._health_for(
                partition_id
            )

            if not health.isolated:
                return False

            self._partition_health[
                partition_id
            ] = PartitionHealth(
                partition_id=partition_id,
                active=True,
                isolated=False,
                failure_count=0,
                backoff_until=0.0,
                last_progress_at=time(),
            )

            self._isolated.discard(
                partition_id
            )

            self._stats[
                "partitions_recovered"
            ] += 1

            return True

    # ------------------------------------------------------------------
    # REBALANCING
    # ------------------------------------------------------------------

    def rebalance_worker(
        self,
        worker_id: str,
    ) -> list[ScaleAssignment]:
        """
        Release the worker's assignments so they can be reacquired by
        another worker.

        Work identity remains unchanged.
        """

        with self._lock:
            assignments = [
                assignment
                for assignment
                in self._active_assignments.values()
                if assignment.worker_id
                == worker_id
            ]

        released: list[
            ScaleAssignment
        ] = []

        for assignment in assignments:
            if self.release(
                assignment.work_id
            ):
                released.append(
                    assignment
                )

        self._stats[
            "worker_rebalances"
        ] += len(released)

        return released

    def rebalance_partition(
        self,
        partition_id: int,
    ) -> list[ScaleAssignment]:
        partition_id = int(
            partition_id
        )

        with self._lock:
            assignments = [
                assignment
                for assignment
                in self._active_assignments.values()
                if assignment.partition_id
                == partition_id
            ]

        released: list[
            ScaleAssignment
        ] = []

        for assignment in assignments:
            if self.release(
                assignment.work_id
            ):
                released.append(
                    assignment
                )

        self._stats[
            "partition_rebalances"
        ] += len(released)

        return released

    # ------------------------------------------------------------------
    # PENDING WORK
    # ------------------------------------------------------------------

    def pending_for_partition(
        self,
        partition_id: int,
    ) -> int:
        with self._lock:
            return int(
                self._pending_by_partition.get(
                    int(partition_id),
                    0,
                )
            )

    def pending_partitions(
        self,
        limit: Optional[int] = None,
    ) -> list[int]:
        with self._lock:
            values = [
                partition_id
                for partition_id, count
                in self._pending_by_partition.items()
                if count > 0
            ]

        ordered = self.fair_partition_order(
            values
        )

        if limit is not None:
            ordered = ordered[
                : max(
                    0,
                    int(limit),
                )
            ]

        return ordered

    # ------------------------------------------------------------------
    # CONTROL-PLANE TICK
    # ------------------------------------------------------------------

    def tick(
        self,
        worker_timeout: float = 90.0,
    ) -> dict[str, int]:
        now = monotonic()

        expired_workers: list[str] = []

        with self._lock:
            for worker_id, heartbeat in (
                self._workers.items()
            ):
                if (
                    now - heartbeat
                    > worker_timeout
                ):
                    expired_workers.append(
                        worker_id
                    )

        rebalanced = 0

        for worker_id in expired_workers:
            rebalanced += len(
                self.rebalance_worker(
                    worker_id
                )
            )

            with self._lock:
                self._workers.pop(
                    worker_id,
                    None,
                )

                self._worker_load.pop(
                    worker_id,
                    None,
                )

        recovered = 0

        current_time = time()

        with self._lock:
            isolated = list(
                self._isolated
            )

        for partition_id in isolated:
            health = self.partition_health(
                partition_id
            )

            if (
                health.backoff_until
                <= current_time
            ):
                if self.recover_partition(
                    partition_id
                ):
                    recovered += 1

        return {
            "expired_workers": len(
                expired_workers
            ),
            "rebalanced_assignments": (
                rebalanced
            ),
            "recovered_partitions": (
                recovered
            ),
        }

    # ------------------------------------------------------------------
    # ACTIVE ASSIGNMENTS
    # ------------------------------------------------------------------

    def assignment(
        self,
        work_id: str,
    ) -> Optional[ScaleAssignment]:
        with self._lock:
            return self._active_assignments.get(
                str(work_id)
            )

    def active_assignments(
        self,
        worker_id: Optional[str] = None,
    ) -> list[ScaleAssignment]:
        with self._lock:
            values = list(
                self._active_assignments.values()
            )

        if worker_id is not None:
            values = [
                assignment
                for assignment in values
                if assignment.worker_id
                == worker_id
            ]

        return values

    # ------------------------------------------------------------------
    # CAPACITY
    # ------------------------------------------------------------------

    def capacity(self) -> ScaleCapacity:
        with self._lock:
            pending = sum(
                self._pending_by_partition.values()
            )

            return ScaleCapacity(
                partitions=self.partition_count,
                workers=len(
                    self._workers
                ),
                active_assignments=len(
                    self._active_assignments
                ),
                pending_assignments=pending,
                isolated_partitions=len(
                    self._isolated
                ),
            )

    # ------------------------------------------------------------------
    # OBSERVABILITY
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        with self._lock:
            capacity = self.capacity()

            return {
                **self._stats,
                "version": (
                    BOTTLENECK_ELIMINATION_VERSION
                ),
                "partition_count": (
                    self.partition_count
                ),
                "worker_capacity": (
                    self.worker_capacity
                ),
                "assignment_batch_size": (
                    self.assignment_batch_size
                ),
                "workers": capacity.workers,
                "active_assignments": (
                    capacity.active_assignments
                ),
                "pending_assignments": (
                    capacity.pending_assignments
                ),
                "isolated_partitions": (
                    capacity.isolated_partitions
                ),
            }


__all__ = [
    "BOTTLENECK_ELIMINATION_VERSION",
    "ScalePartition",
    "ScaleAssignment",
    "ScaleCapacity",
    "PartitionHealth",
    "GlobalScaleBottleneckElimination",
]
