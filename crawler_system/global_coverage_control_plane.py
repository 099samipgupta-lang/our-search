from __future__ import annotations

"""
OUR SEARCH — Global Coverage Control Plane

Phase 8
Brick 8.1 — Global Coverage Control Plane

Version:
    global-coverage-control-plane.v1

Mission:
    Coordinate enormous public-Web coverage without introducing a
    centralized global execution bottleneck.

Design target:
    Billions of public websites and an enormous URL universe.

This layer tracks coverage state at scalable logical partitions.
It does not replace the crawler, discovery fabric, queues, routing,
worker supervision, recovery, storage, or index systems.
"""

import hashlib
import threading
from dataclasses import dataclass
from time import time
from typing import Iterable, Optional


COVERAGE_CONTROL_PLANE_VERSION = (
    "global-coverage-control-plane.v1"
)


@dataclass(frozen=True)
class CoveragePartitionState:
    partition_id: int
    discovered: int
    queued: int
    active: int
    completed: int
    failed: int
    unknown: int
    last_progress_at: float
    coverage_epoch: int


@dataclass(frozen=True)
class CoverageUpdate:
    partition_id: int
    discovered: int = 0
    queued: int = 0
    active: int = 0
    completed: int = 0
    failed: int = 0
    unknown: int = 0


@dataclass(frozen=True)
class CoverageCapacity:
    partitions: int
    tracked_partitions: int
    discovered: int
    queued: int
    active: int
    completed: int
    failed: int
    unknown: int


class GlobalCoverageControlPlane:
    """
    Partitioned coverage accounting and coordination.

    Coverage is keyed by stable logical partitions rather than by
    individual workers. Worker count and machine topology therefore
    do not change coverage identity.
    """

    def __init__(
        self,
        partition_count: int = 1_048_576,
        update_batch_size: int = 10_000,
    ):
        if partition_count <= 0:
            raise ValueError(
                "partition_count must be positive"
            )

        if update_batch_size <= 0:
            raise ValueError(
                "update_batch_size must be positive"
            )

        self.partition_count = int(
            partition_count
        )

        self.update_batch_size = int(
            update_batch_size
        )

        self._lock = threading.RLock()

        self._states: dict[
            int,
            CoveragePartitionState,
        ] = {}

        self._stats = {
            "updates": 0,
            "partitions_created": 0,
            "coverage_epochs": 0,
            "progress_updates": 0,
        }

    # ------------------------------------------------------------------
    # STABLE PARTITIONING
    # ------------------------------------------------------------------

    def partition_for(
        self,
        value: str,
    ) -> int:
        """
        Stable identity → logical coverage partition.

        The result depends only on the supplied identity and the
        configured logical partition namespace.
        """

        value = str(value).strip()

        if not value:
            raise ValueError(
                "coverage identity is required"
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
    # STATE
    # ------------------------------------------------------------------

    def _state_for(
        self,
        partition_id: int,
    ) -> CoveragePartitionState:
        partition_id = int(partition_id)

        state = self._states.get(
            partition_id
        )

        if state is not None:
            return state

        state = CoveragePartitionState(
            partition_id=partition_id,
            discovered=0,
            queued=0,
            active=0,
            completed=0,
            failed=0,
            unknown=0,
            last_progress_at=time(),
            coverage_epoch=0,
        )

        self._states[
            partition_id
        ] = state

        self._stats[
            "partitions_created"
        ] += 1

        return state

    def get(
        self,
        partition_id: int,
    ) -> CoveragePartitionState:
        with self._lock:
            return self._state_for(
                partition_id
            )

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------

    def update(
        self,
        update: CoverageUpdate,
    ) -> CoveragePartitionState:
        partition_id = int(
            update.partition_id
        )

        if not (
            0 <= partition_id
            < self.partition_count
        ):
            raise ValueError(
                "partition_id is outside "
                "the logical namespace"
            )

        with self._lock:
            current = self._state_for(
                partition_id
            )

            epoch = (
                current.coverage_epoch + 1
            )

            state = CoveragePartitionState(
                partition_id=partition_id,
                discovered=max(
                    0,
                    current.discovered
                    + int(update.discovered),
                ),
                queued=max(
                    0,
                    current.queued
                    + int(update.queued),
                ),
                active=max(
                    0,
                    current.active
                    + int(update.active),
                ),
                completed=max(
                    0,
                    current.completed
                    + int(update.completed),
                ),
                failed=max(
                    0,
                    current.failed
                    + int(update.failed),
                ),
                unknown=max(
                    0,
                    current.unknown
                    + int(update.unknown),
                ),
                last_progress_at=time(),
                coverage_epoch=epoch,
            )

            self._states[
                partition_id
            ] = state

            self._stats[
                "updates"
            ] += 1

            self._stats[
                "coverage_epochs"
            ] += 1

            self._stats[
                "progress_updates"
            ] += 1

            return state

    def update_many(
        self,
        updates: Iterable[CoverageUpdate],
    ) -> int:
        count = 0

        for update in updates:
            if count >= self.update_batch_size:
                break

            self.update(update)
            count += 1

        return count

    # ------------------------------------------------------------------
    # DISCOVERY EVENTS
    # ------------------------------------------------------------------

    def record_discovery(
        self,
        identity: str,
        queued: bool = True,
    ) -> CoveragePartitionState:
        return self.update(
            CoverageUpdate(
                partition_id=self.partition_for(
                    identity
                ),
                discovered=1,
                queued=1 if queued else 0,
                unknown=-1,
            )
        )

    def record_activation(
        self,
        identity: str,
    ) -> CoveragePartitionState:
        return self.update(
            CoverageUpdate(
                partition_id=self.partition_for(
                    identity
                ),
                queued=-1,
                active=1,
            )
        )

    def record_completion(
        self,
        identity: str,
    ) -> CoveragePartitionState:
        return self.update(
            CoverageUpdate(
                partition_id=self.partition_for(
                    identity
                ),
                active=-1,
                completed=1,
            )
        )

    def record_failure(
        self,
        identity: str,
    ) -> CoveragePartitionState:
        return self.update(
            CoverageUpdate(
                partition_id=self.partition_for(
                    identity
                ),
                active=-1,
                failed=1,
            )
        )

    def record_unknown(
        self,
        identity: str,
    ) -> CoveragePartitionState:
        return self.update(
            CoverageUpdate(
                partition_id=self.partition_for(
                    identity
                ),
                unknown=1,
            )
        )

    # ------------------------------------------------------------------
    # COVERAGE STATE
    # ------------------------------------------------------------------

    def tracked_partitions(
        self,
    ) -> list[int]:
        with self._lock:
            return sorted(
                self._states.keys()
            )

    def active_partitions(
        self,
    ) -> list[int]:
        with self._lock:
            return [
                partition_id
                for partition_id, state
                in self._states.items()
                if (
                    state.active > 0
                    or state.queued > 0
                )
            ]

    def incomplete_partitions(
        self,
    ) -> list[int]:
        with self._lock:
            return [
                partition_id
                for partition_id, state
                in self._states.items()
                if (
                    state.unknown > 0
                    or state.queued > 0
                    or state.active > 0
                )
            ]

    # ------------------------------------------------------------------
    # COVERAGE PROGRESS
    # ------------------------------------------------------------------

    def progress(
        self,
        partition_id: int,
    ) -> float:
        state = self.get(
            partition_id
        )

        known = (
            state.completed
            + state.failed
            + state.active
            + state.queued
        )

        if known <= 0:
            return 0.0

        return (
            state.completed
            / known
        )

    def stale_partitions(
        self,
        max_age: float,
    ) -> list[int]:
        if max_age < 0:
            raise ValueError(
                "max_age cannot be negative"
            )

        cutoff = (
            time() - max_age
        )

        with self._lock:
            return [
                partition_id
                for partition_id, state
                in self._states.items()
                if (
                    state.last_progress_at
                    < cutoff
                    and (
                        state.queued > 0
                        or state.active > 0
                        or state.unknown > 0
                    )
                )
            ]

    # ------------------------------------------------------------------
    # COVERAGE EPOCH
    # ------------------------------------------------------------------

    def begin_epoch(
        self,
        partition_id: int,
    ) -> int:
        partition_id = int(
            partition_id
        )

        with self._lock:
            state = self._state_for(
                partition_id
            )

            epoch = (
                state.coverage_epoch + 1
            )

            self._states[
                partition_id
            ] = CoveragePartitionState(
                partition_id=state.partition_id,
                discovered=state.discovered,
                queued=state.queued,
                active=state.active,
                completed=state.completed,
                failed=state.failed,
                unknown=state.unknown,
                last_progress_at=state.last_progress_at,
                coverage_epoch=epoch,
            )

            self._stats[
                "coverage_epochs"
            ] += 1

            return epoch

    # ------------------------------------------------------------------
    # CAPACITY
    # ------------------------------------------------------------------

    def capacity(
        self,
    ) -> CoverageCapacity:
        with self._lock:
            states = list(
                self._states.values()
            )

            return CoverageCapacity(
                partitions=self.partition_count,
                tracked_partitions=len(
                    states
                ),
                discovered=sum(
                    state.discovered
                    for state in states
                ),
                queued=sum(
                    state.queued
                    for state in states
                ),
                active=sum(
                    state.active
                    for state in states
                ),
                completed=sum(
                    state.completed
                    for state in states
                ),
                failed=sum(
                    state.failed
                    for state in states
                ),
                unknown=sum(
                    state.unknown
                    for state in states
                ),
            )

    # ------------------------------------------------------------------
    # SNAPSHOT
    # ------------------------------------------------------------------

    def snapshot(
        self,
        partition_ids: Optional[
            Iterable[int]
        ] = None,
    ) -> dict[int, CoveragePartitionState]:
        with self._lock:
            if partition_ids is None:
                ids = list(
                    self._states.keys()
                )
            else:
                ids = [
                    int(value)
                    for value
                    in partition_ids
                ]

            return {
                partition_id: self._state_for(
                    partition_id
                )
                for partition_id in ids
            }

    # ------------------------------------------------------------------
    # STATS
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, object]:
        capacity = self.capacity()

        return {
            **self._stats,
            "version": (
                COVERAGE_CONTROL_PLANE_VERSION
            ),
            "partition_count": (
                self.partition_count
            ),
            "update_batch_size": (
                self.update_batch_size
            ),
            "tracked_partitions": (
                capacity.tracked_partitions
            ),
            "discovered": (
                capacity.discovered
            ),
            "queued": capacity.queued,
            "active": capacity.active,
            "completed": capacity.completed,
            "failed": capacity.failed,
            "unknown": capacity.unknown,
            "fixed_global_execution_limit": False,
        }


__all__ = [
    "COVERAGE_CONTROL_PLANE_VERSION",
    "CoveragePartitionState",
    "CoverageUpdate",
    "CoverageCapacity",
    "GlobalCoverageControlPlane",
]
