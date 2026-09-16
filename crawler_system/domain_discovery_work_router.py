from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from crawler_system.domain_discovery_fabric import (
    DomainDiscoveryFabric,
)
from crawler_system.domain_discovery_work_queue import (
    DiscoveryWorkItem,
    DomainDiscoveryWorkQueue,
    WorkLease,
)


@dataclass(frozen=True)
class RoutedDiscoveryWork:
    work: DiscoveryWorkItem
    lease: WorkLease
    logical_partition: int
    physical_bucket: int


@dataclass(frozen=True)
class WorkRouterCapacity:
    queue_capacity: int
    claimed: int
    active_routes: int


class DomainDiscoveryWorkRouter:
    """
    Global scheduling/router layer between durable discovery work and
    the placement-aware discovery fabric.

    Responsibilities:

        queue
          ↓
        logical partition
          ↓
        durable placement
          ↓
        physical bucket
          ↓
        fenced worker lease

    The router deliberately does not make physical buckets the global
    scheduling unit.

    Logical work identity remains stable while physical placement can
    evolve independently.

    The queue owns execution state.
    The placement fabric owns storage placement.
    The router owns the relationship between them.
    """

    VERSION = "domain-discovery-work-router.v1"

    def __init__(
        self,
        queue: DomainDiscoveryWorkQueue,
        fabric: DomainDiscoveryFabric,
        max_active_routes: int = 100_000,
    ):
        if not isinstance(
            queue,
            DomainDiscoveryWorkQueue,
        ):
            raise TypeError(
                "queue must be DomainDiscoveryWorkQueue"
            )

        if not isinstance(
            fabric,
            DomainDiscoveryFabric,
        ):
            raise TypeError(
                "fabric must be DomainDiscoveryFabric"
            )

        self.queue = queue
        self.fabric = fabric

        self.max_active_routes = max(
            1,
            int(max_active_routes),
        )

        self._lock = threading.RLock()

        self._active_routes: dict[
            str,
            RoutedDiscoveryWork,
        ] = {}

        self._worker_routes: dict[
            str,
            set[str],
        ] = {}

    # ============================================================
    # Logical partition resolution
    # ============================================================

    def logical_partition_for_hostname(
        self,
        hostname: str,
    ) -> int:
        partitioner = getattr(
            self.fabric,
            "partitioner",
            None,
        )

        if partitioner is None:
            raise RuntimeError(
                "fabric does not expose a partitioner"
            )

        return int(
            partitioner.partition(
                hostname
            )
        )

    # ============================================================
    # Placement resolution
    # ============================================================

    def physical_bucket_for_partition(
        self,
        logical_partition: int,
    ) -> int:
        placement_map = getattr(
            self.fabric,
            "placement_map",
            None,
        )

        if placement_map is None:
            raise RuntimeError(
                "fabric does not expose placement_map"
            )

        return int(
            placement_map.get_or_create(
                int(logical_partition)
            )
        )

    def physical_bucket_for_hostname(
        self,
        hostname: str,
    ) -> int:
        return int(
            self.fabric.shard_for_hostname(
                hostname
            )
        )

    # ============================================================
    # Routing
    # ============================================================

    def _route(
        self,
        work: DiscoveryWorkItem,
        lease: WorkLease,
    ) -> RoutedDiscoveryWork:
        logical_partition = int(
            work.logical_partition
        )

        physical_bucket = (
            self.physical_bucket_for_partition(
                logical_partition
            )
        )

        return RoutedDiscoveryWork(
            work=work,
            lease=lease,
            logical_partition=logical_partition,
            physical_bucket=physical_bucket,
        )

    # ============================================================
    # Acquisition
    # ============================================================

    def acquire(
        self,
        worker_id: str,
        limit: int = 100,
    ) -> list[RoutedDiscoveryWork]:
        if not worker_id:
            raise ValueError(
                "worker_id is required"
            )

        limit = max(
            1,
            int(limit),
        )

        with self._lock:
            remaining = (
                self.max_active_routes
                - len(self._active_routes)
            )

            if remaining <= 0:
                return []

            limit = min(
                limit,
                remaining,
            )

        pairs = self.queue.claim(
            worker_id=worker_id,
            limit=limit,
        )

        routed = []

        with self._lock:
            for work, lease in pairs:
                route = self._route(
                    work,
                    lease,
                )

                self._active_routes[
                    work.work_id
                ] = route

                self._worker_routes.setdefault(
                    worker_id,
                    set(),
                ).add(
                    work.work_id
                )

                routed.append(route)

        return routed

    # ============================================================
    # Lease lifecycle
    # ============================================================

    def renew(
        self,
        route: RoutedDiscoveryWork,
    ) -> bool:
        renewed = self.queue.renew(
            route.lease
        )

        if not renewed:
            return False

        with self._lock:
            current = self._active_routes.get(
                route.work.work_id
            )

            if current is None:
                return False

            refreshed_lease = WorkLease(
                work_id=route.lease.work_id,
                worker_id=route.lease.worker_id,
                fencing_token=route.lease.fencing_token,
                leased_at=route.lease.leased_at,
                lease_until=(
                    time.time()
                    + self.queue.lease_seconds
                ),
            )

            self._active_routes[
                route.work.work_id
            ] = RoutedDiscoveryWork(
                work=route.work,
                lease=refreshed_lease,
                logical_partition=(
                    route.logical_partition
                ),
                physical_bucket=(
                    route.physical_bucket
                ),
            )

        return True

    def complete(
        self,
        route: RoutedDiscoveryWork,
    ) -> bool:
        completed = self.queue.complete(
            route.lease
        )

        if completed:
            self._remove_active(
                route
            )

        return completed

    def fail(
        self,
        route: RoutedDiscoveryWork,
        error: str,
        retry: bool = True,
        retry_delay: float = 0.0,
    ) -> bool:
        failed = self.queue.fail(
            lease=route.lease,
            error=error,
            retry=retry,
            retry_delay=retry_delay,
        )

        if failed:
            self._remove_active(
                route
            )

        return failed

    def _remove_active(
        self,
        route: RoutedDiscoveryWork,
    ) -> None:
        with self._lock:
            self._active_routes.pop(
                route.work.work_id,
                None,
            )

            worker_routes = (
                self._worker_routes.get(
                    route.lease.worker_id
                )
            )

            if worker_routes is not None:
                worker_routes.discard(
                    route.work.work_id
                )

                if not worker_routes:
                    self._worker_routes.pop(
                        route.lease.worker_id,
                        None,
                    )

    # ============================================================
    # Failure recovery
    # ============================================================

    def recover_expired(
        self,
        limit: int = 10_000,
    ) -> int:
        recovered = (
            self.queue.recover_expired(
                limit=limit
            )
        )

        if recovered:
            with self._lock:
                active_ids = set(
                    self._active_routes
                )

                for work_id in active_ids:
                    route = (
                        self._active_routes[
                            work_id
                        ]
                    )

                    if route.lease.lease_until <= time.time():
                        self._remove_active(
                            route
                        )

        return recovered

    # ============================================================
    # Worker lifecycle
    # ============================================================

    def release_worker(
        self,
        worker_id: str,
        error: str = "worker released",
    ) -> int:
        with self._lock:
            work_ids = set(
                self._worker_routes.get(
                    worker_id,
                    set(),
                )
            )

        released = 0

        for work_id in work_ids:
            with self._lock:
                route = (
                    self._active_routes.get(
                        work_id
                    )
                )

            if route is None:
                continue

            if self.fail(
                route,
                error=error,
                retry=True,
            ):
                released += 1

        return released

    # ============================================================
    # Work stealing
    # ============================================================

    def acquire_from_any_partition(
        self,
        worker_id: str,
        limit: int = 100,
    ) -> list[RoutedDiscoveryWork]:
        """
        Pull work without requiring the worker to know which logical
        partition or physical bucket contains it.

        This is the core work-stealing interface.
        """

        return self.acquire(
            worker_id=worker_id,
            limit=limit,
        )

    # ============================================================
    # Backpressure
    # ============================================================

    def capacity(
        self,
    ) -> WorkRouterCapacity:
        queue_capacity = (
            self.queue.capacity()
        )

        with self._lock:
            active_routes = len(
                self._active_routes
            )

        return WorkRouterCapacity(
            queue_capacity=(
                queue_capacity.available_capacity
            ),
            claimed=(
                queue_capacity.current_inflight
            ),
            active_routes=active_routes,
        )

    def can_route(
        self,
        requested: int = 1,
    ) -> bool:
        requested = max(
            1,
            int(requested),
        )

        capacity = self.capacity()

        return (
            capacity.queue_capacity
            >= requested
            and (
                self.max_active_routes
                - capacity.active_routes
            ) >= requested
        )

    # ============================================================
    # Active work
    # ============================================================

    def active_routes(
        self,
    ) -> list[RoutedDiscoveryWork]:
        with self._lock:
            return list(
                self._active_routes.values()
            )

    def active_for_worker(
        self,
        worker_id: str,
    ) -> list[RoutedDiscoveryWork]:
        with self._lock:
            work_ids = set(
                self._worker_routes.get(
                    worker_id,
                    set(),
                )
            )

            return [
                self._active_routes[
                    work_id
                ]
                for work_id in work_ids
                if work_id
                in self._active_routes
            ]

    # ============================================================
    # Routing metadata
    # ============================================================

    def route_metadata(
        self,
        route: RoutedDiscoveryWork,
    ) -> dict[str, int | str]:
        return {
            "work_id": route.work.work_id,
            "hostname": route.work.hostname,
            "source": route.work.source,
            "logical_partition": (
                route.logical_partition
            ),
            "physical_bucket": (
                route.physical_bucket
            ),
            "worker_id": (
                route.lease.worker_id
            ),
            "fencing_token": (
                route.lease.fencing_token
            ),
        }

    # ============================================================
    # Global state
    # ============================================================

    def stats(
        self,
    ) -> dict[str, int | float]:
        queue_stats = (
            self.queue.stats()
        )

        capacity = self.capacity()

        with self._lock:
            workers = len(
                self._worker_routes
            )

        return {
            **queue_stats,
            "active_routes": (
                capacity.active_routes
            ),
            "active_workers": workers,
            "available_capacity": (
                capacity.queue_capacity
            ),
            "max_active_routes": (
                self.max_active_routes
            ),
        }
