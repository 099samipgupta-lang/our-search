from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from crawler_system.domain_discovery_scheduler_catalog import (
    DomainDiscoverySchedulerCatalog,
    LogicalWorkState,
    SchedulerLease,
    SchedulerWorker,
)
from crawler_system.domain_discovery_work_queue import (
    DomainDiscoveryWorkQueue,
)
from crawler_system.domain_discovery_work_router import (
    DomainDiscoveryWorkRouter,
)
from crawler_system.domain_discovery_worker_supervisor import (
    DomainDiscoveryWorkerSupervisor,
)


@dataclass(frozen=True)
class SchedulerAssignment:
    assignment_id: str
    worker_id: str
    generation: int
    fencing_epoch: int
    logical_partition: int
    source: str
    work_id: str
    created_at: float


@dataclass(frozen=True)
class SchedulerOrchestratorCapacity:
    scheduler_slots: int
    active_assignments: int
    available_scheduler_slots: int
    queue_capacity: int
    router_capacity: int
    worker_capacity: int


class DomainDiscoverySchedulerOrchestrator:
    """
    Global discovery scheduling control plane.

    Coordinates the durable scheduler catalog, work queue, work router,
    and worker supervisor without enumerating the global domain/URL space.

    Scheduling operates on compact logical work units. The durable queue
    remains authoritative for discovery work while this layer coordinates
    assignment lifecycle, fairness, backpressure, fencing, recovery,
    and scheduler checkpoints.
    """

    VERSION = "domain-discovery-scheduler-orchestrator.v1"

    def __init__(
        self,
        catalog: DomainDiscoverySchedulerCatalog,
        work_queue: DomainDiscoveryWorkQueue,
        work_router: DomainDiscoveryWorkRouter,
        worker_supervisor: DomainDiscoveryWorkerSupervisor,
        max_scheduler_slots: int = 100_000,
        assignment_lease_seconds: float = 300.0,
        aging_seconds: float = 60.0,
        recovery_batch_size: int = 10_000,
    ):
        self.catalog = catalog
        self.work_queue = work_queue
        self.work_router = work_router
        self.worker_supervisor = worker_supervisor

        self.max_scheduler_slots = max(
            1,
            int(max_scheduler_slots),
        )
        self.assignment_lease_seconds = float(
            assignment_lease_seconds
        )
        self.aging_seconds = max(
            0.0,
            float(aging_seconds),
        )
        self.recovery_batch_size = max(
            1,
            int(recovery_batch_size),
        )

        if self.assignment_lease_seconds <= 0:
            raise ValueError(
                "assignment_lease_seconds must be positive"
            )

        self._lock = threading.RLock()
        self._stop_event = threading.Event()

        self._assignments: dict[
            str,
            SchedulerAssignment,
        ] = {}

        self._leases: dict[
            str,
            SchedulerLease,
        ] = {}

        self._worker_records: dict[
            str,
            SchedulerWorker,
        ] = {}

        self._running = False

        self._stats = {
            "schedule_cycles": 0,
            "assignments_created": 0,
            "assignments_completed": 0,
            "assignments_failed": 0,
            "assignment_rejections": 0,
            "backpressure_events": 0,
            "worker_fencing_events": 0,
            "lease_recoveries": 0,
            "scheduler_errors": 0,
            "last_cycle_at": 0.0,
            "last_recovery_at": 0.0,
        }

    # ============================================================
    # Worker lifecycle
    # ============================================================

    def register_worker(
        self,
        worker_id: str,
    ) -> SchedulerWorker:
        worker = self.catalog.register_worker(
            worker_id
        )

        with self._lock:
            self._worker_records[
                worker.worker_id
            ] = worker

        self.worker_supervisor.register_worker(
            worker.worker_id
        )

        return worker

    def heartbeat_worker(
        self,
        worker_id: str,
        generation: int,
        fencing_epoch: int,
    ) -> bool:
        return self.catalog.heartbeat(
            worker_id,
            generation,
            fencing_epoch,
        )

    def fence_expired_workers(self) -> list[str]:
        fenced = (
            self.catalog.fence_expired_workers()
        )

        for worker_id in fenced:
            try:
                self.worker_supervisor.unregister_worker(
                    worker_id
                )
            except Exception:
                pass

        if fenced:
            with self._lock:
                self._stats[
                    "worker_fencing_events"
                ] += len(fenced)

                for worker_id in fenced:
                    self._worker_records.pop(
                        worker_id,
                        None,
                    )

        return fenced

    # ============================================================
    # Logical scheduling state
    # ============================================================

    def register_logical_work(
        self,
        logical_partition: int,
        source: str,
        priority: float = 50.0,
        queued_work: int = 0,
        processing_work: int = 0,
    ) -> LogicalWorkState:
        self.catalog.ensure_source(
            source,
            priority=priority,
        )

        return self.catalog.ensure_logical_work(
            logical_partition=logical_partition,
            source=source,
            priority=priority,
            queued_work=queued_work,
            processing_work=processing_work,
        )

    def record_queue_delta(
        self,
        logical_partition: int,
        source: str,
        queued_delta: int = 0,
        processing_delta: int = 0,
    ) -> bool:
        return self.catalog.update_logical_work(
            logical_partition=logical_partition,
            source=source,
            queued_delta=queued_delta,
            processing_delta=processing_delta,
        )

    # ============================================================
    # Capacity / backpressure
    # ============================================================

    def capacity(
        self,
    ) -> SchedulerOrchestratorCapacity:
        with self._lock:
            active_assignments = len(
                self._assignments
            )

        scheduler_available = max(
            0,
            self.max_scheduler_slots
            - active_assignments,
        )

        queue_capacity = 0
        router_capacity = 0
        worker_capacity = 0

        try:
            queue_state = self.work_queue.capacity()

            queue_capacity = int(
                getattr(
                    queue_state,
                    "available",
                    getattr(
                        queue_state,
                        "available_capacity",
                        0,
                    ),
                )
            )
        except Exception:
            pass

        try:
            router_state = self.work_router.capacity()

            router_capacity = int(
                getattr(
                    router_state,
                    "available_routes",
                    getattr(
                        router_state,
                        "available",
                        0,
                    ),
                )
            )
        except Exception:
            pass

        try:
            worker_capacity = int(
                self.worker_supervisor.worker_count()
            )
        except Exception:
            pass

        return SchedulerOrchestratorCapacity(
            scheduler_slots=self.max_scheduler_slots,
            active_assignments=active_assignments,
            available_scheduler_slots=scheduler_available,
            queue_capacity=max(
                0,
                queue_capacity,
            ),
            router_capacity=max(
                0,
                router_capacity,
            ),
            worker_capacity=max(
                0,
                worker_capacity,
            ),
        )

    def _can_schedule(self) -> bool:
        capacity = self.capacity()

        if (
            capacity.available_scheduler_slots
            <= 0
        ):
            return False

        try:
            if not self.work_router.can_route():
                return False
        except Exception:
            pass

        return True

    # ============================================================
    # Worker selection
    # ============================================================

    def _select_worker(
        self,
    ) -> Optional[SchedulerWorker]:
        with self._lock:
            workers = list(
                self._worker_records.values()
            )
            assignments = list(
                self._assignments.values()
            )

        if not workers:
            return None

        now = time.time()

        healthy = [
            worker
            for worker in workers
            if worker.status == "active"
            and worker.last_heartbeat
            >= (
                now
                - self.catalog.heartbeat_timeout
            )
        ]

        if not healthy:
            return None

        load: dict[str, int] = {}

        for assignment in assignments:
            load[
                assignment.worker_id
            ] = load.get(
                assignment.worker_id,
                0,
            ) + 1

        healthy.sort(
            key=lambda worker: (
                load.get(
                    worker.worker_id,
                    0,
                ),
                worker.last_heartbeat,
                worker.worker_id,
            )
        )

        return healthy[0]

    # ============================================================
    # Schedule one work item
    # ============================================================

    def schedule_one(
        self,
    ) -> Optional[SchedulerAssignment]:
        if not self._can_schedule():
            with self._lock:
                self._stats[
                    "backpressure_events"
                ] += 1

            return None

        worker = self._select_worker()

        if worker is None:
            with self._lock:
                self._stats[
                    "assignment_rejections"
                ] += 1

            return None

        routed = self.work_router.acquire(
            worker_id=worker.worker_id
        )

        if routed is None:
            with self._lock:
                self._stats[
                    "assignment_rejections"
                ] += 1

            return None

        try:
            lease_result = (
                self.catalog.acquire_logical_schedule(
                    worker_id=worker.worker_id,
                    generation=worker.generation,
                    fencing_epoch=worker.fencing_epoch,
                    lease_seconds=(
                        self.assignment_lease_seconds
                    ),
                )
            )
        except Exception:
            try:
                self.work_router.fail(
                    routed
                )
            except Exception:
                pass

            raise

        if lease_result is None:
            try:
                self.work_router.fail(
                    routed
                )
            except Exception:
                pass

            with self._lock:
                self._stats[
                    "assignment_rejections"
                ] += 1

            return None

        logical_state, scheduler_lease = (
            lease_result
        )

        assignment = SchedulerAssignment(
            assignment_id=uuid.uuid4().hex,
            worker_id=worker.worker_id,
            generation=worker.generation,
            fencing_epoch=worker.fencing_epoch,
            logical_partition=(
                logical_state.logical_partition
            ),
            source=logical_state.source,
            work_id=routed.work_id,
            created_at=time.time(),
        )

        with self._lock:
            self._assignments[
                assignment.assignment_id
            ] = assignment

            self._leases[
                assignment.assignment_id
            ] = scheduler_lease

            self._stats[
                "assignments_created"
            ] += 1

        return assignment

    # ============================================================
    # Batch scheduling
    # ============================================================

    def schedule_available(
        self,
        max_assignments: Optional[int] = None,
    ) -> list[SchedulerAssignment]:
        if max_assignments is None:
            max_assignments = (
                self.max_scheduler_slots
            )

        max_assignments = max(
            1,
            int(max_assignments),
        )

        created: list[
            SchedulerAssignment
        ] = []

        for _ in range(max_assignments):
            try:
                assignment = (
                    self.schedule_one()
                )
            except Exception:
                with self._lock:
                    self._stats[
                        "scheduler_errors"
                    ] += 1

                break

            if assignment is None:
                break

            created.append(
                assignment
            )

        return created

    # ============================================================
    # Assignment lookup
    # ============================================================

    def get_assignment(
        self,
        assignment_id: str,
    ) -> Optional[SchedulerAssignment]:
        with self._lock:
            return self._assignments.get(
                assignment_id
            )

    # ============================================================
    # Assignment renewal
    # ============================================================

    def renew_assignment(
        self,
        assignment_id: str,
    ) -> bool:
        with self._lock:
            assignment = self._assignments.get(
                assignment_id
            )
            lease = self._leases.get(
                assignment_id
            )

        if (
            assignment is None
            or lease is None
        ):
            return False

        worker = self.catalog.get_worker(
            assignment.worker_id
        )

        if worker is None:
            return False

        if (
            worker.generation
            != assignment.generation
        ):
            return False

        if (
            worker.fencing_epoch
            != assignment.fencing_epoch
        ):
            return False

        try:
            active_routes = (
                self.work_router.active_for_worker(
                    assignment.worker_id
                )
            )

            for route in active_routes:
                if (
                    route.work_id
                    == assignment.work_id
                ):
                    self.work_router.renew(
                        route
                    )
                    break
        except Exception:
            pass

        if not self.catalog.renew_lease(
            lease
        ):
            return False

        return self.catalog.heartbeat(
            assignment.worker_id,
            assignment.generation,
            assignment.fencing_epoch,
        )

    # ============================================================
    # Assignment completion
    # ============================================================

    def complete_assignment(
        self,
        assignment_id: str,
    ) -> bool:
        with self._lock:
            assignment = self._assignments.get(
                assignment_id
            )
            lease = self._leases.get(
                assignment_id
            )

        if (
            assignment is None
            or lease is None
        ):
            return False

        if not self.catalog.complete_lease(
            lease
        ):
            return False

        try:
            active_routes = (
                self.work_router.active_for_worker(
                    assignment.worker_id
                )
            )

            for route in active_routes:
                if (
                    route.work_id
                    == assignment.work_id
                ):
                    self.work_router.complete(
                        route
                    )
                    break
        except Exception:
            pass

        self._remove_assignment(
            assignment_id,
            "assignments_completed",
        )

        self.catalog.update_logical_work(
            logical_partition=(
                assignment.logical_partition
            ),
            source=assignment.source,
            processing_delta=-1,
        )

        return True

    # ============================================================
    # Assignment failure
    # ============================================================

    def fail_assignment(
        self,
        assignment_id: str,
        retry_at: Optional[float] = None,
    ) -> bool:
        with self._lock:
            assignment = self._assignments.get(
                assignment_id
            )
            lease = self._leases.get(
                assignment_id
            )

        if (
            assignment is None
            or lease is None
        ):
            return False

        if not self.catalog.release_lease(
            lease,
            retry_at=retry_at,
        ):
            return False

        try:
            active_routes = (
                self.work_router.active_for_worker(
                    assignment.worker_id
                )
            )

            for route in active_routes:
                if (
                    route.work_id
                    == assignment.work_id
                ):
                    self.work_router.fail(
                        route
                    )
                    break
        except Exception:
            pass

        self._remove_assignment(
            assignment_id,
            "assignments_failed",
        )

        self.catalog.update_logical_work(
            logical_partition=(
                assignment.logical_partition
            ),
            source=assignment.source,
            processing_delta=-1,
            next_schedule_at=(
                time.time()
                if retry_at is None
                else retry_at
            ),
        )

        return True

    def _remove_assignment(
        self,
        assignment_id: str,
        metric: str,
    ) -> None:
        with self._lock:
            self._assignments.pop(
                assignment_id,
                None,
            )

            self._leases.pop(
                assignment_id,
                None,
            )

            self._stats[metric] += 1

    # ============================================================
    # Recovery
    # ============================================================

    def recover(self) -> int:
        recovered = 0
        now = time.time()

        try:
            recovered += (
                self.catalog.recover_expired_leases(
                    limit=self.recovery_batch_size
                )
            )
        except Exception:
            with self._lock:
                self._stats[
                    "scheduler_errors"
                ] += 1

        try:
            recovered += (
                self.work_router.recover_expired(
                    limit=self.recovery_batch_size
                )
            )
        except Exception:
            with self._lock:
                self._stats[
                    "scheduler_errors"
                ] += 1

        try:
            recovered += (
                self.worker_supervisor.recover_abandoned_work()
            )
        except Exception:
            with self._lock:
                self._stats[
                    "scheduler_errors"
                ] += 1

        expired: list[str] = []

        with self._lock:
            for assignment_id, lease in (
                self._leases.items()
            ):
                if lease.lease_until <= now:
                    expired.append(
                        assignment_id
                    )

        for assignment_id in expired:
            with self._lock:
                assignment = (
                    self._assignments.get(
                        assignment_id
                    )
                )
                lease = self._leases.get(
                    assignment_id
                )

            if (
                assignment is None
                or lease is None
            ):
                continue

            if self.catalog.release_lease(
                lease,
                retry_at=now,
            ):
                self._remove_assignment(
                    assignment_id,
                    "assignments_failed",
                )
                recovered += 1

        with self._lock:
            self._stats[
                "lease_recoveries"
            ] += recovered

            self._stats[
                "last_recovery_at"
            ] = now

        return recovered

    # ============================================================
    # Durable checkpoint
    # ============================================================

    def checkpoint(self) -> bool:
        now = time.time()

        self.catalog.write_checkpoint(
            "scheduler.last_cycle_at",
            str(now),
        )

        with self._lock:
            active_count = len(
                self._assignments
            )

        self.catalog.write_checkpoint(
            "scheduler.active_assignment_count",
            str(active_count),
        )

        self.catalog.write_checkpoint(
            "scheduler.aging_seconds",
            str(self.aging_seconds),
        )

        return self.catalog.write_checkpoint(
            "scheduler.orchestrator_version",
            self.VERSION,
        )

    # ============================================================
    # Scheduler cycle
    # ============================================================

    def cycle(
        self,
        max_assignments: Optional[int] = None,
    ) -> list[SchedulerAssignment]:
        with self._lock:
            self._stats[
                "schedule_cycles"
            ] += 1

        self.fence_expired_workers()
        self.recover()

        assignments = (
            self.schedule_available(
                max_assignments=max_assignments
            )
        )

        with self._lock:
            self._stats[
                "last_cycle_at"
            ] = time.time()

        return assignments

    # ============================================================
    # Continuous scheduler
    # ============================================================

    def run(
        self,
        interval: float = 1.0,
        checkpoint_every: float = 30.0,
    ) -> None:
        interval = max(
            0.01,
            float(interval),
        )

        checkpoint_every = max(
            0.1,
            float(checkpoint_every),
        )

        with self._lock:
            if self._running:
                return

            self._running = True
            self._stop_event.clear()

        last_checkpoint = time.time()

        try:
            while not self._stop_event.is_set():
                try:
                    self.cycle()
                except Exception:
                    with self._lock:
                        self._stats[
                            "scheduler_errors"
                        ] += 1

                now = time.time()

                if (
                    now - last_checkpoint
                    >= checkpoint_every
                ):
                    try:
                        self.checkpoint()
                    except Exception:
                        with self._lock:
                            self._stats[
                                "scheduler_errors"
                            ] += 1

                    last_checkpoint = now

                self._stop_event.wait(
                    interval
                )
        finally:
            with self._lock:
                self._running = False

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    # ============================================================
    # Metrics
    # ============================================================

    def stats(
        self,
    ) -> dict[str, int | float | str]:
        with self._lock:
            stats = dict(self._stats)

            stats[
                "active_assignments"
            ] = len(
                self._assignments
            )

            stats[
                "registered_workers"
            ] = len(
                self._worker_records
            )

        try:
            for key, value in (
                self.catalog.stats().items()
            ):
                stats[
                    f"catalog_{key}"
                ] = value
        except Exception:
            pass

        try:
            for key, value in (
                self.work_queue.stats().items()
            ):
                stats[
                    f"queue_{key}"
                ] = value
        except Exception:
            pass

        try:
            for key, value in (
                self.work_router.stats().items()
            ):
                stats[
                    f"router_{key}"
                ] = value
        except Exception:
            pass

        return stats
