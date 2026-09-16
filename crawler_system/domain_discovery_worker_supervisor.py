from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from crawler_system.domain_discovery_work_router import (
    DomainDiscoveryWorkRouter,
    RoutedDiscoveryWork,
)


@dataclass
class WorkerState:
    worker_id: str
    generation: int
    started_at: float
    last_heartbeat: float
    last_successful_renewal: float
    active_work: int
    status: str


@dataclass(frozen=True)
class WorkerLeaseHealth:
    worker_id: str
    healthy: bool
    active_work: int
    last_heartbeat: float
    age: float


class DomainDiscoveryWorkerSupervisor:
    """
    Distributed worker lifecycle/control layer.

    Responsibilities:

        worker registration
             ↓
        heartbeat
             ↓
        work acquisition
             ↓
        lease renewal
             ↓
        fencing/dead-worker detection
             ↓
        abandoned-work recovery

    Worker identity is independent of:
        - domain count
        - URL count
        - logical partition count
        - physical bucket count

    The supervisor is deliberately a control-plane abstraction.
    """

    VERSION = "domain-discovery-worker-supervisor.v1"

    def __init__(
        self,
        router: DomainDiscoveryWorkRouter,
        heartbeat_timeout: float = 90.0,
        heartbeat_interval: float = 20.0,
        lease_renewal_interval: float = 30.0,
        recovery_interval: float = 30.0,
        max_workers: int = 10_000,
    ):
        if not isinstance(
            router,
            DomainDiscoveryWorkRouter,
        ):
            raise TypeError(
                "router must be DomainDiscoveryWorkRouter"
            )

        self.router = router

        self.heartbeat_timeout = float(
            heartbeat_timeout
        )
        self.heartbeat_interval = float(
            heartbeat_interval
        )
        self.lease_renewal_interval = float(
            lease_renewal_interval
        )
        self.recovery_interval = float(
            recovery_interval
        )

        self.max_workers = max(
            1,
            int(max_workers),
        )

        if self.heartbeat_timeout <= 0:
            raise ValueError(
                "heartbeat_timeout must be positive"
            )

        if self.heartbeat_interval <= 0:
            raise ValueError(
                "heartbeat_interval must be positive"
            )

        if self.lease_renewal_interval <= 0:
            raise ValueError(
                "lease_renewal_interval must be positive"
            )

        if self.recovery_interval <= 0:
            raise ValueError(
                "recovery_interval must be positive"
            )

        self._lock = threading.RLock()

        self._workers: dict[
            str,
            WorkerState,
        ] = {}

        self._worker_generations: dict[
            str,
            int,
        ] = {}

        self._stop_event = threading.Event()

        self._last_recovery = 0.0

    # ============================================================
    # Worker identity
    # ============================================================

    def create_worker_id(
        self,
        prefix: str = "domain-worker",
    ) -> str:
        return (
            f"{prefix}-"
            f"{uuid.uuid4().hex}"
        )

    def register_worker(
        self,
        worker_id: Optional[str] = None,
    ) -> WorkerState:
        if worker_id is None:
            worker_id = self.create_worker_id()

        now = time.time()

        with self._lock:
            if worker_id not in self._workers:
                generation = (
                    self._worker_generations.get(
                        worker_id,
                        0,
                    )
                    + 1
                )

                if (
                    len(self._workers)
                    >= self.max_workers
                ):
                    raise RuntimeError(
                        "maximum worker capacity reached"
                    )

                state = WorkerState(
                    worker_id=worker_id,
                    generation=generation,
                    started_at=now,
                    last_heartbeat=now,
                    last_successful_renewal=now,
                    active_work=0,
                    status="active",
                )

                self._workers[
                    worker_id
                ] = state

                self._worker_generations[
                    worker_id
                ] = generation

            else:
                state = self._workers[
                    worker_id
                ]

                state.last_heartbeat = now
                state.status = "active"

            return state

    # ============================================================
    # Heartbeat
    # ============================================================

    def heartbeat(
        self,
        worker_id: str,
    ) -> bool:
        now = time.time()

        with self._lock:
            state = self._workers.get(
                worker_id
            )

            if state is None:
                return False

            if state.status == "fenced":
                return False

            state.last_heartbeat = now
            state.status = "active"

            return True

    def worker_health(
        self,
        worker_id: str,
    ) -> Optional[WorkerLeaseHealth]:
        now = time.time()

        with self._lock:
            state = self._workers.get(
                worker_id
            )

            if state is None:
                return None

            age = max(
                0.0,
                now - state.last_heartbeat,
            )

            return WorkerLeaseHealth(
                worker_id=worker_id,
                healthy=(
                    state.status == "active"
                    and age
                    <= self.heartbeat_timeout
                ),
                active_work=state.active_work,
                last_heartbeat=(
                    state.last_heartbeat
                ),
                age=age,
            )

    # ============================================================
    # Work acquisition
    # ============================================================

    def acquire_work(
        self,
        worker_id: str,
        limit: int = 100,
    ) -> list[RoutedDiscoveryWork]:
        if not self.heartbeat(worker_id):
            return []

        routes = (
            self.router.acquire(
                worker_id=worker_id,
                limit=limit,
            )
        )

        with self._lock:
            state = self._workers.get(
                worker_id
            )

            if state is not None:
                state.active_work += len(
                    routes
                )

        return routes

    # ============================================================
    # Lease renewal
    # ============================================================

    def renew_work(
        self,
        worker_id: str,
        routes: list[
            RoutedDiscoveryWork
        ],
    ) -> int:
        if not self.heartbeat(worker_id):
            return 0

        renewed = 0

        for route in routes:
            if (
                route.lease.worker_id
                != worker_id
            ):
                continue

            if self.router.renew(route):
                renewed += 1

        if renewed:
            with self._lock:
                state = self._workers.get(
                    worker_id
                )

                if state is not None:
                    state.last_successful_renewal = (
                        time.time()
                    )

        return renewed

    # ============================================================
    # Work completion
    # ============================================================

    def complete_work(
        self,
        worker_id: str,
        route: RoutedDiscoveryWork,
    ) -> bool:
        if (
            route.lease.worker_id
            != worker_id
        ):
            return False

        completed = self.router.complete(
            route
        )

        if completed:
            with self._lock:
                state = self._workers.get(
                    worker_id
                )

                if state is not None:
                    state.active_work = max(
                        0,
                        state.active_work - 1,
                    )

        return completed

    # ============================================================
    # Work failure
    # ============================================================

    def fail_work(
        self,
        worker_id: str,
        route: RoutedDiscoveryWork,
        error: str,
        retry: bool = True,
        retry_delay: float = 0.0,
    ) -> bool:
        if (
            route.lease.worker_id
            != worker_id
        ):
            return False

        failed = self.router.fail(
            route,
            error=error,
            retry=retry,
            retry_delay=retry_delay,
        )

        if failed:
            with self._lock:
                state = self._workers.get(
                    worker_id
                )

                if state is not None:
                    state.active_work = max(
                        0,
                        state.active_work - 1,
                    )

        return failed

    # ============================================================
    # Dead-worker fencing
    # ============================================================

    def fence_dead_workers(
        self,
    ) -> list[str]:
        now = time.time()
        fenced = []

        with self._lock:
            for worker_id, state in list(
                self._workers.items()
            ):
                if state.status != "active":
                    continue

                age = (
                    now
                    - state.last_heartbeat
                )

                if age <= self.heartbeat_timeout:
                    continue

                state.status = "fenced"
                fenced.append(worker_id)

        return fenced

    # ============================================================
    # Worker shutdown
    # ============================================================

    def unregister_worker(
        self,
        worker_id: str,
        retry_work: bool = True,
    ) -> int:
        with self._lock:
            state = self._workers.get(
                worker_id
            )

            if state is None:
                return 0

            state.status = "stopped"

        released = 0

        if retry_work:
            released = (
                self.router.release_worker(
                    worker_id,
                    error="worker stopped",
                )
            )

        return released

    # ============================================================
    # Abandoned-work recovery
    # ============================================================

    def recover_abandoned_work(
        self,
        force: bool = False,
    ) -> int:
        now = time.time()

        if (
            not force
            and (
                now - self._last_recovery
                < self.recovery_interval
            )
        ):
            return 0

        fenced = (
            self.fence_dead_workers()
        )

        recovered = (
            self.router.recover_expired()
        )

        self._last_recovery = now

        with self._lock:
            fenced_set = set(fenced)

            for worker_id in fenced_set:
                state = self._workers.get(
                    worker_id
                )

                if state is not None:
                    state.active_work = 0

        return recovered

    # ============================================================
    # Supervisor tick
    # ============================================================

    def tick(
        self,
    ) -> dict[str, int]:
        fenced = (
            self.fence_dead_workers()
        )

        recovered = (
            self.recover_abandoned_work(
                force=True
            )
        )

        return {
            "workers": self.worker_count(),
            "fenced_workers": len(fenced),
            "recovered_work": recovered,
        }

    # ============================================================
    # Worker state
    # ============================================================

    def worker_count(
        self,
        status: Optional[str] = None,
    ) -> int:
        with self._lock:
            if status is None:
                return len(
                    self._workers
                )

            return sum(
                1
                for worker in self._workers.values()
                if worker.status == status
            )

    def workers(
        self,
    ) -> list[WorkerState]:
        with self._lock:
            return [
                WorkerState(
                    worker_id=worker.worker_id,
                    generation=worker.generation,
                    started_at=worker.started_at,
                    last_heartbeat=worker.last_heartbeat,
                    last_successful_renewal=(
                        worker.last_successful_renewal
                    ),
                    active_work=worker.active_work,
                    status=worker.status,
                )
                for worker in self._workers.values()
            ]

    # ============================================================
    # Capacity
    # ============================================================

    def capacity(
        self,
    ) -> dict[str, int]:
        with self._lock:
            active = sum(
                1
                for worker in self._workers.values()
                if worker.status == "active"
            )

            active_work = sum(
                worker.active_work
                for worker in self._workers.values()
            )

        return {
            "workers": len(self._workers),
            "active_workers": active,
            "active_work": active_work,
            "max_workers": self.max_workers,
            "worker_capacity": max(
                0,
                self.max_workers - active,
            ),
        }

    # ============================================================
    # Continuous supervisor
    # ============================================================

    def run(
        self,
    ) -> None:
        next_heartbeat_cycle = (
            time.time()
            + self.heartbeat_interval
        )

        while not self._stop_event.is_set():
            now = time.time()

            if (
                now >= next_heartbeat_cycle
            ):
                self.tick()

                next_heartbeat_cycle = (
                    now
                    + min(
                        self.heartbeat_interval,
                        self.recovery_interval,
                    )
                )

            self._stop_event.wait(
                min(
                    self.heartbeat_interval,
                    self.recovery_interval,
                )
            )

    def stop(
        self,
    ) -> None:
        self._stop_event.set()

    def stopped(
        self,
    ) -> bool:
        return self._stop_event.is_set()

    # ============================================================
    # Global supervisor state
    # ============================================================

    def stats(
        self,
    ) -> dict[str, int | float]:
        now = time.time()

        with self._lock:
            active = 0
            fenced = 0
            stopped = 0
            active_work = 0

            for worker in self._workers.values():
                active_work += worker.active_work

                if worker.status == "active":
                    active += 1
                elif worker.status == "fenced":
                    fenced += 1
                elif worker.status == "stopped":
                    stopped += 1

            return {
                "workers": len(
                    self._workers
                ),
                "active_workers": active,
                "fenced_workers": fenced,
                "stopped_workers": stopped,
                "active_work": active_work,
                "max_workers": self.max_workers,
                "heartbeat_timeout": (
                    self.heartbeat_timeout
                ),
                "timestamp": now,
            }
