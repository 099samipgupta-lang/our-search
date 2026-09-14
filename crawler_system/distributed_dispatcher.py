from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, Iterable

from .partition_router import PartitionRouter
from .task import CrawlTask
from .worker_ownership import WorkerOwnership
from .worker_registry import WorkerRegistry


@dataclass(frozen=True)
class DispatchDecision:
    task_id: str
    url: str
    partition_id: int
    node_id: str
    worker_count: int


class DistributedDispatcher:
    """
    Deterministic distributed task-routing layer.

    URL
      -> PartitionRouter
      -> partition_id
      -> WorkerOwnership
      -> node_id
      -> WorkerRegistry
      -> dispatch decision

    Actual inter-node transport belongs to Stage 5.4.4.
    """

    def __init__(
        self,
        ownership: WorkerOwnership,
        registry: WorkerRegistry,
        router: PartitionRouter | None = None,
    ):
        if not isinstance(ownership, WorkerOwnership):
            raise TypeError("ownership must be a WorkerOwnership")

        if not isinstance(registry, WorkerRegistry):
            raise TypeError("registry must be a WorkerRegistry")

        if ownership.cluster_id != registry.cluster_id:
            raise ValueError(
                "ownership and registry cluster_id values must match"
            )

        if router is None:
            router = PartitionRouter(
                partition_count=ownership.partition_count
            )

        if not isinstance(router, PartitionRouter):
            raise TypeError("router must be a PartitionRouter")

        if router.partition_count != ownership.partition_count:
            raise ValueError(
                "router and ownership partition counts must match"
            )

        self.ownership = ownership
        self.registry = registry
        self.router = router

        self._lock = threading.RLock()
        self._in_flight: Dict[str, DispatchDecision] = {}
        self._completed: Dict[str, DispatchDecision] = {}

    @staticmethod
    def _validate_task(task: CrawlTask) -> None:
        if not isinstance(task, CrawlTask):
            raise TypeError("task must be a CrawlTask")

        if not task.url or not task.url.strip():
            raise ValueError("task.url must be non-empty")

        if not task.document_id or not task.document_id.strip():
            raise ValueError("task.document_id must be non-empty")

    def partition_for_task(self, task: CrawlTask) -> int:
        self._validate_task(task)

        # PartitionRouter.partition_for() returns the integer
        # partition ID directly.
        return self.router.partition_for(task.url)

    def owner_for_task(self, task: CrawlTask) -> str:
        self._validate_task(task)

        partition_id = self.partition_for_task(task)
        owner = self.ownership.owner_node_for_partition(partition_id)
        return owner.node_id

    def _active_registration(self, node_id: str):
        registration = self.registry.get(node_id)

        if registration is None:
            raise RuntimeError(
                f"owning node is not registered: {node_id}"
            )

        if not registration.active:
            raise RuntimeError(
                f"owning node is inactive: {node_id}"
            )

        return registration

    def decide(self, task: CrawlTask) -> DispatchDecision:
        self._validate_task(task)

        partition_id = self.partition_for_task(task)
        node_id = self.owner_for_task(task)
        registration = self._active_registration(node_id)

        return DispatchDecision(
            task_id=task.document_id,
            url=task.url,
            partition_id=partition_id,
            node_id=node_id,
            worker_count=registration.worker_count,
        )

    def dispatch(self, task: CrawlTask) -> DispatchDecision:
        """
        Reserve a task for its deterministic owning node.

        A task cannot be dispatched twice while it is in flight or after
        completion.
        """
        decision = self.decide(task)

        with self._lock:
            if decision.task_id in self._in_flight:
                raise RuntimeError(
                    f"task already in flight: {decision.task_id}"
                )

            if decision.task_id in self._completed:
                raise RuntimeError(
                    f"task already completed: {decision.task_id}"
                )

            self._in_flight[decision.task_id] = decision
            return decision

    def complete(self, task_id: str) -> DispatchDecision:
        task_id = str(task_id).strip()

        if not task_id:
            raise ValueError("task_id must be non-empty")

        with self._lock:
            decision = self._in_flight.pop(task_id, None)

            if decision is None:
                raise KeyError(
                    f"task is not in flight: {task_id}"
                )

            self._completed[task_id] = decision
            return decision

    def release(self, task_id: str) -> DispatchDecision:
        """
        Release an in-flight task without marking it completed.

        Used when a dispatch attempt must be retried.
        """
        task_id = str(task_id).strip()

        if not task_id:
            raise ValueError("task_id must be non-empty")

        with self._lock:
            decision = self._in_flight.pop(task_id, None)

            if decision is None:
                raise KeyError(
                    f"task is not in flight: {task_id}"
                )

            return decision

    def is_in_flight(self, task_id: str) -> bool:
        task_id = str(task_id).strip()

        with self._lock:
            return task_id in self._in_flight

    def is_completed(self, task_id: str) -> bool:
        task_id = str(task_id).strip()

        with self._lock:
            return task_id in self._completed

    def in_flight(self) -> tuple[DispatchDecision, ...]:
        with self._lock:
            return tuple(
                self._in_flight[key]
                for key in sorted(self._in_flight)
            )

    def completed(self) -> tuple[DispatchDecision, ...]:
        with self._lock:
            return tuple(
                self._completed[key]
                for key in sorted(self._completed)
            )

    def in_flight_count(self) -> int:
        with self._lock:
            return len(self._in_flight)

    def completed_count(self) -> int:
        with self._lock:
            return len(self._completed)

    def dispatch_many(
        self,
        tasks: Iterable[CrawlTask],
    ) -> tuple[DispatchDecision, ...]:
        decisions = []

        for task in tasks:
            decisions.append(self.dispatch(task))

        return tuple(decisions)

    def validate_decision(self, decision: DispatchDecision) -> bool:
        if not isinstance(decision, DispatchDecision):
            raise TypeError("decision must be a DispatchDecision")

        # PartitionRouter.partition_for() returns an integer.
        expected_partition = self.router.partition_for(
            decision.url
        )

        if expected_partition != decision.partition_id:
            raise ValueError(
                "dispatch decision contains incorrect partition"
            )

        expected_node = self.ownership.owner_for_partition(
            decision.partition_id
        )

        if expected_node != decision.node_id:
            raise ValueError(
                "dispatch decision violates partition ownership"
            )

        registration = self._active_registration(decision.node_id)

        if registration.worker_count != decision.worker_count:
            raise ValueError(
                "dispatch decision contains stale worker capacity"
            )

        return True

    def validate_all(self) -> bool:
        with self._lock:
            decisions = (
                tuple(self._in_flight.values())
                + tuple(self._completed.values())
            )

        for decision in decisions:
            self.validate_decision(decision)

        return True

    def node_load(self) -> dict[str, int]:
        with self._lock:
            counts: dict[str, int] = {}

            for decision in self._in_flight.values():
                counts[decision.node_id] = (
                    counts.get(decision.node_id, 0) + 1
                )

            return {
                node_id: counts.get(node_id, 0)
                for node_id in self.registry.node_ids()
            }

    def node_capacity(self) -> dict[str, int]:
        return {
            registration.node_id: registration.worker_count
            for registration in self.registry.active_nodes()
        }

    def clear_completed(self) -> None:
        with self._lock:
            self._completed.clear()
