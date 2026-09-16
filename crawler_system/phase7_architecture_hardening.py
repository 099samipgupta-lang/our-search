from __future__ import annotations

"""
OUR SEARCH — Phase 7 Architecture Hardening

Phase 7
Brick 7.6 — Final Phase-7 Architecture Hardening

Version:
    phase7-architecture-hardening.v1

Mission:
    Establish the final production-scale boundary connecting:

        Global Discovery
              ↓
        Discovery Work Contract
              ↓
        Durable Queue
              ↓
        Placement / Routing
              ↓
        Worker Supervision
              ↓
        Crawler
              ↓
        Storage / Index
              ↓
        Recovery / Reconciliation

Scale target:
    Billions of public websites and an enormous public-Web URL universe.

This module is an architecture boundary and composition layer.
It does not replace the existing production subsystems.
"""

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional


HARDENING_VERSION = "phase7-architecture-hardening.v1"


@dataclass(frozen=True)
class Phase7ComponentBoundary:
    name: str
    role: str
    required: bool = True


@dataclass(frozen=True)
class Phase7PipelineResult:
    discovered: int
    queued: int
    routed: int
    crawled: int
    stored: int
    indexed: int
    recovered: int
    failed: int


class Phase7ArchitectureHardening:
    """
    Final composition boundary for Phase 7.

    Components are injected so each subsystem retains independent
    ownership and can be horizontally scaled or replaced without
    changing the global architecture contract.
    """

    COMPONENTS = (
        Phase7ComponentBoundary(
            "discovery",
            "global source discovery",
        ),
        Phase7ComponentBoundary(
            "contract",
            "canonical discovery work contract",
        ),
        Phase7ComponentBoundary(
            "queue",
            "durable discovery work queue",
        ),
        Phase7ComponentBoundary(
            "router",
            "placement-aware work routing",
        ),
        Phase7ComponentBoundary(
            "workers",
            "worker supervision and fencing",
        ),
        Phase7ComponentBoundary(
            "crawler",
            "web crawling and frontier execution",
        ),
        Phase7ComponentBoundary(
            "storage_index",
            "durable storage and index handoff",
        ),
        Phase7ComponentBoundary(
            "recovery",
            "failure recovery and reconciliation",
        ),
    )

    def __init__(
        self,
        *,
        discovery: Any = None,
        contract: Any = None,
        queue: Any = None,
        router: Any = None,
        workers: Any = None,
        crawler: Any = None,
        storage_index: Any = None,
        recovery: Any = None,
        discovery_crawler: Any = None,
        crawler_storage_index: Any = None,
        bottleneck_control: Any = None,
    ):
        self.discovery = discovery
        self.contract = contract
        self.queue = queue
        self.router = router
        self.workers = workers
        self.crawler = crawler
        self.storage_index = storage_index
        self.recovery = recovery

        self.discovery_crawler = (
            discovery_crawler
        )

        self.crawler_storage_index = (
            crawler_storage_index
        )

        self.bottleneck_control = (
            bottleneck_control
        )

        self._stats = {
            "cycles": 0,
            "discoveries": 0,
            "queued": 0,
            "routed": 0,
            "crawled": 0,
            "stored": 0,
            "indexed": 0,
            "recovered": 0,
            "failed": 0,
            "reconciliations": 0,
            "worker_ticks": 0,
            "recovery_ticks": 0,
            "boundary_rejections": 0,
        }

    # ------------------------------------------------------------------
    # ARCHITECTURE BOUNDARY
    # ------------------------------------------------------------------

    def component_boundaries(
        self,
    ) -> tuple[Phase7ComponentBoundary, ...]:
        return self.COMPONENTS

    def connected_components(
        self,
    ) -> dict[str, bool]:
        return {
            "discovery": (
                self.discovery is not None
            ),
            "contract": (
                self.contract is not None
            ),
            "queue": (
                self.queue is not None
            ),
            "router": (
                self.router is not None
            ),
            "workers": (
                self.workers is not None
            ),
            "crawler": (
                self.crawler is not None
            ),
            "storage_index": (
                self.storage_index is not None
            ),
            "recovery": (
                self.recovery is not None
            ),
            "discovery_crawler": (
                self.discovery_crawler is not None
            ),
            "crawler_storage_index": (
                self.crawler_storage_index
                is not None
            ),
            "bottleneck_control": (
                self.bottleneck_control
                is not None
            ),
        }

    # ------------------------------------------------------------------
    # DISCOVERY → CONTRACT
    # ------------------------------------------------------------------

    def canonicalize(
        self,
        candidate: Any,
    ) -> Any:
        if self.contract is None:
            self._stats[
                "boundary_rejections"
            ] += 1

            raise RuntimeError(
                "Phase 7 contract boundary is not connected"
            )

        method = getattr(
            self.contract,
            "contract_from_candidate",
            None,
        )

        if method is None:
            method = getattr(
                self.contract,
                "create",
                None,
            )

        if method is None:
            self._stats[
                "boundary_rejections"
            ] += 1

            raise AttributeError(
                "contract component does not expose "
                "contract_from_candidate or create"
            )

        return method(candidate)

    # ------------------------------------------------------------------
    # DISCOVERY → QUEUE
    # ------------------------------------------------------------------

    def enqueue_contract(
        self,
        contract: Any,
    ) -> Any:
        if self.queue is None:
            self._stats[
                "boundary_rejections"
            ] += 1

            raise RuntimeError(
                "Phase 7 queue boundary is not connected"
            )

        enqueue = getattr(
            self.queue,
            "enqueue",
            None,
        )

        if enqueue is None:
            raise AttributeError(
                "queue does not expose enqueue"
            )

        work_id = getattr(
            contract,
            "work_id",
            None,
        )

        hostname = getattr(
            contract,
            "hostname",
            None,
        )

        priority = getattr(
            contract,
            "priority",
            50.0,
        )

        source = getattr(
            contract,
            "source",
            "unknown",
        )

        logical_partition = getattr(
            contract,
            "logical_partition",
            None,
        )

        try:
            return enqueue(
                work_id=work_id,
                hostname=hostname,
                priority=priority,
                source=source,
                logical_partition=(
                    logical_partition
                ),
            )
        except TypeError:
            try:
                return enqueue(
                    contract
                )
            except TypeError:
                return enqueue(
                    work_id,
                    hostname,
                    priority,
                    source,
                )

    # ------------------------------------------------------------------
    # CRAWLER HANDOFF
    # ------------------------------------------------------------------

    def activate_crawler_batch(
        self,
        worker_id: str,
        limit: int = 100,
    ) -> list[Any]:
        if self.discovery_crawler is None:
            self._stats[
                "boundary_rejections"
            ] += 1

            raise RuntimeError(
                "discovery/crawler boundary is not connected"
            )

        cycle = getattr(
            self.discovery_crawler,
            "cycle",
            None,
        )

        if cycle is not None:
            result = cycle(
                worker_id=worker_id,
                batch_size=limit,
            )

            values = list(
                result or []
            )

            self._stats[
                "routed"
            ] += len(values)

            self._stats[
                "crawled"
            ] += sum(
                1
                for value in values
                if getattr(
                    value,
                    "activated",
                    False,
                )
            )

            return values

        activate_batch = getattr(
            self.discovery_crawler,
            "activate_batch",
            None,
        )

        if activate_batch is None:
            raise AttributeError(
                "discovery/crawler integration does not expose "
                "cycle or activate_batch"
            )

        values = list(
            activate_batch(
                worker_id=worker_id,
                limit=limit,
            )
            or []
        )

        self._stats[
            "routed"
        ] += len(values)

        self._stats[
            "crawled"
        ] += sum(
            1
            for value in values
            if getattr(
                value,
                "activated",
                False,
            )
        )

        return values

    # ------------------------------------------------------------------
    # CRAWLER → STORAGE / INDEX
    # ------------------------------------------------------------------

    def handoff_crawl_results(
        self,
        results: Iterable[Any],
    ) -> list[Any]:
        if self.crawler_storage_index is None:
            self._stats[
                "boundary_rejections"
            ] += 1

            raise RuntimeError(
                "crawler/storage-index boundary is not connected"
            )

        values = list(
            results or []
        )

        if not values:
            return []

        accept_many = getattr(
            self.crawler_storage_index,
            "accept_many",
            None,
        )

        if accept_many is None:
            accept_many = getattr(
                self.crawler_storage_index,
                "process_batch",
                None,
            )

        if accept_many is None:
            raise AttributeError(
                "crawler/storage-index integration does not expose "
                "accept_many or process_batch"
            )

        output = list(
            accept_many(values)
            or []
        )

        self._stats[
            "stored"
        ] += sum(
            1
            for value in output
            if getattr(
                value,
                "stored",
                False,
            )
        )

        self._stats[
            "indexed"
        ] += sum(
            1
            for value in output
            if getattr(
                value,
                "indexed",
                False,
            )
        )

        self._stats[
            "failed"
        ] += sum(
            1
            for value in output
            if getattr(
                value,
                "error",
                None,
            )
        )

        return output

    # ------------------------------------------------------------------
    # RECOVERY
    # ------------------------------------------------------------------

    def recovery_tick(
        self,
        limit: int = 10000,
    ) -> int:
        if self.recovery is None:
            return 0

        recover_expired = getattr(
            self.recovery,
            "recover_expired",
            None,
        )

        recovered = 0

        if recover_expired is not None:
            try:
                result = recover_expired(
                    limit=limit
                )
            except TypeError:
                result = recover_expired()

            if isinstance(
                result,
                int,
            ):
                recovered = result
            else:
                recovered = len(
                    result or []
                )

        recover_failed = getattr(
            self.recovery,
            "recover_failed",
            None,
        )

        if recover_failed is not None:
            try:
                result = recover_failed(
                    limit=limit
                )
            except TypeError:
                result = recover_failed()

            if isinstance(
                result,
                int,
            ):
                recovered += result
            else:
                recovered += len(
                    result or []
                )

        self._stats[
            "recovered"
        ] += recovered

        self._stats[
            "recovery_ticks"
        ] += 1

        return recovered

    # ------------------------------------------------------------------
    # WORKER / CONTROL PLANE
    # ------------------------------------------------------------------

    def worker_tick(self) -> Any:
        if self.workers is None:
            return None

        tick = getattr(
            self.workers,
            "tick",
            None,
        )

        if tick is None:
            return None

        self._stats[
            "worker_ticks"
        ] += 1

        return tick()

    def scale_tick(self) -> Any:
        if self.bottleneck_control is None:
            return None

        tick = getattr(
            self.bottleneck_control,
            "tick",
            None,
        )

        if tick is None:
            return None

        return tick()

    # ------------------------------------------------------------------
    # DISCOVERY CYCLE
    # ------------------------------------------------------------------

    def discovery_cycle(
        self,
        max_candidates: int = 10000,
    ) -> int:
        if self.discovery is None:
            return 0

        discover = getattr(
            self.discovery,
            "discover_once",
            None,
        )

        if discover is None:
            discover = getattr(
                self.discovery,
                "run_once",
                None,
            )

        if discover is None:
            discover = getattr(
                self.discovery,
                "cycle",
                None,
            )

        if discover is None:
            return 0

        try:
            result = discover()
        except TypeError:
            result = discover(
                max_candidates=max_candidates
            )

        if result is None:
            return 0

        if isinstance(
            result,
            Mapping,
        ):
            candidates = (
                result.get(
                    "candidates",
                    result.get(
                        "discoveries",
                        [],
                    ),
                )
            )
        else:
            candidates = result

        count = 0

        for candidate in (
            candidates or []
        ):
            try:
                contract = self.canonicalize(
                    candidate
                )

                self.enqueue_contract(
                    contract
                )

                count += 1

            except Exception:
                self._stats[
                    "failed"
                ] += 1

        self._stats[
            "discoveries"
        ] += count

        self._stats[
            "queued"
        ] += count

        return count

    # ------------------------------------------------------------------
    # RECONCILIATION
    # ------------------------------------------------------------------

    def reconcile(
        self,
        work_id: str,
        **observations: bool,
    ) -> Any:
        if self.recovery is None:
            return None

        reconcile = getattr(
            self.recovery,
            "reconcile",
            None,
        )

        if reconcile is None:
            return None

        self._stats[
            "reconciliations"
        ] += 1

        return reconcile(
            work_id,
            **observations,
        )

    # ------------------------------------------------------------------
    # COMPLETE ARCHITECTURE CYCLE
    # ------------------------------------------------------------------

    def cycle(
        self,
        worker_id: Optional[str] = None,
        batch_size: int = 100,
        discovery_limit: int = 10000,
    ) -> Phase7PipelineResult:
        """
        One bounded control cycle.

        The bounds apply only to this execution quantum. They are not
        global system limits and do not cap the overall Web workload.
        """

        self._stats[
            "cycles"
        ] += 1

        discovered = self.discovery_cycle(
            max_candidates=discovery_limit
        )

        recovered = self.recovery_tick(
            limit=discovery_limit
        )

        self.worker_tick()
        self.scale_tick()

        routed = 0
        crawled = 0
        stored = 0
        indexed = 0
        failed = 0

        if worker_id is not None:
            activation_results = (
                self.activate_crawler_batch(
                    worker_id=worker_id,
                    limit=batch_size,
                )
            )

            routed = len(
                activation_results
            )

            crawled = sum(
                1
                for result
                in activation_results
                if getattr(
                    result,
                    "activated",
                    False,
                )
            )

            handoff_results = (
                self.handoff_crawl_results(
                    activation_results
                )
            )

            stored = sum(
                1
                for result
                in handoff_results
                if getattr(
                    result,
                    "stored",
                    False,
                )
            )

            indexed = sum(
                1
                for result in handoff_results
                if getattr(
                    result,
                    "indexed",
                    False,
                )
            )

            failed = sum(
                1
                for result in handoff_results
                if getattr(
                    result,
                    "error",
                    None,
                )
            )

        self._stats[
            "routed"
        ] += routed

        self._stats[
            "crawled"
        ] += crawled

        self._stats[
            "stored"
        ] += stored

        self._stats[
            "indexed"
        ] += indexed

        self._stats[
            "failed"
        ] += failed

        return Phase7PipelineResult(
            discovered=discovered,
            queued=discovered,
            routed=routed,
            crawled=crawled,
            stored=stored,
            indexed=indexed,
            recovered=recovered,
            failed=failed,
        )

    # ------------------------------------------------------------------
    # CONTINUOUS OPERATION
    # ------------------------------------------------------------------

    def run(
        self,
        worker_id: str,
        interval: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        import time as _time

        if interval < 0:
            raise ValueError(
                "interval cannot be negative"
            )

        while True:
            self.cycle(
                worker_id=worker_id,
                batch_size=batch_size,
            )

            if interval:
                _time.sleep(
                    interval
                )

    # ------------------------------------------------------------------
    # STATS
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        connected = self.connected_components()

        return {
            **self._stats,
            "hardening_version": (
                HARDENING_VERSION
            ),
            "connected_components": connected,
            "scale_target": (
                "billions of public websites "
                "and an enormous public-Web URL universe"
            ),
            "fixed_global_work_limit": False,
            "google_dependency": False,
        }


__all__ = [
    "HARDENING_VERSION",
    "Phase7ComponentBoundary",
    "Phase7PipelineResult",
    "Phase7ArchitectureHardening",
]
