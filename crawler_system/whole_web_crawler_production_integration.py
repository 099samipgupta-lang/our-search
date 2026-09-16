from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional


@dataclass(frozen=True)
class ProductionIntegrationResult:
    discovered: int
    expanded: int
    activated: int
    indexed: int
    recovered: int
    failed: int


class WholeWebCrawlerProductionIntegration:
    """
    Final production orchestration plane for OUR SEARCH.

    Global Source Federation
            ↓
    Massive Domain / URL Expansion
            ↓
    Durable Discovery Queue
            ↓
    Placement-aware Routing
            ↓
    Worker Supervision
            ↓
    Recovery / Rebalancing / Fault Isolation
            ↓
    WholeWebCrawler
            ↓
    Distributed Storage / Index Integration

    This layer is deliberately an orchestration boundary.
    Existing crawler components remain independently usable.
    """

    VERSION = "whole-web-crawler-production-integration.v1"

    def __init__(
        self,
        crawler: Any,
        source_federation: Any,
        expansion: Any,
        worker_supervisor: Any,
        recovery: Any,
        storage_index: Any,
        crawler_activation: Optional[
            Callable[[str, str], bool]
        ] = None,
    ):
        self.crawler = crawler
        self.source_federation = source_federation
        self.expansion = expansion
        self.worker_supervisor = worker_supervisor
        self.recovery = recovery
        self.storage_index = storage_index
        self.crawler_activation = (
            crawler_activation
            or self._default_activation
        )

        self._lock = threading.RLock()
        self._stop_event = threading.Event()

        self._stats = {
            "cycles": 0,
            "source_results": 0,
            "discovered": 0,
            "expanded": 0,
            "activated": 0,
            "indexed": 0,
            "recovered": 0,
            "failed": 0,
            "worker_fences": 0,
            "faults": 0,
            "rebalances": 0,
            "runtime_errors": 0,
        }

    def _default_activation(
        self,
        hostname: str,
        url: str,
    ) -> bool:
        """
        Activate through the existing crawler's public URL path.

        No direct modification of WholeWebCrawler internals is required.
        """

        add_url = getattr(
            self.crawler,
            "_add_url",
            None,
        )

        if add_url is None:
            add_url = getattr(
                self.crawler,
                "add_url",
                None,
            )

        if add_url is None:
            return False

        try:
            return bool(add_url(url))
        except Exception:
            return False

    @staticmethod
    def _result_items(result: Any) -> Iterable[Any]:
        if result is None:
            return ()

        if isinstance(result, dict):
            for key in (
                "candidates",
                "results",
                "records",
                "domains",
                "items",
            ):
                value = result.get(key)

                if value is not None:
                    return value

            return ()

        for key in (
            "candidates",
            "results",
            "records",
            "domains",
            "items",
        ):
            value = getattr(
                result,
                key,
                None,
            )

            if value is not None:
                return value

        return ()

    def _federate_once(self) -> list[Any]:
        """
        Obtain the next available discovery output from the
        source-federation plane.

        Supports existing federation implementations without
        requiring one rigid result representation.
        """

        federation = self.source_federation

        result = None

        for method_name in (
            "discover_once",
            "run_once",
            "cycle",
        ):
            method = getattr(
                federation,
                method_name,
                None,
            )

            if method is None:
                continue

            result = method()

            break

        if result is None:
            return []

        if isinstance(result, (list, tuple, set)):
            return list(result)

        return list(
            self._result_items(result)
        )

    def _expand(
        self,
        candidates: Iterable[Any],
    ) -> int:
        result = self.expansion.expand(
            candidates
        )

        expanded = int(
            getattr(
                result,
                "queued",
                0,
            )
            or 0
        )

        return expanded

    def _activate_worker_batch(
        self,
        worker_id: str,
        limit: int,
    ) -> tuple[int, int, int]:
        """
        Consume durable expansion work and activate it through
        the production crawler.

        Returns:
            activated, indexed, failed
        """

        activated = 0
        indexed = 0
        failed = 0

        work_items = self.expansion.acquire_for_crawler(
            worker_id=worker_id,
            limit=limit,
        )

        for item in work_items:
            hostname = getattr(
                item,
                "hostname",
                None,
            )

            url = getattr(
                item,
                "url",
                None,
            )

            if not hostname or not url:
                failed += 1
                continue

            work_id = getattr(
                item,
                "work_id",
                None,
            )

            fencing_token = int(
                getattr(
                    item,
                    "fencing_token",
                    0,
                )
                or 0
            )

            try:
                success = self.crawler_activation(
                    str(hostname),
                    str(url),
                )

                if not success:
                    self.expansion.fail(
                        work_id=work_id,
                        worker_id=worker_id,
                        fencing_token=fencing_token,
                        error="crawler activation rejected",
                    )

                    failed += 1
                    continue

                activated += 1

                complete = self.expansion.complete(
                    work_id=work_id,
                    worker_id=worker_id,
                    fencing_token=fencing_token,
                )

                if not complete:
                    failed += 1
                    continue

                try:
                    inserted = self.storage_index.ingest_candidates(
                        [item]
                    )

                    indexed += int(
                        inserted or 0
                    )
                except Exception as exc:
                    self.storage_index.fail_handoff(
                        [],
                        str(exc),
                    )

            except Exception as exc:
                failed += 1

                try:
                    self.expansion.fail(
                        work_id=work_id,
                        worker_id=worker_id,
                        fencing_token=fencing_token,
                        error=str(exc),
                    )
                except Exception:
                    pass

        return activated, indexed, failed

    def _recover(self) -> int:
        recovered = 0

        try:
            recovered += int(
                self.expansion.recover()
                or 0
            )
        except Exception:
            pass

        try:
            tick = self.recovery.tick()

            recovered += int(
                tick.get(
                    "expired_leases_recovered",
                    0,
                )
                or 0
            )
        except Exception:
            pass

        return recovered

    def _supervisor_tick(self) -> None:
        supervisor = self.worker_supervisor

        tick = getattr(
            supervisor,
            "tick",
            None,
        )

        if tick is None:
            return

        result = tick()

        if isinstance(result, dict):
            fenced = int(
                result.get(
                    "fenced_workers",
                    0,
                )
                or 0
            )

            if fenced:
                with self._lock:
                    self._stats[
                        "worker_fences"
                    ] += fenced

    def cycle(
        self,
        worker_id: Optional[str] = None,
        worker_batch_size: int = 100,
    ) -> ProductionIntegrationResult:
        """
        Execute one complete production integration cycle.

        The cycle is bounded in memory while the overall system
        remains continuously expandable.
        """

        if worker_batch_size < 1:
            raise ValueError(
                "worker_batch_size must be >= 1"
            )

        with self._lock:
            self._stats["cycles"] += 1

        discovered = 0
        expanded = 0
        activated = 0
        indexed = 0
        recovered = 0
        failed = 0

        try:
            candidates = self._federate_once()

            discovered = len(candidates)

            if candidates:
                expanded = self._expand(
                    candidates
                )

        except Exception:
            with self._lock:
                self._stats[
                    "runtime_errors"
                ] += 1

        if worker_id is not None:
            try:
                (
                    activated,
                    indexed,
                    activation_failed,
                ) = self._activate_worker_batch(
                    worker_id=worker_id,
                    limit=worker_batch_size,
                )

                failed += activation_failed

            except Exception:
                failed += 1

                with self._lock:
                    self._stats[
                        "runtime_errors"
                    ] += 1

        try:
            recovered = self._recover()
        except Exception:
            with self._lock:
                self._stats[
                    "runtime_errors"
                ] += 1

        try:
            self._supervisor_tick()
        except Exception:
            with self._lock:
                self._stats[
                    "runtime_errors"
                ] += 1

        with self._lock:
            self._stats["source_results"] += discovered
            self._stats["discovered"] += discovered
            self._stats["expanded"] += expanded
            self._stats["activated"] += activated
            self._stats["indexed"] += indexed
            self._stats["recovered"] += recovered
            self._stats["failed"] += failed

        return ProductionIntegrationResult(
            discovered=discovered,
            expanded=expanded,
            activated=activated,
            indexed=indexed,
            recovered=recovered,
            failed=failed,
        )

    def run(
        self,
        worker_id: str,
        interval: float = 5.0,
        worker_batch_size: int = 100,
    ) -> None:
        if interval < 0:
            raise ValueError(
                "interval must be >= 0"
            )

        self._stop_event.clear()

        while not self._stop_event.is_set():
            try:
                self.cycle(
                    worker_id=worker_id,
                    worker_batch_size=worker_batch_size,
                )
            except Exception:
                with self._lock:
                    self._stats[
                        "runtime_errors"
                    ] += 1

            if interval:
                self._stop_event.wait(
                    interval
                )

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopped(self) -> bool:
        return self._stop_event.is_set()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            result = dict(self._stats)

        result["version"] = self.VERSION

        try:
            result["expansion"] = (
                self.expansion.stats()
            )
        except Exception:
            result["expansion"] = {}

        try:
            result["recovery"] = (
                self.recovery.stats()
            )
        except Exception:
            result["recovery"] = {}

        try:
            result["storage_index"] = (
                self.storage_index.stats()
            )
        except Exception:
            result["storage_index"] = {}

        try:
            result["workers"] = (
                self.worker_supervisor.stats()
            )
        except Exception:
            result["workers"] = {}

        return result
