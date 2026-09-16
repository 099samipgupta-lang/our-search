from __future__ import annotations

import threading
import time
from typing import Any, Optional

from crawler_system.certificate_transparency_domain_source import (
    CertificateTransparencyDomainSource,
)
from crawler_system.domain_candidate_pipeline import DomainCandidatePipeline
from crawler_system.domain_discovery_fabric import DomainDiscoveryFabric
from crawler_system.domain_discovery_fabric_controller import (
    DomainDiscoveryFabricController,
)
from crawler_system.domain_discovery_sources import (
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
)


class GlobalWebDiscoveryIntegration:
    """
    Production bridge between independent global domain discovery and
    the existing WholeWebCrawler.

    Architecture:

        independent discovery sources
                    ↓
        DomainDiscoverySourceRegistry
                    ↓
        DomainCandidatePipeline
                    ↓
        durable sharded DomainDiscoveryFabric
                    ↓
        DomainDiscoveryFabricController
                    ↓
        crawler._add_url()
                    ↓
        existing durable URL frontier

    This subsystem does not replace the crawler, frontier, index, or
    existing Web-graph discovery system.
    """

    VERSION = "global-web-discovery.v2"

    def __init__(
        self,
        crawler,
        storage_root: str,
        registry: DomainDiscoverySourceRegistry | None = None,
        shard_count: int = 64,
        lease_timeout: float = 300.0,
        discovery_workers: int = 8,
        domain_workers: int = 32,
        claim_batch_size: int = 100,
        lease_recovery_interval: float = 30.0,
        cycle_interval: float = 5.0,
        max_contexts_per_cycle: int = 100,
        worker_idle_sleep: float = 0.25,
    ):
        if crawler is None:
            raise ValueError("crawler must not be None")

        self.crawler = crawler

        self.storage_root = str(storage_root).rstrip("/")
        if not self.storage_root:
            raise ValueError("storage_root must not be empty")

        self.registry = (
            registry
            if registry is not None
            else DomainDiscoverySourceRegistry()
        )

        self.fabric = DomainDiscoveryFabric(
            storage_root=(
                f"{self.storage_root}/domain_discovery_fabric"
            ),
            shard_count=shard_count,
            lease_timeout=lease_timeout,
        )

        self.candidate_pipeline = DomainCandidatePipeline()

        self.controller = DomainDiscoveryFabricController(
            registry=self.registry,
            fabric=self.fabric,
            candidate_pipeline=self.candidate_pipeline,
            activation_callback=self._activate_domain,
            discovery_workers=discovery_workers,
            domain_workers=domain_workers,
            claim_batch_size=claim_batch_size,
            lease_recovery_interval=lease_recovery_interval,
            cycle_interval=cycle_interval,
            max_contexts_per_cycle=max_contexts_per_cycle,
            worker_idle_sleep=worker_idle_sleep,
        )

        self.cycle_interval = max(0.0, float(cycle_interval))
        self._last_cycle_at = 0.0

        self._lock = threading.RLock()

        self.stats: dict[str, Any] = {
            "version": self.VERSION,
            "cycles": 0,
            "scheduled_cycles": 0,
            "skipped_cycles": 0,
            "discoveries": 0,
            "processed_rounds": 0,
            "activations": 0,
            "activation_failures": 0,
            "runtime_errors": 0,
            "last_cycle_at": None,
            "last_error": None,
        }

    # =============================================================
    # DOMAIN ACTIVATION
    # =============================================================

    def _activate_domain(
        self,
        hostname: str,
        url: str,
        priority: int,
        source: Optional[str],
    ) -> bool:
        """
        Inject a newly discovered domain's first URL directly into the
        production crawler ingestion seam.

        The global discovery fabric therefore feeds the same durable
        URL-state/frontier machinery used by ordinary discoveries.
        """

        if not isinstance(url, str) or not url.strip():
            return False

        source_name = (
            source.strip()
            if isinstance(source, str) and source.strip()
            else "global_domain_discovery"
        )

        try:
            added = self.crawler._add_url(
                url.strip(),
                source=source_name,
                depth=0,
                seed=False,
                source_url=url.strip(),
            )

            if added:
                with self._lock:
                    self.stats["activations"] += 1
                return True

            normalized = self.crawler.normalizer.normalize(
                url.strip()
            )

            if normalized is not None:
                existing = self.crawler.url_state.get(
                    normalized
                )

                if existing is not None:
                    with self._lock:
                        self.stats["activations"] += 1
                    return True

        except Exception as exc:
            with self._lock:
                self.stats["activation_failures"] += 1
                self.stats["last_error"] = repr(exc)

        with self._lock:
            self.stats["activation_failures"] += 1

        return False

    # =============================================================
    # DISCOVERY
    # =============================================================

    def discover_once(
        self,
        contexts: Optional[list[DomainDiscoveryContext]] = None,
    ) -> dict[str, Any]:
        if contexts is None:
            contexts = [DomainDiscoveryContext()]

        result = self.controller.discover_contexts(
            contexts
        )

        with self._lock:
            self.stats["discoveries"] += 1

        return result

    # =============================================================
    # DURABLE PROCESSING
    # =============================================================

    def process_once(
        self,
        max_rounds: int = 1,
    ) -> dict[str, Any]:
        recovered = self.fabric.recover_expired_leases()

        result = self.controller.process_available(
            max_rounds=max_rounds
        )

        with self._lock:
            self.stats["processed_rounds"] += 1

        if isinstance(result, dict):
            result.setdefault(
                "recovered_leases",
                recovered,
            )

        return result

    # =============================================================
    # PRODUCTION CYCLE
    # =============================================================

    def cycle(
        self,
        force: bool = False,
    ) -> Optional[dict[str, Any]]:
        """
        Execute one scheduled global-discovery cycle.

        Scheduling is deliberately separated from WholeWebCrawler's
        much faster URL-fetch loop so external discovery sources are
        not called once per fetch cycle.
        """

        now = time.monotonic()

        with self._lock:
            if (
                not force
                and self.cycle_interval > 0.0
                and now - self._last_cycle_at < self.cycle_interval
            ):
                self.stats["skipped_cycles"] += 1
                return None

            self._last_cycle_at = now
            self.stats["scheduled_cycles"] += 1

        try:
            discovery = self.discover_once()
            processing = self.process_once(
                max_rounds=1
            )

            result = {
                "discovery": discovery,
                "processing": processing,
            }

            with self._lock:
                self.stats["cycles"] += 1
                self.stats["last_cycle_at"] = time.time()
                self.stats["last_error"] = None

            return result

        except Exception as exc:
            with self._lock:
                self.stats["runtime_errors"] += 1
                self.stats["last_error"] = repr(exc)

            return {
                "error": repr(exc),
            }

    # =============================================================
    # LIFECYCLE COMPATIBILITY
    # =============================================================

    def start(self, interval: Optional[float] = None) -> bool:
        """
        Optional standalone lifecycle.

        WholeWebCrawler production mode does not need this thread;
        it drives cycle() from its own lifecycle.
        """

        return False

    def stop(self, timeout: float = 30.0) -> bool:
        return True

    def status(self) -> dict[str, Any]:
        with self._lock:
            stats = dict(self.stats)

        return {
            "version": self.VERSION,
            "stats": stats,
            "fabric": self.fabric.stats(),
            "controller": self.controller.metrics().__dict__,
            "sources": self.registry.names(),
        }

    def close(self) -> None:
        self.stop()
