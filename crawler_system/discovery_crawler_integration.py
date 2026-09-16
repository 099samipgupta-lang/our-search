from __future__ import annotations

"""
OUR SEARCH — Discovery → Crawler Integration

Phase 7
Brick 7.2

Version:
    discovery-crawler-integration.v1

Purpose:
    Provides the canonical production handoff between the enormous-scale
    global discovery plane and WholeWebCrawler.

Flow:

    Discovery
        ↓
    DiscoveryWorkContract
        ↓
    Durable Work Queue
        ↓
    Placement-Aware Router
        ↓
    Worker Lease + Fencing
        ↓
    Crawler Activation
        ↓
    Crawl Frontier

This layer deliberately does not own discovery, queue persistence,
placement, worker supervision, or crawling. It coordinates their
interfaces through the canonical work contract.
"""

from dataclasses import dataclass
from typing import Any, Callable, Optional

from crawler_system.global_discovery_work_contract import (
    CONTRACT_VERSION,
    DiscoveryWorkContract,
    contract_from_candidate,
    contract_from_work_item,
    validate_contract,
)


INTEGRATION_VERSION = "discovery-crawler-integration.v1"


@dataclass(frozen=True)
class CrawlerActivationResult:
    work_id: str
    hostname: str
    url: str
    activated: bool
    duplicate: bool = False
    error: Optional[str] = None


class DiscoveryCrawlerIntegration:
    """
    Production discovery-to-crawler handoff coordinator.

    The integration is intentionally dependency-light. Existing queue,
    router, worker-supervisor, and crawler implementations are injected
    instead of being replaced.
    """

    def __init__(
        self,
        crawler: Any,
        work_queue: Any,
        work_router: Any,
        worker_supervisor: Any,
        storage_root: str = "crawler_storage",
        activation_callback: Optional[
            Callable[[DiscoveryWorkContract], bool]
        ] = None,
    ):
        self.crawler = crawler
        self.work_queue = work_queue
        self.work_router = work_router
        self.worker_supervisor = worker_supervisor
        self.storage_root = storage_root
        self.activation_callback = activation_callback

        self._stats = {
            "contracts_created": 0,
            "contracts_rejected": 0,
            "work_acquired": 0,
            "work_activated": 0,
            "duplicates": 0,
            "activation_failures": 0,
            "queue_completions": 0,
            "queue_failures": 0,
            "lease_renewals": 0,
            "recovered_work": 0,
        }

    # ------------------------------------------------------------------
    # CONTRACT CREATION
    # ------------------------------------------------------------------

    def contract_from_candidate(
        self,
        candidate: Any,
    ) -> DiscoveryWorkContract:
        contract = contract_from_candidate(candidate)
        validate_contract(contract)

        self._stats["contracts_created"] += 1
        return contract

    def contract_from_work_item(
        self,
        work_item: Any,
    ) -> DiscoveryWorkContract:
        contract = contract_from_work_item(work_item)
        validate_contract(contract)

        self._stats["contracts_created"] += 1
        return contract

    # ------------------------------------------------------------------
    # CRAWLER ACTIVATION
    # ------------------------------------------------------------------

    def _activate(
        self,
        contract: DiscoveryWorkContract,
    ) -> bool:
        """
        Activate one canonical contract in WholeWebCrawler.

        Preferred path:
            explicitly injected callback.

        Fallback:
            WholeWebCrawler._add_url(...)
            or WholeWebCrawler.add_url(...)
        """

        validate_contract(contract)

        if self.activation_callback is not None:
            return bool(
                self.activation_callback(contract)
            )

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
            raise AttributeError(
                "crawler does not expose _add_url or add_url"
            )

        result = add_url(contract.url)

        return bool(result)

    def activate_contract(
        self,
        contract: DiscoveryWorkContract,
    ) -> CrawlerActivationResult:
        try:
            validate_contract(contract)

            activated = self._activate(contract)

            if activated:
                self._stats["work_activated"] += 1

                return CrawlerActivationResult(
                    work_id=contract.work_id,
                    hostname=contract.hostname,
                    url=contract.url,
                    activated=True,
                )

            self._stats["duplicates"] += 1

            return CrawlerActivationResult(
                work_id=contract.work_id,
                hostname=contract.hostname,
                url=contract.url,
                activated=False,
                duplicate=True,
            )

        except Exception as exc:
            self._stats["activation_failures"] += 1

            return CrawlerActivationResult(
                work_id=contract.work_id,
                hostname=contract.hostname,
                url=contract.url,
                activated=False,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # ROUTER HANDOFF
    # ------------------------------------------------------------------

    def acquire(
        self,
        worker_id: str,
        limit: int = 100,
    ) -> list[Any]:
        """
        Acquire router work for a crawler worker.

        The router remains responsible for queue claiming, placement,
        leases, and fencing.
        """

        if limit <= 0:
            return []

        acquire = getattr(
            self.work_router,
            "acquire",
            None,
        )

        if acquire is None:
            acquire = getattr(
                self.work_router,
                "acquire_from_any_partition",
                None,
            )

        if acquire is None:
            raise AttributeError(
                "work router does not expose an acquisition interface"
            )

        try:
            result = acquire(
                worker_id=worker_id,
                limit=limit,
            )
        except TypeError:
            try:
                result = acquire(
                    worker_id,
                    limit,
                )
            except TypeError:
                result = acquire(
                    worker_id,
                )

        items = list(result or [])

        self._stats["work_acquired"] += len(items)

        return items

    # ------------------------------------------------------------------
    # BATCH ACTIVATION
    # ------------------------------------------------------------------

    def activate_batch(
        self,
        worker_id: str,
        limit: int = 100,
    ) -> list[CrawlerActivationResult]:
        routed_items = self.acquire(
            worker_id=worker_id,
            limit=limit,
        )

        results: list[CrawlerActivationResult] = []

        for item in routed_items:
            try:
                contract = self.contract_from_work_item(item)
            except Exception as exc:
                self._stats["contracts_rejected"] += 1

                work_id = str(
                    getattr(item, "work_id", "")
                )

                hostname = str(
                    getattr(item, "hostname", "")
                )

                url = str(
                    getattr(item, "url", "")
                )

                results.append(
                    CrawlerActivationResult(
                        work_id=work_id,
                        hostname=hostname,
                        url=url,
                        activated=False,
                        error=str(exc),
                    )
                )

                self._fail_item(
                    item,
                    str(exc),
                )

                continue

            activation = self.activate_contract(
                contract
            )

            results.append(activation)

            if activation.activated:
                self._complete_item(item)
            elif activation.duplicate:
                self._complete_item(item)
            else:
                self._fail_item(
                    item,
                    activation.error or "activation failed",
                )

        return results

    # ------------------------------------------------------------------
    # ROUTER COMPLETION / FAILURE
    # ------------------------------------------------------------------

    def _complete_item(
        self,
        item: Any,
    ) -> None:
        work_id = getattr(
            item,
            "work_id",
            None,
        )

        if not work_id:
            return

        complete = getattr(
            self.work_router,
            "complete",
            None,
        )

        if complete is None:
            complete = getattr(
                self.work_queue,
                "complete",
                None,
            )

        if complete is None:
            return

        fencing_token = getattr(
            item,
            "fencing_token",
            None,
        )

        worker_id = getattr(
            item,
            "worker_id",
            None,
        )

        try:
            if fencing_token is not None:
                complete(
                    work_id,
                    worker_id=worker_id,
                    fencing_token=fencing_token,
                )
            else:
                complete(work_id)

            self._stats["queue_completions"] += 1

        except TypeError:
            try:
                complete(work_id)
                self._stats["queue_completions"] += 1
            except Exception:
                pass
        except Exception:
            pass

    def _fail_item(
        self,
        item: Any,
        error: str,
    ) -> None:
        work_id = getattr(
            item,
            "work_id",
            None,
        )

        if not work_id:
            return

        fail = getattr(
            self.work_router,
            "fail",
            None,
        )

        if fail is None:
            fail = getattr(
                self.work_queue,
                "fail",
                None,
            )

        if fail is None:
            return

        fencing_token = getattr(
            item,
            "fencing_token",
            None,
        )

        worker_id = getattr(
            item,
            "worker_id",
            None,
        )

        try:
            if fencing_token is not None:
                fail(
                    work_id,
                    worker_id=worker_id,
                    fencing_token=fencing_token,
                    error=error,
                )
            else:
                fail(
                    work_id,
                    error=error,
                )

            self._stats["queue_failures"] += 1

        except TypeError:
            try:
                fail(
                    work_id,
                    error,
                )
                self._stats["queue_failures"] += 1
            except Exception:
                pass
        except Exception:
            pass

    # ------------------------------------------------------------------
    # LEASE MANAGEMENT
    # ------------------------------------------------------------------

    def renew_item(
        self,
        item: Any,
    ) -> bool:
        renew = getattr(
            self.work_router,
            "renew",
            None,
        )

        if renew is None:
            return False

        work_id = getattr(
            item,
            "work_id",
            None,
        )

        worker_id = getattr(
            item,
            "worker_id",
            None,
        )

        fencing_token = getattr(
            item,
            "fencing_token",
            None,
        )

        if not work_id:
            return False

        try:
            if fencing_token is not None:
                result = renew(
                    work_id,
                    worker_id=worker_id,
                    fencing_token=fencing_token,
                )
            else:
                result = renew(work_id)

            if result:
                self._stats["lease_renewals"] += 1

            return bool(result)

        except TypeError:
            try:
                result = renew(work_id)

                if result:
                    self._stats["lease_renewals"] += 1

                return bool(result)

            except Exception:
                return False

        except Exception:
            return False

    # ------------------------------------------------------------------
    # RECOVERY
    # ------------------------------------------------------------------

    def recover(self) -> int:
        recover = getattr(
            self.work_router,
            "recover_expired",
            None,
        )

        if recover is None:
            recover = getattr(
                self.work_queue,
                "recover_expired",
                None,
            )

        if recover is None:
            return 0

        try:
            result = recover()

            if isinstance(result, int):
                recovered = result
            else:
                recovered = len(result or [])

            self._stats["recovered_work"] += recovered

            return recovered

        except Exception:
            return 0

    # ------------------------------------------------------------------
    # WORKER RELEASE
    # ------------------------------------------------------------------

    def release_worker(
        self,
        worker_id: str,
    ) -> int:
        release = getattr(
            self.work_router,
            "release_worker",
            None,
        )

        if release is None:
            return 0

        try:
            result = release(worker_id)

            if isinstance(result, int):
                return result

            return len(result or [])

        except Exception:
            return 0

    # ------------------------------------------------------------------
    # SINGLE CYCLE
    # ------------------------------------------------------------------

    def cycle(
        self,
        worker_id: str,
        batch_size: int = 100,
    ) -> list[CrawlerActivationResult]:
        """
        One bounded discovery-to-crawler handoff cycle.

        No fixed global workload ceiling is introduced here. batch_size
        only controls one worker's local execution quantum.
        """

        if batch_size <= 0:
            return []

        return self.activate_batch(
            worker_id=worker_id,
            limit=batch_size,
        )

    # ------------------------------------------------------------------
    # OBSERVABILITY
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        return {
            **self._stats,
            "integration_version": INTEGRATION_VERSION,
            "contract_version": CONTRACT_VERSION,
        }


__all__ = [
    "INTEGRATION_VERSION",
    "CrawlerActivationResult",
    "DiscoveryCrawlerIntegration",
]
