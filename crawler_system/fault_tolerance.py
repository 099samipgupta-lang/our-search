from __future__ import annotations

import threading
import time
from dataclasses import dataclass


class FaultToleranceError(Exception):
    """Raised when crawler fault-tolerance invariants are violated."""


@dataclass(frozen=True)
class RecoveryReport:
    recovered_leases: int
    timestamp: float


class FaultToleranceManager:
    """
    Fault-tolerance control plane for the crawler.

    URLStateStore remains the authoritative durable source of URL,
    lease, concurrency, retry, and recovery state.
    """

    def __init__(
        self,
        frontier,
        *,
        lease_timeout: float = 30.0,
    ):
        if lease_timeout < 0:
            raise ValueError(
                "lease_timeout must not be negative"
            )

        self.frontier = frontier
        self.lease_timeout = float(lease_timeout)
        self._lock = threading.RLock()

    def _stores(self):
        stores = []

        registry = getattr(self.frontier, "registry", None)
        if registry is not None:
            stores.extend(
                registry.stores().values()
            )

        state_store = getattr(
            self.frontier,
            "state_store",
            None,
        )

        if state_store is not None:
            stores.append(state_store)

        result = []
        seen = set()

        for store in stores:
            identity = id(store)

            if identity in seen:
                continue

            seen.add(identity)
            result.append(store)

        return result

    def recover_expired_leases(
        self,
        *,
        now: float | None = None,
    ) -> RecoveryReport:
        timestamp = (
            time.time()
            if now is None
            else float(now)
        )

        if timestamp < 0:
            raise ValueError(
                "now must be non-negative"
            )

        recovered = 0

        with self._lock:
            for store in self._stores():
                recovered += int(
                    store.recover_expired_leases(
                        lease_timeout=self.lease_timeout,
                        now=timestamp,
                    )
                )

        return RecoveryReport(
            recovered_leases=recovered,
            timestamp=timestamp,
        )

    def validate_no_negative_concurrency(self) -> bool:
        """
        Validate concurrency counters associated with leased URLs.

        URLStateStore guarantees that lease recovery and lifecycle
        operations clamp counters at zero. This check verifies the
        visible leased-state invariants without introducing another
        source of truth.
        """
        for store in self._stores():
            leased = store.list_by_state("leased")

            for record in leased:
                host = record.get("host")

                if not host:
                    raise FaultToleranceError(
                        "leased URL has no host: "
                        f"{record.get('url')}"
                    )

                host_record = store.get_host(host)

                if host_record is None:
                    raise FaultToleranceError(
                        "leased URL references missing host: "
                        f"{host}"
                    )

                active = int(
                    host_record["active_concurrency"]
                )

                if active < 0:
                    raise FaultToleranceError(
                        "negative host concurrency: "
                        f"{host}"
                    )

        return True

    def validate_leases(self) -> bool:
        for store in self._stores():
            leased = store.list_by_state("leased")

            for record in leased:
                if not record.get("lease_owner"):
                    raise FaultToleranceError(
                        "leased URL has no lease owner: "
                        f"{record.get('url')}"
                    )

                if record.get("leased_at") is None:
                    raise FaultToleranceError(
                        "leased URL has no leased_at: "
                        f"{record.get('url')}"
                    )

        return True

    def validate(self) -> bool:
        self.validate_no_negative_concurrency()
        self.validate_leases()
        return True
