"""
OUR SEARCH Storage — Milestone 13
Production health and diagnostics.
"""

from dataclasses import dataclass
from time import time

from .integrity_monitor import StorageIntegrityMonitor


@dataclass(frozen=True)
class StorageHealth:
    healthy: bool
    checked_at: float
    backend: str
    objects: int
    errors: int
    details: str = ""


class StorageHealthMonitor:
    """Provides production-oriented storage health diagnostics."""

    def __init__(self, storage):
        self.storage = storage
        self._errors = 0
        self.integrity_monitor = StorageIntegrityMonitor(storage)

    def record_error(self):
        self._errors += 1

    def check(self, prefix=""):
        checked_at = time()

        try:
            keys = self.storage.list_keys(prefix)

            return StorageHealth(
                healthy=True,
                checked_at=checked_at,
                backend=type(self.storage).__name__,
                objects=len(keys),
                errors=self._errors,
                details="storage namespace readable",
            )

        except Exception as exc:
            self._errors += 1

            return StorageHealth(
                healthy=False,
                checked_at=checked_at,
                backend=type(self.storage).__name__,
                objects=0,
                errors=self._errors,
                details=str(exc),
            )

    def require_healthy(self, prefix=""):
        health = self.check(prefix)

        if not health.healthy:
            raise RuntimeError(
                f"Storage health check failed: "
                f"{health.details}"
            )

        return health
