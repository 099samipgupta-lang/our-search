"""
OUR SEARCH Storage — Milestone 13
Production storage metrics.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class StorageMetricsSnapshot:
    puts: int
    gets: int
    exists: int
    deletes: int
    errors: int


class StorageMetrics:
    """Thread-safe counters for core storage operations."""

    def __init__(self):
        import threading

        self._lock = threading.Lock()
        self._puts = 0
        self._gets = 0
        self._exists = 0
        self._deletes = 0
        self._errors = 0

    def record_put(self):
        with self._lock:
            self._puts += 1

    def record_get(self):
        with self._lock:
            self._gets += 1

    def record_exists(self):
        with self._lock:
            self._exists += 1

    def record_delete(self):
        with self._lock:
            self._deletes += 1

    def record_error(self):
        with self._lock:
            self._errors += 1

    def snapshot(self):
        with self._lock:
            return StorageMetricsSnapshot(
                puts=self._puts,
                gets=self._gets,
                exists=self._exists,
                deletes=self._deletes,
                errors=self._errors,
            )
