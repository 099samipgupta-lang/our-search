"""
OUR SEARCH Storage — Milestone 12
Capacity, Performance and Scale Testing Infrastructure.
"""

from dataclasses import dataclass
from time import perf_counter


@dataclass(frozen=True)
class BenchmarkResult:
    name: str
    operations: int
    elapsed_seconds: float

    @property
    def operations_per_second(self):
        if self.elapsed_seconds <= 0:
            return float("inf")
        return self.operations / self.elapsed_seconds


class StorageBenchmark:
    """Reusable benchmark runner for storage capacity/performance tests."""

    def __init__(self, storage):
        self.storage = storage

    def measure(self, name, operation, operations):
        start = perf_counter()

        for _ in range(operations):
            operation()

        elapsed = perf_counter() - start

        return BenchmarkResult(
            name=name,
            operations=operations,
            elapsed_seconds=elapsed,
        )


class CapacityBenchmark:
    """Measures storage capacity and basic sequential throughput."""

    def __init__(self, storage):
        self.storage = storage

    def write_objects(self, prefix, payload, count):
        start = perf_counter()

        for index in range(count):
            self.storage.put(f"{prefix}/{index}", payload)

        elapsed = perf_counter() - start

        return BenchmarkResult(
            name="sequential_write",
            operations=count,
            elapsed_seconds=elapsed,
        )

    def read_objects(self, prefix, count):
        start = perf_counter()

        for index in range(count):
            self.storage.get(f"{prefix}/{index}")

        elapsed = perf_counter() - start

        return BenchmarkResult(
            name="sequential_read",
            operations=count,
            elapsed_seconds=elapsed,
        )


class ConcurrentCapacityBenchmark:
    """Measures storage throughput under concurrent operations."""

    def __init__(self, storage):
        self.storage = storage

    def concurrent_write(self, prefix, payload, count, workers=4):
        from concurrent.futures import ThreadPoolExecutor

        def write(index):
            self.storage.put(f"{prefix}/{index}", payload)

        start = perf_counter()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(write, range(count)))

        elapsed = perf_counter() - start

        return BenchmarkResult(
            name="concurrent_write",
            operations=count,
            elapsed_seconds=elapsed,
        )

    def concurrent_read(self, prefix, count, workers=4):
        from concurrent.futures import ThreadPoolExecutor

        def read(index):
            return self.storage.get(f"{prefix}/{index}")

        start = perf_counter()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(read, range(count)))

        elapsed = perf_counter() - start

        return BenchmarkResult(
            name="concurrent_read",
            operations=count,
            elapsed_seconds=elapsed,
        )
