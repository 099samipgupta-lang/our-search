"""
Stage 5.1.2 — Partition state registry.

Each partition owns an independent URLStateStore / SQLite database.

The registry is intentionally a local process abstraction for Stage 5.1.
It establishes the ownership boundary that can later be moved to separate
machines/process groups without changing the crawler's logical model.
"""

from __future__ import annotations

import os
import threading

from crawler_system.partition_router import PartitionRouter, Partition
from crawler_system.url_state import URLStateStore


class PartitionRegistry:
    def __init__(
        self,
        router: PartitionRouter,
        create_directories: bool = True,
    ):
        self.router = router
        self._stores: dict[int, URLStateStore] = {}
        self._lock = threading.RLock()

        if create_directories:
            os.makedirs(
                self.router.database_root,
                exist_ok=True,
            )

    def _open_partition(self, partition_id: int) -> URLStateStore:
        partition = Partition(
            partition_id=partition_id,
            database_path=self.router.partition_path(partition_id),
        )

        directory = os.path.dirname(partition.database_path)

        if directory:
            os.makedirs(directory, exist_ok=True)

        return URLStateStore(
            database_path=partition.database_path
        )

    def store_for_partition(
        self,
        partition_id: int,
    ) -> URLStateStore:
        partition_id = int(partition_id)

        if not 0 <= partition_id < self.router.partition_count:
            raise ValueError(
                f"invalid partition id: {partition_id}"
            )

        with self._lock:
            store = self._stores.get(partition_id)

            if store is None:
                store = self._open_partition(partition_id)
                self._stores[partition_id] = store

            return store

    def store_for_url(self, url: str) -> URLStateStore:
        partition_id = self.router.partition_for(url)
        return self.store_for_partition(partition_id)

    def partition_for_url(self, url: str) -> int:
        return self.router.partition_for(url)

    def partitions(self) -> list[int]:
        return list(range(self.router.partition_count))

    def stores(self) -> dict[int, URLStateStore]:
        with self._lock:
            return dict(self._stores)

    def close(self) -> None:
        with self._lock:
            stores = list(self._stores.values())
            self._stores.clear()

        for store in stores:
            try:
                store.close()
            except Exception:
                pass
