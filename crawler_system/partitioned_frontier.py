"""
Stage 5.1.2 — Partitioned crawl frontier.

Compatibility facade over the existing CrawlFrontier.

Each host is permanently assigned to one partition.
Each partition has its own URLStateStore and CrawlFrontier.

This removes the single SQLite database as the global write bottleneck.
"""

from __future__ import annotations

import os
import threading
import time

from crawler_system.frontier import CrawlFrontier
from crawler_system.partition_registry import PartitionRegistry
from crawler_system.partition_router import PartitionRouter


class PartitionedFrontier:
    def __init__(
        self,
        partition_count: int = 4,
        default_delay: float = 2,
        max_retries: int = 3,
        storage_root: str = "crawler_storage/partitions",
    ):
        self.partition_count = int(partition_count)

        if self.partition_count <= 0:
            raise ValueError(
                "partition_count must be greater than zero"
            )

        self.default_delay = float(default_delay)
        self.max_retries = int(max_retries)

        self.router = PartitionRouter(
            partition_count=self.partition_count,
            database_root=storage_root,
        )

        self.registry = PartitionRegistry(self.router)

        self.frontiers = {}

        self._lock = threading.RLock()

        self._owner = (
            f"partitioned-frontier-"
            f"{os.getpid()}-"
            f"{id(self)}"
        )

        self._next_partition = 0

        self._initialize_frontiers()

    def _initialize_frontiers(self):
        for partition_id in range(self.partition_count):
            store = self.registry.store_for_partition(
                partition_id
            )

            frontier = CrawlFrontier(
                default_delay=self.default_delay,
                max_retries=self.max_retries,
                state_store=store,
            )

            # Make lease ownership unique across the entire crawler.
            frontier.owner = (
                f"{self._owner}-p{partition_id}"
            )

            self.frontiers[partition_id] = frontier

    def partition_for(self, url: str) -> int:
        return self.router.partition_for(url)

    def state_store_for(self, url: str):
        return self.registry.store_for_url(url)

    def frontier_for(self, url: str) -> CrawlFrontier:
        partition_id = self.partition_for(url)
        return self.frontiers[partition_id]

    @property
    def state_store(self):
        """
        Compatibility property.

        New code should use state_store_for(url), because there is no
        longer one global state store.
        """
        return None

    def add(
        self,
        url,
        priority=0,
        available_at=None,
        source=None,
    ):
        frontier = self.frontier_for(url)

        return frontier.add(
            url,
            priority=priority,
            available_at=available_at,
        )

    def enqueue(
        self,
        url,
        priority=0,
        available_at=None,
        source=None,
    ):
        return self.add(
            url,
            priority=priority,
            available_at=available_at,
            source=source,
        )

    def get_next(self):
        """
        Claim the next available URL from any partition.

        Every claim probes every partition. This is intentionally exhaustive
        for Stage 5.1.2 so queued work cannot become stranded behind the
        partition rotation cursor.

        Each partition still performs its own atomic SQLite claim, so there
        is no shared SQLite write transaction across partitions.
        """

        with self._lock:
            for partition_id in range(self.partition_count):
                frontier = self.frontiers[partition_id]

                url = frontier.get_next()

                if url is not None:
                    return url

        return None

    def get_state(self, url: str):
        return self.state_store_for(url).get(url)

    def complete(self, url):
        return self.frontier_for(url).complete(url)

    def mark_crawled(self, url):
        return self.frontier_for(url).mark_crawled(url)

    def release(
        self,
        url,
        retry_at=None,
        priority=None,
        available_at=None,
    ):
        return self.frontier_for(url).release(
            url,
            retry_at=retry_at,
            priority=priority,
            available_at=available_at,
        )

    def recover_leases(self):
        recovered = 0

        for frontier in self.frontiers.values():
            recovered += frontier.recover_leases()

        return recovered

    def mark_failed(
        self,
        url,
        error=None,
        retry_delay=30,
    ):
        return self.frontier_for(url).mark_failed(
            url,
            error=error,
            retry_delay=retry_delay,
        )

    def reprioritize(self, url, priority):
        return self.frontier_for(url).reprioritize(
            url,
            priority,
        )

    def size(self):
        return sum(
            frontier.size()
            for frontier in self.frontiers.values()
        )

    def queued_size(self):
        return sum(
            frontier.queued_size()
            for frontier in self.frontiers.values()
        )

    def leased_size(self):
        return sum(
            frontier.leased_size()
            for frontier in self.frontiers.values()
        )

    def counts(self):
        aggregate = {
            "discovered": 0,
            "queued": 0,
            "leased": 0,
            "crawled": 0,
            "retry": 0,
            "failed": 0,
        }

        for frontier in self.frontiers.values():
            counts = frontier.state_store.counts()

            for key in aggregate:
                aggregate[key] += int(
                    counts.get(key, 0)
                )

        return aggregate

    def partition_counts(self):
        result = {}

        for partition_id, frontier in self.frontiers.items():
            result[partition_id] = frontier.state_store.counts()

        return result

    def close(self):
        self.registry.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
