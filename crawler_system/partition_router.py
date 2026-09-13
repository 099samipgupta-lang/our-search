"""
Stage 5.1.2 — Deterministic crawl partition routing.

A partition is the ownership boundary for URL/host state.

IMPORTANT:
Partitioning is deliberately host-based rather than URL-based.
All URLs belonging to the same host must share one partition so that
crawl-delay, leases, retries, and host scheduling cannot be bypassed.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Partition:
    partition_id: int
    database_path: str


class PartitionRouter:
    """
    Deterministically maps crawl URLs to partitions.

    Ownership key:
        normalized hostname

    Hash:
        SHA-256

    This is intentionally independent of Python's built-in hash(),
    because Python hash randomization can produce different values
    in different processes.
    """

    def __init__(
        self,
        partition_count: int,
        database_root: str = "crawler_storage/partitions",
    ):
        partition_count = int(partition_count)

        if partition_count <= 0:
            raise ValueError("partition_count must be greater than zero")

        self.partition_count = partition_count
        self.database_root = database_root

    @staticmethod
    def normalize_host(url: str) -> str:
        """
        Return the deterministic host ownership key.

        Includes port when explicitly present because the current crawler
        stores hosts using urlparse(url).netloc semantics.
        """
        parsed = urlsplit(str(url).strip())

        hostname = parsed.hostname
        if not hostname:
            raise ValueError(f"URL has no hostname: {url!r}")

        hostname = hostname.lower().rstrip(".")

        # Preserve explicit non-default ports.
        port = parsed.port

        if port is not None:
            if ":" in hostname and not hostname.startswith("["):
                hostname = f"[{hostname}]"
            return f"{hostname}:{port}"

        return hostname

    @classmethod
    def ownership_key(cls, url: str) -> str:
        return cls.normalize_host(url)

    def partition_for(self, url: str) -> int:
        host = self.ownership_key(url)

        digest = hashlib.sha256(
            host.encode("utf-8")
        ).digest()

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return value % self.partition_count

    def partition_path(self, partition_id: int) -> str:
        partition_id = int(partition_id)

        if not 0 <= partition_id < self.partition_count:
            raise ValueError(
                f"invalid partition id {partition_id}; "
                f"expected 0..{self.partition_count - 1}"
            )

        return (
            f"{self.database_root}/"
            f"partition_{partition_id}/"
            f"state.sqlite3"
        )

    def partition(self, url: str) -> Partition:
        partition_id = self.partition_for(url)

        return Partition(
            partition_id=partition_id,
            database_path=self.partition_path(partition_id),
        )

    def all_partitions(self) -> list[Partition]:
        return [
            Partition(
                partition_id=i,
                database_path=self.partition_path(i),
            )
            for i in range(self.partition_count)
        ]
