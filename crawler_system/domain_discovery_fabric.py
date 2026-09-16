from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional


@dataclass(frozen=True)
class DomainDiscoveryRecord:
    hostname: str
    url: str
    source: str
    evidence: Optional[str] = None
    discovered_at: Optional[float] = None
    metadata: Optional[dict[str, Any]] = None
    priority: float = 50.0


@dataclass(frozen=True)
class ClaimedDomainDiscovery:
    hostname: str
    url: str
    source: str
    evidence: Optional[str]
    discovered_at: float
    metadata: dict[str, Any]
    priority: float
    attempts: int
    shard_id: int


class DomainDiscoveryPartitioner:
    """
    Stable hostname partitioning.

    Hostnames are canonicalized before hashing.
    SHA-256 is used so ownership is deterministic across
    processes, machines and Python runtimes.
    """

    def __init__(self, shard_count: int):
        shard_count = int(shard_count)
        if shard_count <= 0:
            raise ValueError("shard_count must be greater than zero")
        self.shard_count = shard_count

    @staticmethod
    def normalize_hostname(hostname: str) -> Optional[str]:
        if not isinstance(hostname, str):
            return None

        hostname = hostname.strip().lower().rstrip(".")

        if not hostname:
            return None

        return hostname

    def partition(self, hostname: str) -> int:
        hostname = self.normalize_hostname(hostname)

        if hostname is None:
            raise ValueError("hostname must be non-empty")

        digest = hashlib.sha256(
            hostname.encode("utf-8", "strict")
        ).digest()

        value = int.from_bytes(
            digest[:8],
            "big",
            signed=False,
        )

        return value % self.shard_count


class DomainDiscoveryShard:
    """
    Durable shard-local domain-discovery queue.

    SQLite is deliberately scoped to one shard.
    There is no global SQLite database and no global process lock.

    A worker may claim work from one shard while another worker
    independently operates on another shard.
    """

    def __init__(
        self,
        shard_id: int,
        database_path: str,
        lease_timeout: float = 300.0,
    ):
        self.shard_id = int(shard_id)
        self.database_path = str(database_path)
        self.lease_timeout = float(lease_timeout)

        if self.shard_id < 0:
            raise ValueError("shard_id must be non-negative")

        if self.lease_timeout <= 0:
            raise ValueError("lease_timeout must be positive")

        self._lock = threading.RLock()

        directory = os.path.dirname(
            os.path.abspath(self.database_path)
        )

        if directory:
            os.makedirs(directory, exist_ok=True)

        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(
            self.database_path,
            timeout=30.0,
        )

        connection.row_factory = sqlite3.Row

        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")

        return connection

    def _initialize(self):
        with self._lock:
            connection = self._connect()

            try:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS domain_discovery_work (
                        hostname TEXT PRIMARY KEY,
                        url TEXT NOT NULL,
                        source TEXT NOT NULL,
                        evidence TEXT,
                        discovered_at REAL NOT NULL,
                        metadata_json TEXT NOT NULL DEFAULT '{}',
                        priority REAL NOT NULL DEFAULT 50.0,

                        status TEXT NOT NULL DEFAULT 'queued',

                        lease_owner TEXT,
                        leased_at REAL,

                        attempts INTEGER NOT NULL DEFAULT 0,

                        completed_at REAL,
                        failed_at REAL,
                        last_error TEXT
                    );

                    CREATE INDEX IF NOT EXISTS
                        idx_domain_discovery_ready
                    ON domain_discovery_work (
                        status,
                        priority DESC,
                        discovered_at ASC,
                        hostname ASC
                    );

                    CREATE INDEX IF NOT EXISTS
                        idx_domain_discovery_lease
                    ON domain_discovery_work (
                        status,
                        leased_at
                    );

                    CREATE INDEX IF NOT EXISTS
                        idx_domain_discovery_source
                    ON domain_discovery_work (
                        source
                    );

                    CREATE INDEX IF NOT EXISTS
                        idx_domain_discovery_priority
                    ON domain_discovery_work (
                        priority DESC
                    );
                    """
                )

                connection.commit()

            finally:
                connection.close()

    @staticmethod
    def _normalize_priority(priority: Any) -> float:
        try:
            value = float(priority)
        except (TypeError, ValueError):
            value = 50.0

        return max(
            0.0,
            min(100.0, value),
        )

    @staticmethod
    def _metadata_json(metadata: Any) -> str:
        if not isinstance(metadata, dict):
            metadata = {}

        try:
            return json.dumps(
                metadata,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        except (TypeError, ValueError):
            return "{}"

    @staticmethod
    def _metadata_from_json(value: Any) -> dict[str, Any]:
        try:
            result = json.loads(value or "{}")
        except (
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ):
            return {}

        return result if isinstance(result, dict) else {}

    def enqueue_many(
        self,
        records: Iterable[DomainDiscoveryRecord],
    ) -> dict[str, int]:

        inserted = 0
        duplicates = 0
        invalid = 0

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute("BEGIN IMMEDIATE")

                for record in records:
                    if not isinstance(
                        record,
                        DomainDiscoveryRecord,
                    ):
                        invalid += 1
                        continue

                    hostname = DomainDiscoveryPartitioner.normalize_hostname(
                        record.hostname
                    )

                    if hostname is None:
                        invalid += 1
                        continue

                    if (
                        not isinstance(record.url, str)
                        or not record.url
                    ):
                        invalid += 1
                        continue

                    if (
                        not isinstance(record.source, str)
                        or not record.source.strip()
                    ):
                        invalid += 1
                        continue

                    discovered_at = (
                        now
                        if record.discovered_at is None
                        else float(record.discovered_at)
                    )

                    priority = self._normalize_priority(
                        record.priority
                    )

                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO domain_discovery_work (
                            hostname,
                            url,
                            source,
                            evidence,
                            discovered_at,
                            metadata_json,
                            priority,
                            status
                        )
                        VALUES (
                            ?, ?, ?, ?, ?, ?, ?, 'queued'
                        )
                        """,
                        (
                            hostname,
                            record.url,
                            record.source.strip(),
                            record.evidence,
                            discovered_at,
                            self._metadata_json(
                                record.metadata
                            ),
                            priority,
                        ),
                    )

                    if cursor.rowcount == 1:
                        inserted += 1
                    else:
                        duplicates += 1

                connection.commit()

            except Exception:
                connection.rollback()
                raise

            finally:
                connection.close()

        return {
            "inserted": inserted,
            "duplicates": duplicates,
            "invalid": invalid,
        }

    def claim_many(
        self,
        limit: int,
        lease_owner: str,
        now: Optional[float] = None,
    ) -> list[ClaimedDomainDiscovery]:

        limit = max(1, int(limit))

        if (
            not isinstance(lease_owner, str)
            or not lease_owner.strip()
        ):
            raise ValueError(
                "lease_owner must be non-empty"
            )

        lease_owner = lease_owner.strip()

        now = (
            time.time()
            if now is None
            else float(now)
        )

        claimed: list[ClaimedDomainDiscovery] = []

        with self._lock:
            connection = self._connect()

            try:
                connection.execute("BEGIN IMMEDIATE")

                rows = connection.execute(
                    """
                    SELECT
                        hostname,
                        url,
                        source,
                        evidence,
                        discovered_at,
                        metadata_json,
                        priority,
                        attempts
                    FROM domain_discovery_work
                    WHERE status = 'queued'
                    ORDER BY
                        priority DESC,
                        discovered_at ASC,
                        hostname ASC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()

                for row in rows:
                    cursor = connection.execute(
                        """
                        UPDATE domain_discovery_work
                        SET
                            status = 'processing',
                            lease_owner = ?,
                            leased_at = ?,
                            attempts = attempts + 1
                        WHERE
                            hostname = ?
                            AND status = 'queued'
                        """,
                        (
                            lease_owner,
                            now,
                            row["hostname"],
                        ),
                    )

                    if cursor.rowcount != 1:
                        continue

                    claimed.append(
                        ClaimedDomainDiscovery(
                            hostname=row["hostname"],
                            url=row["url"],
                            source=row["source"],
                            evidence=row["evidence"],
                            discovered_at=float(
                                row["discovered_at"]
                            ),
                            metadata=self._metadata_from_json(
                                row["metadata_json"]
                            ),
                            priority=float(
                                row["priority"]
                            ),
                            attempts=int(
                                row["attempts"]
                            ) + 1,
                            shard_id=self.shard_id,
                        )
                    )

                connection.commit()

            except Exception:
                connection.rollback()
                raise

            finally:
                connection.close()

        return claimed

    def mark_complete(
        self,
        hostname: str,
        lease_owner: Optional[str] = None,
        now: Optional[float] = None,
    ) -> bool:

        hostname = (
            DomainDiscoveryPartitioner
            .normalize_hostname(hostname)
        )

        if hostname is None:
            return False

        now = (
            time.time()
            if now is None
            else float(now)
        )

        with self._lock:
            connection = self._connect()

            try:
                if lease_owner is None:
                    cursor = connection.execute(
                        """
                        UPDATE domain_discovery_work
                        SET
                            status = 'complete',
                            completed_at = ?,
                            lease_owner = NULL,
                            leased_at = NULL
                        WHERE
                            hostname = ?
                            AND status = 'processing'
                        """,
                        (now, hostname),
                    )
                else:
                    cursor = connection.execute(
                        """
                        UPDATE domain_discovery_work
                        SET
                            status = 'complete',
                            completed_at = ?,
                            lease_owner = NULL,
                            leased_at = NULL
                        WHERE
                            hostname = ?
                            AND status = 'processing'
                            AND lease_owner = ?
                        """,
                        (
                            now,
                            hostname,
                            lease_owner,
                        ),
                    )

                connection.commit()

                return cursor.rowcount == 1

            finally:
                connection.close()

    def mark_failed(
        self,
        hostname: str,
        error: str,
        retry: bool = True,
        lease_owner: Optional[str] = None,
        now: Optional[float] = None,
    ) -> bool:

        hostname = (
            DomainDiscoveryPartitioner
            .normalize_hostname(hostname)
        )

        if hostname is None:
            return False

        now = (
            time.time()
            if now is None
            else float(now)
        )

        status = (
            "queued"
            if retry
            else "failed"
        )

        with self._lock:
            connection = self._connect()

            try:
                if lease_owner is None:
                    cursor = connection.execute(
                        """
                        UPDATE domain_discovery_work
                        SET
                            status = ?,
                            failed_at = ?,
                            lease_owner = NULL,
                            leased_at = NULL,
                            last_error = ?
                        WHERE
                            hostname = ?
                            AND status = 'processing'
                        """,
                        (
                            status,
                            now,
                            str(error),
                            hostname,
                        ),
                    )
                else:
                    cursor = connection.execute(
                        """
                        UPDATE domain_discovery_work
                        SET
                            status = ?,
                            failed_at = ?,
                            lease_owner = NULL,
                            leased_at = NULL,
                            last_error = ?
                        WHERE
                            hostname = ?
                            AND status = 'processing'
                            AND lease_owner = ?
                        """,
                        (
                            status,
                            now,
                            str(error),
                            hostname,
                            lease_owner,
                        ),
                    )

                connection.commit()

                return cursor.rowcount == 1

            finally:
                connection.close()

    def recover_expired_leases(
        self,
        now: Optional[float] = None,
    ) -> int:

        now = (
            time.time()
            if now is None
            else float(now)
        )

        cutoff = now - self.lease_timeout

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE domain_discovery_work
                    SET
                        status = 'queued',
                        lease_owner = NULL,
                        leased_at = NULL
                    WHERE
                        status = 'processing'
                        AND leased_at IS NOT NULL
                        AND leased_at < ?
                    """,
                    (cutoff,),
                )

                connection.commit()

                return cursor.rowcount

            finally:
                connection.close()

    def count(
        self,
        status: Optional[str] = None,
    ) -> int:

        with self._lock:
            connection = self._connect()

            try:
                if status is None:
                    row = connection.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM domain_discovery_work
                        """
                    ).fetchone()
                else:
                    row = connection.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM domain_discovery_work
                        WHERE status = ?
                        """,
                        (status,),
                    ).fetchone()

                return int(row["count"])

            finally:
                connection.close()

    def stats(self) -> dict[str, Any]:

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT
                        status,
                        COUNT(*) AS count
                    FROM domain_discovery_work
                    GROUP BY status
                    """
                ).fetchall()

                result = {
                    "shard_id": self.shard_id,
                    "database_path": self.database_path,
                    "total": 0,
                    "queued": 0,
                    "processing": 0,
                    "complete": 0,
                    "failed": 0,
                }

                for row in rows:
                    status = row["status"]
                    count = int(row["count"])

                    if status in result:
                        result[status] = count

                    result["total"] += count

                return result

            finally:
                connection.close()


class DomainDiscoveryFabric:
    """
    Distributed durable domain-discovery fabric.

    Important:
        shard_count controls durable storage ownership.
        Worker count is independent from shard count.

    Therefore many workers can process the same fabric concurrently
    without creating one global queue lock.
    """

    def __init__(
        self,
        storage_root: str,
        shard_count: int = 64,
        lease_timeout: float = 300.0,
    ):
        shard_count = int(shard_count)

        if shard_count <= 0:
            raise ValueError(
                "shard_count must be greater than zero"
            )

        self.storage_root = str(storage_root)
        self.shard_count = shard_count
        self.lease_timeout = float(lease_timeout)

        os.makedirs(
            self.storage_root,
            exist_ok=True,
        )

        self.partitioner = DomainDiscoveryPartitioner(
            shard_count
        )

        self.shards = [
            DomainDiscoveryShard(
                shard_id=index,
                database_path=os.path.join(
                    self.storage_root,
                    f"domain_shard_{index:05d}.db",
                ),
                lease_timeout=self.lease_timeout,
            )
            for index in range(shard_count)
        ]

    def shard_for_hostname(
        self,
        hostname: str,
    ) -> int:
        return self.partitioner.partition(hostname)

    def enqueue(
        self,
        record: DomainDiscoveryRecord,
    ) -> dict[str, int]:

        shard_id = self.shard_for_hostname(
            record.hostname
        )

        return self.shards[shard_id].enqueue_many(
            [record]
        )

    def enqueue_many(
        self,
        records: Iterable[DomainDiscoveryRecord],
    ) -> dict[str, int]:

        grouped: dict[int, list[DomainDiscoveryRecord]] = {}

        for record in records:
            if not isinstance(
                record,
                DomainDiscoveryRecord,
            ):
                grouped.setdefault(-1, []).append(record)
                continue

            try:
                shard_id = self.shard_for_hostname(
                    record.hostname
                )
            except Exception:
                grouped.setdefault(-1, []).append(record)
                continue

            grouped.setdefault(
                shard_id,
                [],
            ).append(record)

        totals = {
            "inserted": 0,
            "duplicates": 0,
            "invalid": 0,
        }

        if -1 in grouped:
            totals["invalid"] += len(
                grouped.pop(-1)
            )

        for shard_id, shard_records in grouped.items():
            result = self.shards[shard_id].enqueue_many(
                shard_records
            )

            for key in totals:
                totals[key] += result[key]

        return totals

    def claim_many(
        self,
        shard_id: int,
        limit: int,
        lease_owner: str,
    ) -> list[ClaimedDomainDiscovery]:

        shard_id = int(shard_id)

        if (
            shard_id < 0
            or shard_id >= self.shard_count
        ):
            raise ValueError("invalid shard_id")

        return self.shards[shard_id].claim_many(
            limit=limit,
            lease_owner=lease_owner,
        )

    def recover_expired_leases(self) -> int:
        return sum(
            shard.recover_expired_leases()
            for shard in self.shards
        )

    def mark_complete(
        self,
        claimed: ClaimedDomainDiscovery,
        lease_owner: Optional[str] = None,
    ) -> bool:

        return self.shards[
            claimed.shard_id
        ].mark_complete(
            claimed.hostname,
            lease_owner=lease_owner,
        )

    def mark_failed(
        self,
        claimed: ClaimedDomainDiscovery,
        error: str,
        retry: bool = True,
        lease_owner: Optional[str] = None,
    ) -> bool:

        return self.shards[
            claimed.shard_id
        ].mark_failed(
            claimed.hostname,
            error=error,
            retry=retry,
            lease_owner=lease_owner,
        )

    def stats(self) -> dict[str, Any]:

        shard_stats = [
            shard.stats()
            for shard in self.shards
        ]

        result = {
            "shard_count": self.shard_count,
            "total": 0,
            "queued": 0,
            "processing": 0,
            "complete": 0,
            "failed": 0,
            "shards_with_work": 0,
            "shards": shard_stats,
        }

        for shard in shard_stats:
            result["total"] += shard["total"]
            result["queued"] += shard["queued"]
            result["processing"] += shard["processing"]
            result["complete"] += shard["complete"]
            result["failed"] += shard["failed"]

            if shard["total"] > 0:
                result["shards_with_work"] += 1

        return result
