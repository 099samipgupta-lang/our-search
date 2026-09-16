import hashlib
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import urlsplit


@dataclass(frozen=True)
class DiscoveryRecord:
    key: str
    url: str
    hostname: str
    source: str
    source_url: Optional[str] = None
    priority: float = 50.0
    discovered_at: Optional[float] = None
    metadata: Optional[dict] = None


@dataclass(frozen=True)
class ClaimedDiscovery:
    key: str
    url: str
    hostname: str
    source: str
    source_url: Optional[str]
    priority: float
    discovered_at: float
    metadata: dict
    shard_id: int


class DiscoveryPartitioner:
    """
    Deterministically maps discovery work to shards.

    The hash is intentionally stable across processes and machines.
    Python's built-in hash() is NOT used because its result is
    process-randomized.

    Hostnames are the preferred partition key for domain discovery.
    URLs are used when no hostname is available.
    """

    def __init__(self, shard_count: int):
        shard_count = int(shard_count)

        if shard_count <= 0:
            raise ValueError("shard_count must be greater than zero")

        self.shard_count = shard_count

    @staticmethod
    def _digest(value: str) -> int:
        digest = hashlib.sha256(
            value.encode("utf-8", "strict")
        ).digest()

        return int.from_bytes(digest[:8], "big", signed=False)

    def partition(self, key: str) -> int:
        if not isinstance(key, str) or not key:
            raise ValueError("partition key must be a non-empty string")

        return self._digest(key) % self.shard_count

    def partition_hostname(self, hostname: str) -> int:
        return self.partition(hostname.strip().lower().rstrip("."))

    def partition_url(self, url: str) -> int:
        parsed = urlsplit(url)

        hostname = parsed.hostname

        if hostname:
            return self.partition_hostname(hostname)

        return self.partition(url)


class DiscoveryShard:
    """
    Durable shard-local discovery queue.

    Each shard owns an independent SQLite database.

    There is deliberately no process-global lock across shards.
    A lock protects one shard instance only.

    This allows multiple workers/processes/machines to operate on
    different shards independently.
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
                    CREATE TABLE IF NOT EXISTS discovery_work (
                        key TEXT PRIMARY KEY,
                        url TEXT NOT NULL,
                        hostname TEXT NOT NULL,
                        source TEXT NOT NULL,
                        source_url TEXT,
                        priority REAL NOT NULL DEFAULT 50.0,
                        discovered_at REAL NOT NULL,
                        metadata_json TEXT NOT NULL DEFAULT '{}',
                        status TEXT NOT NULL DEFAULT 'queued',
                        lease_owner TEXT,
                        leased_at REAL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        completed_at REAL,
                        failed_at REAL,
                        last_error TEXT
                    );

                    CREATE INDEX IF NOT EXISTS
                        idx_discovery_work_ready
                    ON discovery_work (
                        status,
                        priority DESC,
                        discovered_at ASC
                    );

                    CREATE INDEX IF NOT EXISTS
                        idx_discovery_work_lease
                    ON discovery_work (
                        status,
                        leased_at
                    );

                    CREATE INDEX IF NOT EXISTS
                        idx_discovery_work_hostname
                    ON discovery_work (hostname);

                    CREATE INDEX IF NOT EXISTS
                        idx_discovery_work_source
                    ON discovery_work (source);
                    """
                )

                connection.commit()

            finally:
                connection.close()

    @staticmethod
    def _normalize_priority(priority) -> float:
        try:
            value = float(priority)
        except (TypeError, ValueError):
            value = 50.0

        return max(0.0, min(100.0, value))

    @staticmethod
    def _metadata_json(metadata) -> str:
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
    def _metadata_from_json(value):
        try:
            result = json.loads(value or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}

        return result if isinstance(result, dict) else {}

    def enqueue_many(
        self,
        records: Iterable[DiscoveryRecord],
    ) -> dict:
        now = time.time()

        inserted = 0
        duplicates = 0
        invalid = 0

        with self._lock:
            connection = self._connect()

            try:
                connection.execute("BEGIN IMMEDIATE")

                for record in records:
                    if not isinstance(record, DiscoveryRecord):
                        invalid += 1
                        continue

                    if not record.key:
                        invalid += 1
                        continue

                    if not isinstance(record.url, str) or not record.url:
                        invalid += 1
                        continue

                    if not isinstance(record.hostname, str):
                        invalid += 1
                        continue

                    if not isinstance(record.source, str) or not record.source:
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
                        INSERT OR IGNORE INTO discovery_work (
                            key,
                            url,
                            hostname,
                            source,
                            source_url,
                            priority,
                            discovered_at,
                            metadata_json,
                            status
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued')
                        """,
                        (
                            record.key,
                            record.url,
                            record.hostname,
                            record.source,
                            record.source_url,
                            priority,
                            discovered_at,
                            self._metadata_json(record.metadata),
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
    ) -> list[ClaimedDiscovery]:
        limit = max(1, int(limit))

        if not isinstance(lease_owner, str) or not lease_owner:
            raise ValueError("lease_owner must be non-empty")

        now = time.time() if now is None else float(now)

        claimed = []

        with self._lock:
            connection = self._connect()

            try:
                connection.execute("BEGIN IMMEDIATE")

                rows = connection.execute(
                    """
                    SELECT
                        key,
                        url,
                        hostname,
                        source,
                        source_url,
                        priority,
                        discovered_at,
                        metadata_json
                    FROM discovery_work
                    WHERE status = 'queued'
                    ORDER BY
                        priority DESC,
                        discovered_at ASC,
                        key ASC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()

                for row in rows:
                    cursor = connection.execute(
                        """
                        UPDATE discovery_work
                        SET
                            status = 'processing',
                            lease_owner = ?,
                            leased_at = ?,
                            attempts = attempts + 1
                        WHERE
                            key = ?
                            AND status = 'queued'
                        """,
                        (
                            lease_owner,
                            now,
                            row["key"],
                        ),
                    )

                    if cursor.rowcount != 1:
                        continue

                    claimed.append(
                        ClaimedDiscovery(
                            key=row["key"],
                            url=row["url"],
                            hostname=row["hostname"],
                            source=row["source"],
                            source_url=row["source_url"],
                            priority=float(row["priority"]),
                            discovered_at=float(row["discovered_at"]),
                            metadata=self._metadata_from_json(
                                row["metadata_json"]
                            ),
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
        key: str,
        lease_owner: Optional[str] = None,
        now: Optional[float] = None,
    ) -> bool:
        now = time.time() if now is None else float(now)

        with self._lock:
            connection = self._connect()

            try:
                if lease_owner is None:
                    cursor = connection.execute(
                        """
                        UPDATE discovery_work
                        SET
                            status = 'complete',
                            completed_at = ?,
                            lease_owner = NULL,
                            leased_at = NULL
                        WHERE
                            key = ?
                            AND status = 'processing'
                        """,
                        (now, key),
                    )
                else:
                    cursor = connection.execute(
                        """
                        UPDATE discovery_work
                        SET
                            status = 'complete',
                            completed_at = ?,
                            lease_owner = NULL,
                            leased_at = NULL
                        WHERE
                            key = ?
                            AND status = 'processing'
                            AND lease_owner = ?
                        """,
                        (now, key, lease_owner),
                    )

                connection.commit()

                return cursor.rowcount == 1

            finally:
                connection.close()

    def mark_failed(
        self,
        key: str,
        error: str,
        retry: bool = True,
        lease_owner: Optional[str] = None,
        now: Optional[float] = None,
    ) -> bool:
        now = time.time() if now is None else float(now)

        status = "queued" if retry else "failed"

        with self._lock:
            connection = self._connect()

            try:
                if lease_owner is None:
                    cursor = connection.execute(
                        """
                        UPDATE discovery_work
                        SET
                            status = ?,
                            failed_at = ?,
                            lease_owner = NULL,
                            leased_at = NULL,
                            last_error = ?
                        WHERE
                            key = ?
                            AND status = 'processing'
                        """,
                        (
                            status,
                            now,
                            str(error),
                            key,
                        ),
                    )
                else:
                    cursor = connection.execute(
                        """
                        UPDATE discovery_work
                        SET
                            status = ?,
                            failed_at = ?,
                            lease_owner = NULL,
                            leased_at = NULL,
                            last_error = ?
                        WHERE
                            key = ?
                            AND status = 'processing'
                            AND lease_owner = ?
                        """,
                        (
                            status,
                            now,
                            str(error),
                            key,
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
        now = time.time() if now is None else float(now)

        cutoff = now - self.lease_timeout

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE discovery_work
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

    def count(self, status: Optional[str] = None) -> int:
        with self._lock:
            connection = self._connect()

            try:
                if status is None:
                    row = connection.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM discovery_work
                        """
                    ).fetchone()
                else:
                    row = connection.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM discovery_work
                        WHERE status = ?
                        """,
                        (status,),
                    ).fetchone()

                return int(row["count"])

            finally:
                connection.close()

    def stats(self) -> dict:
        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM discovery_work
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

                    result[status] = count
                    result["total"] += count

                return result

            finally:
                connection.close()


class DiscoveryFabric:
    """
    Horizontally scalable discovery fabric.

    The fabric is intentionally composed of independent durable
    shards rather than one global queue.

    Scaling model:

        shard_count = 1
            one durable shard

        shard_count = 32
            32 independently addressable durable shards

        shard_count = 256
            256 independently addressable durable shards

        shard_count = 4096
            4096 independently addressable durable shards

    The same partitioning function is used regardless of scale.

    Increasing the shard count creates more parallel ownership
    capacity without changing the discovery record model.
    """

    def __init__(
        self,
        storage_root: str,
        shard_count: int = 64,
        lease_timeout: float = 300.0,
    ):
        self.storage_root = str(storage_root)
        self.shard_count = int(shard_count)
        self.lease_timeout = float(lease_timeout)

        if self.shard_count <= 0:
            raise ValueError("shard_count must be greater than zero")

        self.partitioner = DiscoveryPartitioner(
            self.shard_count
        )

        os.makedirs(self.storage_root, exist_ok=True)

        self._shards = tuple(
            DiscoveryShard(
                shard_id=shard_id,
                database_path=os.path.join(
                    self.storage_root,
                    f"shard_{shard_id:05d}.db",
                ),
                lease_timeout=self.lease_timeout,
            )
            for shard_id in range(self.shard_count)
        )

    def shard_for_hostname(self, hostname: str) -> int:
        return self.partitioner.partition_hostname(hostname)

    def shard_for_url(self, url: str) -> int:
        return self.partitioner.partition_url(url)

    @staticmethod
    def make_url_key(url: str) -> str:
        return hashlib.sha256(
            url.encode("utf-8", "strict")
        ).hexdigest()

    @staticmethod
    def make_domain_key(hostname: str) -> str:
        hostname = hostname.strip().lower().rstrip(".")

        return hashlib.sha256(
            f"domain:{hostname}".encode(
                "utf-8",
                "strict",
            )
        ).hexdigest()

    def enqueue(
        self,
        url: str,
        hostname: Optional[str] = None,
        source: str = "discovery",
        source_url: Optional[str] = None,
        priority: float = 50.0,
        discovered_at: Optional[float] = None,
        metadata: Optional[dict] = None,
        key: Optional[str] = None,
    ) -> dict:
        if not isinstance(url, str) or not url:
            raise ValueError("url must be non-empty")

        if hostname is None:
            hostname = urlsplit(url).hostname

        if not isinstance(hostname, str) or not hostname:
            raise ValueError(
                "hostname is required or must be derivable from url"
            )

        hostname = hostname.strip().lower().rstrip(".")

        shard_id = self.shard_for_hostname(hostname)

        record = DiscoveryRecord(
            key=key or self.make_url_key(url),
            url=url,
            hostname=hostname,
            source=source,
            source_url=source_url,
            priority=priority,
            discovered_at=discovered_at,
            metadata=metadata,
        )

        result = self._shards[shard_id].enqueue_many(
            [record]
        )

        result["shard_id"] = shard_id

        return result

    def enqueue_many(
        self,
        records: Iterable[DiscoveryRecord],
    ) -> dict:
        buckets = {
            shard_id: []
            for shard_id in range(self.shard_count)
        }

        invalid = 0

        for record in records:
            if not isinstance(record, DiscoveryRecord):
                invalid += 1
                continue

            hostname = (
                record.hostname.strip()
                .lower()
                .rstrip(".")
            )

            if not hostname:
                invalid += 1
                continue

            shard_id = self.shard_for_hostname(hostname)

            buckets[shard_id].append(record)

        inserted = 0
        duplicates = 0

        # Each shard is independent. There is intentionally no
        # transaction spanning all shards.
        for shard_id, shard_records in buckets.items():
            if not shard_records:
                continue

            result = self._shards[shard_id].enqueue_many(
                shard_records
            )

            inserted += result["inserted"]
            duplicates += result["duplicates"]
            invalid += result["invalid"]

        return {
            "inserted": inserted,
            "duplicates": duplicates,
            "invalid": invalid,
        }

    def claim_many(
        self,
        shard_id: int,
        limit: int,
        lease_owner: str,
    ) -> list[ClaimedDiscovery]:
        shard_id = int(shard_id)

        if not 0 <= shard_id < self.shard_count:
            raise ValueError("invalid shard_id")

        return self._shards[shard_id].claim_many(
            limit=limit,
            lease_owner=lease_owner,
        )

    def recover_expired_leases(self) -> int:
        recovered = 0

        for shard in self._shards:
            recovered += shard.recover_expired_leases()

        return recovered

    def mark_complete(
        self,
        shard_id: int,
        key: str,
        lease_owner: Optional[str] = None,
    ) -> bool:
        return self._shards[int(shard_id)].mark_complete(
            key=key,
            lease_owner=lease_owner,
        )

    def mark_failed(
        self,
        shard_id: int,
        key: str,
        error: str,
        retry: bool = True,
        lease_owner: Optional[str] = None,
    ) -> bool:
        return self._shards[int(shard_id)].mark_failed(
            key=key,
            error=error,
            retry=retry,
            lease_owner=lease_owner,
        )

    def shard_stats(self) -> list[dict]:
        return [
            shard.stats()
            for shard in self._shards
        ]

    def global_stats(self) -> dict:
        result = {
            "shard_count": self.shard_count,
            "total": 0,
            "queued": 0,
            "processing": 0,
            "complete": 0,
            "failed": 0,
        }

        for stats in self.shard_stats():
            result["total"] += stats["total"]
            result["queued"] += stats["queued"]
            result["processing"] += stats["processing"]
            result["complete"] += stats["complete"]
            result["failed"] += stats["failed"]

        return result

    def distribution(self) -> list[int]:
        return [
            shard.count()
            for shard in self._shards
        ]
