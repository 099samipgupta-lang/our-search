from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional


@dataclass(frozen=True)
class IndexDiscoveryRecord:
    record_id: str
    hostname: str
    url: str
    source: str
    priority: float
    discovered_at: float
    metadata: Optional[dict[str, Any]] = None


@dataclass(frozen=True)
class StoragePartition:
    partition_id: int
    record_count: int
    byte_count: int


@dataclass(frozen=True)
class IndexBatch:
    batch_id: str
    records: tuple[IndexDiscoveryRecord, ...]
    created_at: float


class DistributedStorageIndexIntegration:
    """
    Distributed storage/index integration plane.

    Discovery
        ↓
    immutable discovery records
        ↓
    deterministic partitioning
        ↓
    durable local storage
        ↓
    index handoff
        ↓
    crawler/index infrastructure

    Important properties:

    - deterministic partition ownership
    - append-oriented durable discovery journal
    - idempotent records
    - bounded batches
    - replay after interruption
    - independent storage partitions
    - no global in-memory Web-sized dataset
    - index handoff can be retried safely
    """

    VERSION = "distributed-storage-index-integration.v1"

    DEFAULT_PARTITIONS = 1_048_576

    def __init__(
        self,
        storage_root: str,
        partition_count: int = DEFAULT_PARTITIONS,
        batch_size: int = 10_000,
    ):
        if partition_count < 1:
            raise ValueError(
                "partition_count must be >= 1"
            )

        if batch_size < 1:
            raise ValueError(
                "batch_size must be >= 1"
            )

        self.storage_root = storage_root
        self.partition_count = int(
            partition_count
        )
        self.batch_size = int(batch_size)

        self.database_path = (
            f"{storage_root}/"
            "distributed_index_discovery.db"
        )

        self._lock = threading.RLock()

        self._stats = {
            "records_seen": 0,
            "records_inserted": 0,
            "duplicates": 0,
            "batches_created": 0,
            "batches_handed_off": 0,
            "handoff_failures": 0,
            "replays": 0,
        }

        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=30,
        )

        connection.row_factory = sqlite3.Row

        connection.execute(
            "PRAGMA journal_mode=WAL"
        )
        connection.execute(
            "PRAGMA synchronous=NORMAL"
        )
        connection.execute(
            "PRAGMA busy_timeout=30000"
        )

        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS
                discovery_records (
                    record_id TEXT PRIMARY KEY,
                    partition_id INTEGER NOT NULL,
                    hostname TEXT NOT NULL,
                    url TEXT NOT NULL,
                    source TEXT NOT NULL,
                    priority REAL NOT NULL,
                    discovered_at REAL NOT NULL,
                    metadata_json TEXT,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS
                index_handoff (
                    record_id TEXT PRIMARY KEY,
                    partition_id INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    handed_off_at REAL,
                    last_error TEXT
                );

                CREATE TABLE IF NOT EXISTS
                storage_checkpoints (
                    partition_id INTEGER PRIMARY KEY,
                    last_record_id TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                idx_discovery_partition
                ON discovery_records(partition_id);

                CREATE INDEX IF NOT EXISTS
                idx_discovery_priority
                ON discovery_records(
                    partition_id,
                    priority DESC,
                    discovered_at ASC
                );

                CREATE INDEX IF NOT EXISTS
                idx_handoff_pending
                ON index_handoff(
                    partition_id,
                    state,
                    attempts
                );

                CREATE INDEX IF NOT EXISTS
                idx_handoff_state
                ON index_handoff(state);
                """
            )

    @staticmethod
    def _normalize_hostname(
        hostname: str,
    ) -> str:
        return (
            str(hostname)
            .strip()
            .lower()
            .rstrip(".")
        )

    def partition_for_hostname(
        self,
        hostname: str,
    ) -> int:
        normalized = self._normalize_hostname(
            hostname
        )

        digest = hashlib.sha256(
            normalized.encode("utf-8")
        ).digest()

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return value % self.partition_count

    @staticmethod
    def record_id(
        hostname: str,
        url: str,
        source: str,
    ) -> str:
        payload = (
            f"{hostname.strip().lower()}\x00"
            f"{url.strip()}\x00"
            f"{source.strip()}"
        )

        return hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()

    def _validate(
        self,
        record: IndexDiscoveryRecord,
    ) -> bool:
        if not record.record_id:
            return False

        if not record.hostname:
            return False

        if not record.url:
            return False

        if not record.source:
            return False

        hostname = self._normalize_hostname(
            record.hostname
        )

        if not hostname:
            return False

        if len(hostname) > 253:
            return False

        if "/" in hostname:
            return False

        return True

    def ingest(
        self,
        records: Iterable[IndexDiscoveryRecord],
    ) -> int:
        """
        Idempotently persist discovery records.

        The complete input is never required to exist in memory.
        """

        inserted = 0
        seen = 0
        duplicates = 0

        with self._connect() as connection:
            for record in records:
                seen += 1

                if not self._validate(record):
                    continue

                hostname = self._normalize_hostname(
                    record.hostname
                )

                partition_id = (
                    self.partition_for_hostname(
                        hostname
                    )
                )

                metadata_json = (
                    json.dumps(
                        record.metadata,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    if record.metadata is not None
                    else None
                )

                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO
                    discovery_records (
                        record_id,
                        partition_id,
                        hostname,
                        url,
                        source,
                        priority,
                        discovered_at,
                        metadata_json,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.record_id,
                        partition_id,
                        hostname,
                        record.url.strip(),
                        record.source.strip(),
                        float(record.priority),
                        float(record.discovered_at),
                        metadata_json,
                        time.time(),
                    ),
                )

                if cursor.rowcount:
                    inserted += 1

                    connection.execute(
                        """
                        INSERT OR IGNORE INTO
                        index_handoff (
                            record_id,
                            partition_id,
                            state,
                            attempts,
                            created_at
                        )
                        VALUES (?, ?, 'pending', 0, ?)
                        """,
                        (
                            record.record_id,
                            partition_id,
                            time.time(),
                        ),
                    )
                else:
                    duplicates += 1

            connection.commit()

        with self._lock:
            self._stats["records_seen"] += seen
            self._stats[
                "records_inserted"
            ] += inserted
            self._stats["duplicates"] += duplicates

        return inserted

    def ingest_candidates(
        self,
        candidates: Iterable[Any],
    ) -> int:
        """
        Accepts domain-discovery style objects or dictionaries.
        """

        def convert() -> Iterable[
            IndexDiscoveryRecord
        ]:
            for candidate in candidates:
                if isinstance(candidate, dict):
                    hostname = candidate.get(
                        "hostname"
                    )
                    url = candidate.get("url")
                    source = candidate.get(
                        "source"
                    )

                    if (
                        not hostname
                        or not url
                        or not source
                    ):
                        continue

                    discovered_at = float(
                        candidate.get(
                            "discovered_at",
                            time.time(),
                        )
                    )

                    yield IndexDiscoveryRecord(
                        record_id=self.record_id(
                            str(hostname),
                            str(url),
                            str(source),
                        ),
                        hostname=str(hostname),
                        url=str(url),
                        source=str(source),
                        priority=float(
                            candidate.get(
                                "priority",
                                50.0,
                            )
                        ),
                        discovered_at=discovered_at,
                        metadata=candidate.get(
                            "metadata"
                        ),
                    )

                    continue

                hostname = getattr(
                    candidate,
                    "hostname",
                    None,
                )
                url = getattr(
                    candidate,
                    "url",
                    None,
                )
                source = getattr(
                    candidate,
                    "source",
                    None,
                )

                if (
                    not hostname
                    or not url
                    or not source
                ):
                    continue

                yield IndexDiscoveryRecord(
                    record_id=self.record_id(
                        str(hostname),
                        str(url),
                        str(source),
                    ),
                    hostname=str(hostname),
                    url=str(url),
                    source=str(source),
                    priority=float(
                        getattr(
                            candidate,
                            "priority",
                            50.0,
                        )
                    ),
                    discovered_at=float(
                        getattr(
                            candidate,
                            "discovered_at",
                            time.time(),
                        )
                        or time.time()
                    ),
                    metadata=getattr(
                        candidate,
                        "metadata",
                        None,
                    ),
                )

        return self.ingest(convert())

    def create_handoff_batch(
        self,
        partition_id: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> IndexBatch:
        """
        Atomically select pending records for index handoff.

        Records remain pending until explicitly acknowledged.
        """

        if limit is None:
            limit = self.batch_size

        if limit < 1:
            return IndexBatch(
                batch_id="",
                records=(),
                created_at=time.time(),
            )

        now = time.time()

        with self._connect() as connection:
            if partition_id is None:
                rows = connection.execute(
                    """
                    SELECT
                        d.record_id,
                        d.hostname,
                        d.url,
                        d.source,
                        d.priority,
                        d.discovered_at,
                        d.metadata_json
                    FROM discovery_records d
                    JOIN index_handoff h
                      ON h.record_id = d.record_id
                    WHERE h.state = 'pending'
                    ORDER BY
                        d.priority DESC,
                        d.discovered_at ASC,
                        d.record_id ASC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()

            else:
                rows = connection.execute(
                    """
                    SELECT
                        d.record_id,
                        d.hostname,
                        d.url,
                        d.source,
                        d.priority,
                        d.discovered_at,
                        d.metadata_json
                    FROM discovery_records d
                    JOIN index_handoff h
                      ON h.record_id = d.record_id
                    WHERE h.state = 'pending'
                      AND d.partition_id = ?
                    ORDER BY
                        d.priority DESC,
                        d.discovered_at ASC,
                        d.record_id ASC
                    LIMIT ?
                    """,
                    (
                        int(partition_id),
                        int(limit),
                    ),
                ).fetchall()

            records = []

            for row in rows:
                metadata = None

                if row["metadata_json"]:
                    try:
                        metadata = json.loads(
                            row["metadata_json"]
                        )
                    except Exception:
                        metadata = None

                records.append(
                    IndexDiscoveryRecord(
                        record_id=row[
                            "record_id"
                        ],
                        hostname=row[
                            "hostname"
                        ],
                        url=row["url"],
                        source=row[
                            "source"
                        ],
                        priority=float(
                            row["priority"]
                        ),
                        discovered_at=float(
                            row["discovered_at"]
                        ),
                        metadata=metadata,
                    )
                )

            batch_id = self._batch_id(
                records,
                now,
            )

        if records:
            with self._lock:
                self._stats[
                    "batches_created"
                ] += 1

        return IndexBatch(
            batch_id=batch_id,
            records=tuple(records),
            created_at=now,
        )

    @staticmethod
    def _batch_id(
        records: list[IndexDiscoveryRecord],
        created_at: float,
    ) -> str:
        payload = (
            f"{created_at}\n"
            + "\n".join(
                record.record_id
                for record in records
            )
        )

        return hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()

    def acknowledge_handoff(
        self,
        records: Iterable[IndexDiscoveryRecord],
    ) -> int:
        """
        Mark records as durably handed off.

        This operation is idempotent.
        """

        count = 0
        now = time.time()

        with self._connect() as connection:
            for record in records:
                cursor = connection.execute(
                    """
                    UPDATE index_handoff
                    SET state = 'complete',
                        attempts = attempts + 1,
                        handed_off_at = ?,
                        last_error = NULL
                    WHERE record_id = ?
                      AND state != 'complete'
                    """,
                    (
                        now,
                        record.record_id,
                    ),
                )

                count += cursor.rowcount

            connection.commit()

        if count:
            with self._lock:
                self._stats[
                    "batches_handed_off"
                ] += count

        return count

    def fail_handoff(
        self,
        records: Iterable[IndexDiscoveryRecord],
        error: str,
    ) -> int:
        """
        Return failed index handoffs to pending state.

        The durable attempt counter preserves retry history.
        """

        count = 0

        with self._connect() as connection:
            for record in records:
                cursor = connection.execute(
                    """
                    UPDATE index_handoff
                    SET state = 'pending',
                        attempts = attempts + 1,
                        last_error = ?
                    WHERE record_id = ?
                      AND state != 'complete'
                    """,
                    (
                        str(error),
                        record.record_id,
                    ),
                )

                count += cursor.rowcount

            connection.commit()

        if count:
            with self._lock:
                self._stats[
                    "handoff_failures"
                ] += count

        return count

    def replay_pending(
        self,
        partition_id: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> IndexBatch:
        """
        Replay unacknowledged index work after interruption.
        """

        with self._lock:
            self._stats["replays"] += 1

        return self.create_handoff_batch(
            partition_id=partition_id,
            limit=limit,
        )

    def checkpoint_partition(
        self,
        partition_id: int,
        last_record_id: Optional[str],
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO storage_checkpoints (
                    partition_id,
                    last_record_id,
                    updated_at
                )
                VALUES (?, ?, ?)
                ON CONFLICT(partition_id)
                DO UPDATE SET
                    last_record_id = excluded.last_record_id,
                    updated_at = excluded.updated_at
                """,
                (
                    int(partition_id),
                    last_record_id,
                    time.time(),
                ),
            )

    def partition_stats(
        self,
        partition_id: int,
    ) -> StoragePartition:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS record_count,
                    COALESCE(
                        SUM(
                            LENGTH(url)
                            + LENGTH(hostname)
                            + LENGTH(source)
                        ),
                        0
                    ) AS byte_count
                FROM discovery_records
                WHERE partition_id = ?
                """,
                (int(partition_id),),
            ).fetchone()

        return StoragePartition(
            partition_id=int(partition_id),
            record_count=int(
                row["record_count"]
            ),
            byte_count=int(
                row["byte_count"]
            ),
        )

    def pending_count(
        self,
        partition_id: Optional[int] = None,
    ) -> int:
        with self._connect() as connection:
            if partition_id is None:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM index_handoff
                    WHERE state = 'pending'
                    """
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM index_handoff
                    WHERE state = 'pending'
                      AND partition_id = ?
                    """,
                    (int(partition_id),),
                ).fetchone()

        return int(row[0])

    def stats(self) -> dict[str, Any]:
        with self._lock:
            result = dict(self._stats)

        with self._connect() as connection:
            total = connection.execute(
                """
                SELECT COUNT(*)
                FROM discovery_records
                """
            ).fetchone()[0]

            pending = connection.execute(
                """
                SELECT COUNT(*)
                FROM index_handoff
                WHERE state = 'pending'
                """
            ).fetchone()[0]

            complete = connection.execute(
                """
                SELECT COUNT(*)
                FROM index_handoff
                WHERE state = 'complete'
                """
            ).fetchone()[0]

            partitions = connection.execute(
                """
                SELECT COUNT(DISTINCT partition_id)
                FROM discovery_records
                """
            ).fetchone()[0]

        result.update(
            {
                "version": self.VERSION,
                "partition_count": self.partition_count,
                "batch_size": self.batch_size,
                "durable_records": int(total),
                "pending_handoffs": int(pending),
                "completed_handoffs": int(complete),
                "active_partitions": int(
                    partitions
                ),
            }
        )

        return result
