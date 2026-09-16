from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass(frozen=True)
class DiscoveryWorkItem:
    work_id: str
    logical_partition: int
    hostname: str
    priority: float
    source: str
    available_at: float
    attempts: int
    status: str


@dataclass(frozen=True)
class WorkLease:
    work_id: str
    worker_id: str
    fencing_token: str
    leased_at: float
    lease_until: float


@dataclass(frozen=True)
class WorkQueueCapacity:
    max_inflight: int
    current_inflight: int
    available_capacity: int


class DomainDiscoveryWorkQueue:
    """
    Durable distributed scheduling layer for enormous Web discovery.

    Architectural separation:

        logical partition
              ↓
        durable work item
              ↓
        global scheduling policy
              ↓
        worker acquisition
              ↓
        fenced lease
              ↓
        execution

    This queue deliberately does NOT equate:

        worker count
        physical bucket count
        logical partition count
        domain count
        URL count

    Those dimensions can therefore scale independently.

    SQLite is used only as the durable local queue implementation.
    The API is intentionally designed around distributed semantics so
    the persistence backend can later be partitioned/remote without
    changing worker behavior.
    """

    VERSION = "domain-discovery-work-queue.v1"

    DEFAULT_MAX_INFLIGHT = 100_000
    DEFAULT_LEASE_SECONDS = 300.0
    DEFAULT_MAX_ATTEMPTS = 8

    def __init__(
        self,
        storage_path: str,
        max_inflight: int = DEFAULT_MAX_INFLIGHT,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ):
        if not storage_path:
            raise ValueError(
                "storage_path is required"
            )

        self.max_inflight = max(
            1,
            int(max_inflight),
        )

        self.lease_seconds = float(
            lease_seconds
        )

        self.max_attempts = max(
            1,
            int(max_attempts),
        )

        if self.lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive"
            )

        self.storage_path = storage_path
        self._lock = threading.RLock()

        self._connect().close()

    # ============================================================
    # Persistence
    # ============================================================

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.storage_path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
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

        self._initialize(connection)

        return connection

    @staticmethod
    def _initialize(
        connection: sqlite3.Connection,
    ) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS discovery_work (
                work_id TEXT PRIMARY KEY,
                logical_partition INTEGER NOT NULL,
                hostname TEXT NOT NULL,
                priority REAL NOT NULL,
                source TEXT NOT NULL,
                available_at REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER NOT NULL DEFAULT 0,
                worker_id TEXT,
                fencing_token TEXT,
                leased_at REAL,
                lease_until REAL,
                completed_at REAL,
                failed_at REAL,
                last_error TEXT
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_discovery_work_ready
            ON discovery_work(
                status,
                available_at,
                priority DESC
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_discovery_work_partition
            ON discovery_work(
                logical_partition,
                status
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_discovery_work_worker
            ON discovery_work(
                worker_id,
                status
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_discovery_work_lease
            ON discovery_work(
                status,
                lease_until
            )
            """
        )

    # ============================================================
    # Stable work identity
    # ============================================================

    @staticmethod
    def make_work_id(
        hostname: str,
        source: str,
    ) -> str:
        normalized_hostname = (
            hostname.strip().lower().rstrip(".")
        )

        normalized_source = (
            source.strip().lower()
        )

        digest = hashlib.sha256(
            (
                normalized_hostname
                + "\x00"
                + normalized_source
            ).encode(
                "utf-8"
            )
        ).hexdigest()

        return digest

    # ============================================================
    # Durable enqueue
    # ============================================================

    def enqueue(
        self,
        hostname: str,
        logical_partition: int,
        priority: float,
        source: str,
        available_at: Optional[float] = None,
        work_id: Optional[str] = None,
    ) -> bool:
        hostname = (
            hostname.strip().lower().rstrip(".")
        )

        source = source.strip()

        if not hostname:
            return False

        if not source:
            return False

        if available_at is None:
            available_at = time.time()

        if work_id is None:
            work_id = self.make_work_id(
                hostname,
                source,
            )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO discovery_work (
                        work_id,
                        logical_partition,
                        hostname,
                        priority,
                        source,
                        available_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        work_id,
                        int(logical_partition),
                        hostname,
                        float(priority),
                        source,
                        float(available_at),
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    def enqueue_many(
        self,
        items: Iterable[
            DiscoveryWorkItem
        ],
    ) -> dict[str, int]:
        inserted = 0
        duplicates = 0
        invalid = 0

        with self._lock:
            connection = self._connect()

            try:
                for item in items:
                    if not isinstance(
                        item,
                        DiscoveryWorkItem,
                    ):
                        invalid += 1
                        continue

                    hostname = (
                        item.hostname
                        .strip()
                        .lower()
                        .rstrip(".")
                    )

                    source = item.source.strip()

                    if not hostname or not source:
                        invalid += 1
                        continue

                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO discovery_work (
                            work_id,
                            logical_partition,
                            hostname,
                            priority,
                            source,
                            available_at,
                            status,
                            attempts
                        )
                        VALUES (?, ?, ?, ?, ?, ?, 'queued', ?)
                        """,
                        (
                            item.work_id,
                            int(
                                item.logical_partition
                            ),
                            hostname,
                            float(item.priority),
                            source,
                            float(item.available_at),
                            int(item.attempts),
                        ),
                    )

                    if cursor.rowcount == 1:
                        inserted += 1
                    else:
                        duplicates += 1

            finally:
                connection.close()

        return {
            "inserted": inserted,
            "duplicates": duplicates,
            "invalid": invalid,
        }

    # ============================================================
    # Capacity / backpressure
    # ============================================================

    def capacity(
        self,
    ) -> WorkQueueCapacity:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM discovery_work
                    WHERE status = 'processing'
                    """
                ).fetchone()

                current = int(
                    row["count"]
                )

            finally:
                connection.close()

        return WorkQueueCapacity(
            max_inflight=self.max_inflight,
            current_inflight=current,
            available_capacity=max(
                0,
                self.max_inflight - current,
            ),
        )

    def can_accept_work(
        self,
        requested: int = 1,
    ) -> bool:
        requested = max(
            1,
            int(requested),
        )

        capacity = self.capacity()

        return (
            capacity.available_capacity
            >= requested
        )

    # ============================================================
    # Durable lease acquisition
    # ============================================================

    def claim(
        self,
        worker_id: str,
        limit: int = 100,
    ) -> list[
        tuple[
            DiscoveryWorkItem,
            WorkLease,
        ]
    ]:
        if not worker_id:
            raise ValueError(
                "worker_id is required"
            )

        limit = max(
            1,
            int(limit),
        )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                inflight_row = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM discovery_work
                    WHERE status = 'processing'
                    """
                ).fetchone()

                inflight = int(
                    inflight_row["count"]
                )

                remaining_capacity = max(
                    0,
                    self.max_inflight - inflight,
                )

                limit = min(
                    limit,
                    remaining_capacity,
                )

                if limit <= 0:
                    connection.execute(
                        "COMMIT"
                    )
                    return []

                rows = connection.execute(
                    """
                    SELECT *
                    FROM discovery_work
                    WHERE status = 'queued'
                      AND available_at <= ?
                      AND attempts < ?
                    ORDER BY
                        priority DESC,
                        available_at ASC,
                        logical_partition ASC,
                        work_id ASC
                    LIMIT ?
                    """,
                    (
                        now,
                        self.max_attempts,
                        limit,
                    ),
                ).fetchall()

                result = []

                for row in rows:
                    fencing_token = uuid.uuid4().hex
                    lease_until = (
                        now
                        + self.lease_seconds
                    )

                    updated = connection.execute(
                        """
                        UPDATE discovery_work
                        SET
                            status = 'processing',
                            attempts = attempts + 1,
                            worker_id = ?,
                            fencing_token = ?,
                            leased_at = ?,
                            lease_until = ?
                        WHERE work_id = ?
                          AND status = 'queued'
                        """,
                        (
                            worker_id,
                            fencing_token,
                            now,
                            lease_until,
                            row["work_id"],
                        ),
                    )

                    if updated.rowcount != 1:
                        continue

                    item = DiscoveryWorkItem(
                        work_id=row["work_id"],
                        logical_partition=int(
                            row["logical_partition"]
                        ),
                        hostname=row["hostname"],
                        priority=float(
                            row["priority"]
                        ),
                        source=row["source"],
                        available_at=float(
                            row["available_at"]
                        ),
                        attempts=int(
                            row["attempts"]
                        ) + 1,
                        status="processing",
                    )

                    lease = WorkLease(
                        work_id=row["work_id"],
                        worker_id=worker_id,
                        fencing_token=fencing_token,
                        leased_at=now,
                        lease_until=lease_until,
                    )

                    result.append(
                        (
                            item,
                            lease,
                        )
                    )

                connection.execute(
                    "COMMIT"
                )

                return result

            except Exception:
                try:
                    connection.execute(
                        "ROLLBACK"
                    )
                except Exception:
                    pass

                raise

            finally:
                connection.close()

    # ============================================================
    # Lease renewal
    # ============================================================

    def renew(
        self,
        lease: WorkLease,
    ) -> bool:
        now = time.time()
        lease_until = (
            now + self.lease_seconds
        )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE discovery_work
                    SET
                        lease_until = ?
                    WHERE work_id = ?
                      AND status = 'processing'
                      AND worker_id = ?
                      AND fencing_token = ?
                      AND lease_until > ?
                    """,
                    (
                        lease_until,
                        lease.work_id,
                        lease.worker_id,
                        lease.fencing_token,
                        now,
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Fenced completion
    # ============================================================

    def complete(
        self,
        lease: WorkLease,
    ) -> bool:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE discovery_work
                    SET
                        status = 'complete',
                        completed_at = ?,
                        worker_id = NULL,
                        fencing_token = NULL,
                        leased_at = NULL,
                        lease_until = NULL
                    WHERE work_id = ?
                      AND status = 'processing'
                      AND worker_id = ?
                      AND fencing_token = ?
                    """,
                    (
                        now,
                        lease.work_id,
                        lease.worker_id,
                        lease.fencing_token,
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Fenced failure / retry
    # ============================================================

    def fail(
        self,
        lease: WorkLease,
        error: str,
        retry: bool = True,
        retry_delay: float = 0.0,
    ) -> bool:
        now = time.time()

        if retry:
            available_at = (
                now + max(
                    0.0,
                    float(retry_delay),
                )
            )

            next_status = "queued"
        else:
            available_at = now
            next_status = "failed"

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE discovery_work
                    SET
                        status = ?,
                        available_at = ?,
                        failed_at = ?,
                        last_error = ?,
                        worker_id = NULL,
                        fencing_token = NULL,
                        leased_at = NULL,
                        lease_until = NULL
                    WHERE work_id = ?
                      AND status = 'processing'
                      AND worker_id = ?
                      AND fencing_token = ?
                    """,
                    (
                        next_status,
                        available_at,
                        now,
                        str(error),
                        lease.work_id,
                        lease.worker_id,
                        lease.fencing_token,
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Expired lease recovery
    # ============================================================

    def recover_expired(
        self,
        limit: int = 10_000,
    ) -> int:
        now = time.time()

        limit = max(
            1,
            int(limit),
        )

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                rows = connection.execute(
                    """
                    SELECT work_id, attempts
                    FROM discovery_work
                    WHERE status = 'processing'
                      AND lease_until IS NOT NULL
                      AND lease_until <= ?
                    ORDER BY lease_until ASC
                    LIMIT ?
                    """,
                    (
                        now,
                        limit,
                    ),
                ).fetchall()

                recovered = 0

                for row in rows:
                    attempts = int(
                        row["attempts"]
                    )

                    if attempts >= self.max_attempts:
                        status = "failed"
                    else:
                        status = "queued"

                    cursor = connection.execute(
                        """
                        UPDATE discovery_work
                        SET
                            status = ?,
                            available_at = ?,
                            failed_at = ?,
                            last_error = ?,
                            worker_id = NULL,
                            fencing_token = NULL,
                            leased_at = NULL,
                            lease_until = NULL
                        WHERE work_id = ?
                          AND status = 'processing'
                          AND lease_until <= ?
                        """,
                        (
                            status,
                            now,
                            now,
                            "worker lease expired",
                            row["work_id"],
                            now,
                        ),
                    )

                    recovered += cursor.rowcount

                connection.execute(
                    "COMMIT"
                )

                return recovered

            except Exception:
                try:
                    connection.execute(
                        "ROLLBACK"
                    )
                except Exception:
                    pass

                raise

            finally:
                connection.close()

    # ============================================================
    # Queue inspection
    # ============================================================

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

                return int(
                    row["count"]
                )

            finally:
                connection.close()

    def stats(
        self,
    ) -> dict[str, int]:
        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT
                        status,
                        COUNT(*) AS count
                    FROM discovery_work
                    GROUP BY status
                    """
                ).fetchall()

                result = {
                    "queued": 0,
                    "processing": 0,
                    "complete": 0,
                    "failed": 0,
                }

                for row in rows:
                    status = row["status"]

                    if status in result:
                        result[status] = int(
                            row["count"]
                        )

                result["total"] = sum(
                    result.values()
                )

                return result

            finally:
                connection.close()
