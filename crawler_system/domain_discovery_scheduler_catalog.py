from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass(frozen=True)
class SchedulerWorker:
    worker_id: str
    generation: int
    status: str
    registered_at: float
    last_heartbeat: float
    fencing_epoch: int


@dataclass(frozen=True)
class LogicalWorkState:
    logical_partition: int
    priority: float
    source: str
    queued_work: int
    processing_work: int
    last_scheduled_at: float
    next_schedule_at: float
    scheduling_epoch: int


@dataclass(frozen=True)
class SchedulerLease:
    lease_id: str
    worker_id: str
    logical_partition: int
    fencing_epoch: int
    acquired_at: float
    lease_until: float


@dataclass(frozen=True)
class SchedulerCapacity:
    active_workers: int
    max_workers: int
    active_leases: int
    max_leases: int
    available_worker_capacity: int
    available_lease_capacity: int


class DomainDiscoverySchedulerCatalog:
    """
    Durable global discovery scheduler/state catalog.

    This is the persistent control-plane abstraction above the durable
    work queue and placement fabric.

    It tracks:

        workers
        worker generations
        fencing epochs
        logical scheduling state
        scheduler leases
        source scheduling state
        recovery checkpoints
        backpressure state

    The scheduler operates on logical work units rather than
    enumerating every domain, URL, physical bucket, or worker.

    The implementation uses SQLite as the current durable backend,
    but the schema/API is intentionally partition-friendly: the
    control-plane state is organized around independently addressable
    worker, logical-partition, source, and checkpoint records.
    """

    VERSION = "domain-discovery-scheduler-catalog.v1"

    DEFAULT_MAX_WORKERS = 100_000
    DEFAULT_MAX_LEASES = 100_000

    def __init__(
        self,
        storage_path: str,
        max_workers: int = DEFAULT_MAX_WORKERS,
        max_leases: int = DEFAULT_MAX_LEASES,
        heartbeat_timeout: float = 90.0,
        scheduler_epoch: Optional[str] = None,
    ):
        if not storage_path:
            raise ValueError(
                "storage_path is required"
            )

        self.storage_path = storage_path

        self.max_workers = max(
            1,
            int(max_workers),
        )

        self.max_leases = max(
            1,
            int(max_leases),
        )

        self.heartbeat_timeout = float(
            heartbeat_timeout
        )

        if self.heartbeat_timeout <= 0:
            raise ValueError(
                "heartbeat_timeout must be positive"
            )

        self.scheduler_epoch = (
            scheduler_epoch
            or uuid.uuid4().hex
        )

        self._lock = threading.RLock()

        connection = self._connect()

        try:
            self._initialize_metadata(
                connection
            )
        finally:
            connection.close()

    # ============================================================
    # Database
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
            CREATE TABLE IF NOT EXISTS scheduler_workers (
                worker_id TEXT PRIMARY KEY,
                generation INTEGER NOT NULL,
                status TEXT NOT NULL,
                registered_at REAL NOT NULL,
                last_heartbeat REAL NOT NULL,
                fencing_epoch INTEGER NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_scheduler_workers_status
            ON scheduler_workers(
                status,
                last_heartbeat
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS logical_work_state (
                logical_partition INTEGER NOT NULL,
                source TEXT NOT NULL,
                priority REAL NOT NULL DEFAULT 50.0,
                queued_work INTEGER NOT NULL DEFAULT 0,
                processing_work INTEGER NOT NULL DEFAULT 0,
                last_scheduled_at REAL NOT NULL DEFAULT 0,
                next_schedule_at REAL NOT NULL DEFAULT 0,
                scheduling_epoch INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (
                    logical_partition,
                    source
                )
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_logical_work_ready
            ON logical_work_state(
                next_schedule_at,
                priority DESC,
                last_scheduled_at
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduler_leases (
                lease_id TEXT PRIMARY KEY,
                worker_id TEXT NOT NULL,
                logical_partition INTEGER NOT NULL,
                fencing_epoch INTEGER NOT NULL,
                acquired_at REAL NOT NULL,
                lease_until REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'active'
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_scheduler_leases_worker
            ON scheduler_leases(
                worker_id,
                status
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_scheduler_leases_expiry
            ON scheduler_leases(
                status,
                lease_until
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS source_schedule_state (
                source TEXT PRIMARY KEY,
                priority REAL NOT NULL DEFAULT 50.0,
                last_scheduled_at REAL NOT NULL DEFAULT 0,
                next_schedule_at REAL NOT NULL DEFAULT 0,
                scheduling_epoch INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduler_checkpoints (
                checkpoint_name TEXT PRIMARY KEY,
                checkpoint_value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduler_metadata (
                metadata_key TEXT PRIMARY KEY,
                metadata_value TEXT NOT NULL
            )
            """
        )

    def _initialize_metadata(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        now = time.time()

        connection.execute(
            """
            INSERT OR IGNORE INTO scheduler_metadata (
                metadata_key,
                metadata_value
            )
            VALUES ('scheduler_epoch', ?)
            """,
            (
                self.scheduler_epoch,
            ),
        )

        connection.execute(
            """
            INSERT OR IGNORE INTO scheduler_metadata (
                metadata_key,
                metadata_value
            )
            VALUES ('created_at', ?)
            """,
            (
                str(now),
            ),
        )

    # ============================================================
    # Stable scheduling hash
    # ============================================================

    @staticmethod
    def scheduling_hash(
        logical_partition: int,
        source: str,
    ) -> int:
        payload = (
            f"{int(logical_partition)}"
            "\x00"
            f"{source.strip().lower()}"
        ).encode("utf-8")

        digest = hashlib.sha256(
            payload
        ).digest()

        return int.from_bytes(
            digest[:8],
            "big",
        )

    # ============================================================
    # Worker registration
    # ============================================================

    def register_worker(
        self,
        worker_id: str,
    ) -> SchedulerWorker:
        if not worker_id:
            raise ValueError(
                "worker_id is required"
            )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                active_row = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM scheduler_workers
                    WHERE status = 'active'
                    """
                ).fetchone()

                active_count = int(
                    active_row["count"]
                )

                existing = connection.execute(
                    """
                    SELECT *
                    FROM scheduler_workers
                    WHERE worker_id = ?
                    """,
                    (worker_id,),
                ).fetchone()

                if existing is None:
                    if (
                        active_count
                        >= self.max_workers
                    ):
                        raise RuntimeError(
                            "maximum active worker capacity reached"
                        )

                    generation = 1
                    fencing_epoch = 1

                    connection.execute(
                        """
                        INSERT INTO scheduler_workers (
                            worker_id,
                            generation,
                            status,
                            registered_at,
                            last_heartbeat,
                            fencing_epoch
                        )
                        VALUES (?, ?, 'active', ?, ?, ?)
                        """,
                        (
                            worker_id,
                            generation,
                            now,
                            now,
                            fencing_epoch,
                        ),
                    )

                else:
                    generation = (
                        int(
                            existing["generation"]
                        )
                        + 1
                    )

                    fencing_epoch = (
                        int(
                            existing[
                                "fencing_epoch"
                            ]
                        )
                        + 1
                    )

                    connection.execute(
                        """
                        UPDATE scheduler_workers
                        SET
                            generation = ?,
                            status = 'active',
                            last_heartbeat = ?,
                            fencing_epoch = ?
                        WHERE worker_id = ?
                        """,
                        (
                            generation,
                            now,
                            fencing_epoch,
                            worker_id,
                        ),
                    )

                connection.execute(
                    "COMMIT"
                )

                row = connection.execute(
                    """
                    SELECT *
                    FROM scheduler_workers
                    WHERE worker_id = ?
                    """,
                    (worker_id,),
                ).fetchone()

                return SchedulerWorker(
                    worker_id=row["worker_id"],
                    generation=int(
                        row["generation"]
                    ),
                    status=row["status"],
                    registered_at=float(
                        row["registered_at"]
                    ),
                    last_heartbeat=float(
                        row["last_heartbeat"]
                    ),
                    fencing_epoch=int(
                        row["fencing_epoch"]
                    ),
                )

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
    # Worker heartbeat
    # ============================================================

    def heartbeat(
        self,
        worker_id: str,
        generation: int,
        fencing_epoch: int,
    ) -> bool:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE scheduler_workers
                    SET
                        last_heartbeat = ?
                    WHERE worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND status = 'active'
                    """,
                    (
                        now,
                        worker_id,
                        int(generation),
                        int(fencing_epoch),
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Worker fencing
    # ============================================================

    def fence_worker(
        self,
        worker_id: str,
        generation: Optional[int] = None,
    ) -> bool:
        with self._lock:
            connection = self._connect()

            try:
                if generation is None:
                    cursor = connection.execute(
                        """
                        UPDATE scheduler_workers
                        SET
                            status = 'fenced',
                            fencing_epoch =
                                fencing_epoch + 1
                        WHERE worker_id = ?
                          AND status = 'active'
                        """,
                        (worker_id,),
                    )
                else:
                    cursor = connection.execute(
                        """
                        UPDATE scheduler_workers
                        SET
                            status = 'fenced',
                            fencing_epoch =
                                fencing_epoch + 1
                        WHERE worker_id = ?
                          AND generation = ?
                          AND status = 'active'
                        """,
                        (
                            worker_id,
                            int(generation),
                        ),
                    )

                return cursor.rowcount == 1

            finally:
                connection.close()

    def fence_expired_workers(
        self,
    ) -> list[str]:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT worker_id
                    FROM scheduler_workers
                    WHERE status = 'active'
                      AND last_heartbeat < ?
                    """,
                    (
                        now
                        - self.heartbeat_timeout,
                    ),
                ).fetchall()

                worker_ids = [
                    row["worker_id"]
                    for row in rows
                ]

                for worker_id in worker_ids:
                    connection.execute(
                        """
                        UPDATE scheduler_workers
                        SET
                            status = 'fenced',
                            fencing_epoch =
                                fencing_epoch + 1
                        WHERE worker_id = ?
                          AND status = 'active'
                        """,
                        (worker_id,),
                    )

                return worker_ids

            finally:
                connection.close()

    # ============================================================
    # Worker lookup
    # ============================================================

    def get_worker(
        self,
        worker_id: str,
    ) -> Optional[SchedulerWorker]:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM scheduler_workers
                    WHERE worker_id = ?
                    """,
                    (worker_id,),
                ).fetchone()

                if row is None:
                    return None

                return SchedulerWorker(
                    worker_id=row["worker_id"],
                    generation=int(
                        row["generation"]
                    ),
                    status=row["status"],
                    registered_at=float(
                        row["registered_at"]
                    ),
                    last_heartbeat=float(
                        row["last_heartbeat"]
                    ),
                    fencing_epoch=int(
                        row["fencing_epoch"]
                    ),
                )

            finally:
                connection.close()

    # ============================================================
    # Logical scheduling state
    # ============================================================

    def ensure_logical_work(
        self,
        logical_partition: int,
        source: str,
        priority: float = 50.0,
        queued_work: int = 0,
        processing_work: int = 0,
    ) -> LogicalWorkState:
        source = source.strip()

        if not source:
            raise ValueError(
                "source is required"
            )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    """
                    INSERT INTO logical_work_state (
                        logical_partition,
                        source,
                        priority,
                        queued_work,
                        processing_work,
                        last_scheduled_at,
                        next_schedule_at,
                        scheduling_epoch
                    )
                    VALUES (?, ?, ?, ?, ?, 0, ?, 0)
                    ON CONFLICT (
                        logical_partition,
                        source
                    )
                    DO UPDATE SET
                        priority = excluded.priority
                    """,
                    (
                        int(logical_partition),
                        source,
                        float(priority),
                        max(
                            0,
                            int(queued_work),
                        ),
                        max(
                            0,
                            int(processing_work),
                        ),
                        now,
                    ),
                )

                row = connection.execute(
                    """
                    SELECT *
                    FROM logical_work_state
                    WHERE logical_partition = ?
                      AND source = ?
                    """,
                    (
                        int(logical_partition),
                        source,
                    ),
                ).fetchone()

                return self._logical_state_from_row(
                    row
                )

            finally:
                connection.close()

    @staticmethod
    def _logical_state_from_row(
        row: sqlite3.Row,
    ) -> LogicalWorkState:
        return LogicalWorkState(
            logical_partition=int(
                row["logical_partition"]
            ),
            priority=float(
                row["priority"]
            ),
            source=row["source"],
            queued_work=int(
                row["queued_work"]
            ),
            processing_work=int(
                row["processing_work"]
            ),
            last_scheduled_at=float(
                row["last_scheduled_at"]
            ),
            next_schedule_at=float(
                row["next_schedule_at"]
            ),
            scheduling_epoch=int(
                row["scheduling_epoch"]
            ),
        )

    def update_logical_work(
        self,
        logical_partition: int,
        source: str,
        queued_delta: int = 0,
        processing_delta: int = 0,
        priority: Optional[float] = None,
        next_schedule_at: Optional[float] = None,
    ) -> bool:
        source = source.strip()

        with self._lock:
            connection = self._connect()

            try:
                current = connection.execute(
                    """
                    SELECT *
                    FROM logical_work_state
                    WHERE logical_partition = ?
                      AND source = ?
                    """,
                    (
                        int(logical_partition),
                        source,
                    ),
                ).fetchone()

                if current is None:
                    self.ensure_logical_work(
                        logical_partition,
                        source,
                        priority=(
                            50.0
                            if priority is None
                            else priority
                        ),
                    )

                    current = connection.execute(
                        """
                        SELECT *
                        FROM logical_work_state
                        WHERE logical_partition = ?
                          AND source = ?
                        """,
                        (
                            int(logical_partition),
                            source,
                        ),
                    ).fetchone()

                queued = max(
                    0,
                    int(
                        current["queued_work"]
                    )
                    + int(queued_delta),
                )

                processing = max(
                    0,
                    int(
                        current[
                            "processing_work"
                        ]
                    )
                    + int(processing_delta),
                )

                if priority is None:
                    priority_value = float(
                        current["priority"]
                    )
                else:
                    priority_value = float(
                        priority
                    )

                if next_schedule_at is None:
                    next_value = float(
                        current[
                            "next_schedule_at"
                        ]
                    )
                else:
                    next_value = float(
                        next_schedule_at
                    )

                cursor = connection.execute(
                    """
                    UPDATE logical_work_state
                    SET
                        queued_work = ?,
                        processing_work = ?,
                        priority = ?,
                        next_schedule_at = ?,
                        scheduling_epoch =
                            scheduling_epoch + 1
                    WHERE logical_partition = ?
                      AND source = ?
                    """,
                    (
                        queued,
                        processing,
                        priority_value,
                        next_value,
                        int(logical_partition),
                        source,
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Scheduling acquisition
    # ============================================================

    def acquire_logical_schedule(
        self,
        worker_id: str,
        generation: int,
        fencing_epoch: int,
        lease_seconds: float = 300.0,
    ) -> Optional[
        tuple[
            LogicalWorkState,
            SchedulerLease,
        ]
    ]:
        now = time.time()

        lease_seconds = float(
            lease_seconds
        )

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive"
            )

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                worker = connection.execute(
                    """
                    SELECT *
                    FROM scheduler_workers
                    WHERE worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND status = 'active'
                    """,
                    (
                        worker_id,
                        int(generation),
                        int(fencing_epoch),
                    ),
                ).fetchone()

                if worker is None:
                    connection.execute(
                        "ROLLBACK"
                    )
                    return None

                active_leases = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM scheduler_leases
                    WHERE status = 'active'
                    """
                ).fetchone()

                if (
                    int(
                        active_leases["count"]
                    )
                    >= self.max_leases
                ):
                    connection.execute(
                        "ROLLBACK"
                    )
                    return None

                row = connection.execute(
                    """
                    SELECT *
                    FROM logical_work_state
                    WHERE
                        (
                            queued_work > 0
                            OR processing_work > 0
                        )
                        AND next_schedule_at <= ?
                    ORDER BY
                        priority DESC,
                        next_schedule_at ASC,
                        last_scheduled_at ASC,
                        scheduling_epoch ASC,
                        logical_partition ASC,
                        source ASC
                    LIMIT 1
                    """,
                    (now,),
                ).fetchone()

                if row is None:
                    connection.execute(
                        "ROLLBACK"
                    )
                    return None

                lease_id = uuid.uuid4().hex
                lease_until = (
                    now + lease_seconds
                )

                connection.execute(
                    """
                    INSERT INTO scheduler_leases (
                        lease_id,
                        worker_id,
                        logical_partition,
                        fencing_epoch,
                        acquired_at,
                        lease_until,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'active')
                    """,
                    (
                        lease_id,
                        worker_id,
                        int(
                            row[
                                "logical_partition"
                            ]
                        ),
                        int(fencing_epoch),
                        now,
                        lease_until,
                    ),
                )

                connection.execute(
                    """
                    UPDATE logical_work_state
                    SET
                        last_scheduled_at = ?,
                        next_schedule_at = ?,
                        scheduling_epoch =
                            scheduling_epoch + 1
                    WHERE logical_partition = ?
                      AND source = ?
                    """,
                    (
                        now,
                        now,
                        int(
                            row[
                                "logical_partition"
                            ]
                        ),
                        row["source"],
                    ),
                )

                connection.execute(
                    "COMMIT"
                )

                updated_row = connection.execute(
                    """
                    SELECT *
                    FROM logical_work_state
                    WHERE logical_partition = ?
                      AND source = ?
                    """,
                    (
                        int(
                            row[
                                "logical_partition"
                            ]
                        ),
                        row["source"],
                    ),
                ).fetchone()

                logical_state = (
                    self._logical_state_from_row(
                        updated_row
                    )
                )

                lease = SchedulerLease(
                    lease_id=lease_id,
                    worker_id=worker_id,
                    logical_partition=int(
                        row[
                            "logical_partition"
                        ]
                    ),
                    fencing_epoch=int(
                        fencing_epoch
                    ),
                    acquired_at=now,
                    lease_until=lease_until,
                )

                return (
                    logical_state,
                    lease,
                )

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

    def renew_lease(
        self,
        lease: SchedulerLease,
    ) -> bool:
        now = time.time()

        lease_until = (
            now
            + max(
                1.0,
                self.heartbeat_timeout,
            )
        )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE scheduler_leases
                    SET lease_until = ?
                    WHERE lease_id = ?
                      AND worker_id = ?
                      AND fencing_epoch = ?
                      AND status = 'active'
                      AND lease_until > ?
                    """,
                    (
                        lease_until,
                        lease.lease_id,
                        lease.worker_id,
                        int(
                            lease.fencing_epoch
                        ),
                        now,
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Fenced lease completion
    # ============================================================

    def complete_lease(
        self,
        lease: SchedulerLease,
    ) -> bool:
        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE scheduler_leases
                    SET status = 'complete'
                    WHERE lease_id = ?
                      AND worker_id = ?
                      AND fencing_epoch = ?
                      AND status = 'active'
                    """,
                    (
                        lease.lease_id,
                        lease.worker_id,
                        int(
                            lease.fencing_epoch
                        ),
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Fenced lease failure
    # ============================================================

    def release_lease(
        self,
        lease: SchedulerLease,
        retry_at: Optional[float] = None,
    ) -> bool:
        if retry_at is None:
            retry_at = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE scheduler_leases
                    SET status = 'released'
                    WHERE lease_id = ?
                      AND worker_id = ?
                      AND fencing_epoch = ?
                      AND status = 'active'
                    """,
                    (
                        lease.lease_id,
                        lease.worker_id,
                        int(
                            lease.fencing_epoch
                        ),
                    ),
                )

                if cursor.rowcount != 1:
                    return False

                connection.execute(
                    """
                    UPDATE logical_work_state
                    SET
                        next_schedule_at = ?,
                        scheduling_epoch =
                            scheduling_epoch + 1
                    WHERE logical_partition = ?
                    """,
                    (
                        float(retry_at),
                        int(
                            lease.logical_partition
                        ),
                    ),
                )

                return True

            finally:
                connection.close()

    # ============================================================
    # Expired lease recovery
    # ============================================================

    def recover_expired_leases(
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
                    SELECT *
                    FROM scheduler_leases
                    WHERE status = 'active'
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
                    cursor = connection.execute(
                        """
                        UPDATE scheduler_leases
                        SET status = 'expired'
                        WHERE lease_id = ?
                          AND status = 'active'
                          AND lease_until <= ?
                        """,
                        (
                            row["lease_id"],
                            now,
                        ),
                    )

                    if cursor.rowcount != 1:
                        continue

                    connection.execute(
                        """
                        UPDATE logical_work_state
                        SET
                            next_schedule_at = ?,
                            scheduling_epoch =
                                scheduling_epoch + 1
                        WHERE logical_partition = ?
                        """,
                        (
                            now,
                            int(
                                row[
                                    "logical_partition"
                                ]
                            ),
                        ),
                    )

                    recovered += 1

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
    # Source scheduling state
    # ============================================================

    def ensure_source(
        self,
        source: str,
        priority: float = 50.0,
    ) -> bool:
        source = source.strip()

        if not source:
            return False

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT INTO source_schedule_state (
                        source,
                        priority,
                        last_scheduled_at,
                        next_schedule_at,
                        scheduling_epoch,
                        active
                    )
                    VALUES (?, ?, 0, ?, 0, 1)
                    ON CONFLICT(source)
                    DO UPDATE SET
                        priority = excluded.priority
                    """,
                    (
                        source,
                        float(priority),
                        now,
                    ),
                )

                return cursor.rowcount >= 1

            finally:
                connection.close()

    # ============================================================
    # Durable checkpoints
    # ============================================================

    def write_checkpoint(
        self,
        name: str,
        value: str,
    ) -> bool:
        if not name:
            raise ValueError(
                "checkpoint name is required"
            )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT INTO scheduler_checkpoints (
                        checkpoint_name,
                        checkpoint_value,
                        updated_at
                    )
                    VALUES (?, ?, ?)
                    ON CONFLICT(checkpoint_name)
                    DO UPDATE SET
                        checkpoint_value =
                            excluded.checkpoint_value,
                        updated_at =
                            excluded.updated_at
                    """,
                    (
                        name,
                        str(value),
                        now,
                    ),
                )

                return cursor.rowcount >= 1

            finally:
                connection.close()

    def read_checkpoint(
        self,
        name: str,
    ) -> Optional[str]:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT checkpoint_value
                    FROM scheduler_checkpoints
                    WHERE checkpoint_name = ?
                    """,
                    (name,),
                ).fetchone()

                if row is None:
                    return None

                return str(
                    row["checkpoint_value"]
                )

            finally:
                connection.close()

    # ============================================================
    # Capacity
    # ============================================================

    def capacity(
        self,
    ) -> SchedulerCapacity:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                workers = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM scheduler_workers
                    WHERE status = 'active'
                      AND last_heartbeat >= ?
                    """,
                    (
                        now
                        - self.heartbeat_timeout,
                    ),
                ).fetchone()

                leases = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM scheduler_leases
                    WHERE status = 'active'
                      AND lease_until > ?
                    """,
                    (now,),
                ).fetchone()

                active_workers = int(
                    workers["count"]
                )

                active_leases = int(
                    leases["count"]
                )

                return SchedulerCapacity(
                    active_workers=active_workers,
                    max_workers=self.max_workers,
                    active_leases=active_leases,
                    max_leases=self.max_leases,
                    available_worker_capacity=max(
                        0,
                        self.max_workers
                        - active_workers,
                    ),
                    available_lease_capacity=max(
                        0,
                        self.max_leases
                        - active_leases,
                    ),
                )

            finally:
                connection.close()

    # ============================================================
    # Global statistics
    # ============================================================

    def stats(
        self,
    ) -> dict[str, int | float | str]:
        capacity = self.capacity()

        with self._lock:
            connection = self._connect()

            try:
                logical = connection.execute(
                    """
                    SELECT
                        COUNT(*) AS units,
                        COALESCE(
                            SUM(queued_work),
                            0
                        ) AS queued,
                        COALESCE(
                            SUM(processing_work),
                            0
                        ) AS processing
                    FROM logical_work_state
                    """
                ).fetchone()

                leases = connection.execute(
                    """
                    SELECT
                        status,
                        COUNT(*) AS count
                    FROM scheduler_leases
                    GROUP BY status
                    """
                ).fetchall()

                workers = connection.execute(
                    """
                    SELECT
                        status,
                        COUNT(*) AS count
                    FROM scheduler_workers
                    GROUP BY status
                    """
                ).fetchall()

                return {
                    "scheduler_epoch": (
                        self.scheduler_epoch
                    ),
                    "logical_work_units": int(
                        logical["units"]
                    ),
                    "queued_logical_work": int(
                        logical["queued"]
                    ),
                    "processing_logical_work": int(
                        logical["processing"]
                    ),
                    "active_workers": (
                        capacity.active_workers
                    ),
                    "max_workers": (
                        capacity.max_workers
                    ),
                    "active_leases": (
                        capacity.active_leases
                    ),
                    "max_leases": (
                        capacity.max_leases
                    ),
                    "available_worker_capacity": (
                        capacity.available_worker_capacity
                    ),
                    "available_lease_capacity": (
                        capacity.available_lease_capacity
                    ),
                    "worker_states": sum(
                        int(row["count"])
                        for row in workers
                    ),
                    "lease_states": sum(
                        int(row["count"])
                        for row in leases
                    ),
                }

            finally:
                connection.close()
