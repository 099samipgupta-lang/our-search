"""
OUR SEARCH
Phase 9.1 — Global Distributed Execution Fabric

Architecture target:
    Giant Web-scale execution infrastructure capable of coordinating
    enormous numbers of workers and workloads across billions/trillions
    of publicly accessible Web resources.

This module is an execution-control architecture.
It does NOT impose a small fixed worker, machine, queue, or workload
ceiling.

It intentionally separates:
    workload identity
    execution partitions
    worker registration
    worker generations
    leases
    fencing
    capacity
    heartbeats
    fault isolation
    placement
    checkpoints
    execution accounting

SQLite is used here only as a durable local/control-plane implementation.
A production deployment can map the same contracts onto partitioned
distributed metadata services without changing the execution model.

No Google technology or Google infrastructure is used.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, asdict
from typing import Any, Iterable, Optional


VERSION = "global-distributed-execution-fabric.v1"

DEFAULT_PARTITION_COUNT = 1_048_576
DEFAULT_LEASE_SECONDS = 300.0
DEFAULT_HEARTBEAT_TIMEOUT = 90.0
DEFAULT_CHECKPOINT_INTERVAL = 30.0


@dataclass(frozen=True)
class ExecutionPartition:
    partition_id: int
    namespace: str
    state: str = "active"
    owner_id: Optional[str] = None
    owner_generation: Optional[int] = None
    fencing_epoch: int = 0
    workload_count: int = 0
    active_work: int = 0
    completed_work: int = 0
    failed_work: int = 0
    last_scheduled_at: Optional[float] = None
    updated_at: Optional[float] = None


@dataclass(frozen=True)
class ExecutionWorker:
    worker_id: str
    generation: int
    state: str
    capacity: int
    active_work: int
    registered_at: float
    last_heartbeat: float
    heartbeat_deadline: float
    fencing_epoch: int
    region: Optional[str] = None
    zone: Optional[str] = None
    endpoint: Optional[str] = None


@dataclass(frozen=True)
class ExecutionWork:
    work_id: str
    partition_id: int
    workload_type: str
    payload_hash: str
    priority: float
    created_at: float
    available_at: float
    status: str = "queued"
    attempts: int = 0
    worker_id: Optional[str] = None
    worker_generation: Optional[int] = None
    fencing_token: Optional[int] = None
    lease_until: Optional[float] = None
    completed_at: Optional[float] = None
    failed_at: Optional[float] = None
    next_attempt_at: Optional[float] = None
    last_error: Optional[str] = None


@dataclass(frozen=True)
class ExecutionLease:
    work_id: str
    worker_id: str
    worker_generation: int
    fencing_token: int
    leased_at: float
    lease_until: float


@dataclass(frozen=True)
class ExecutionCheckpoint:
    controller_id: str
    epoch: int
    partition_cursor: int
    workload_cursor: Optional[str]
    updated_at: float


@dataclass(frozen=True)
class ExecutionCapacity:
    worker_capacity: int
    active_workers: int
    active_work: int
    available_worker_slots: int
    queued_work: int
    can_accept: bool


@dataclass(frozen=True)
class ExecutionCycleResult:
    epoch: int
    recovered_leases: int
    expired_workers: int
    partitions_observed: int
    queued_work: int
    processing_work: int
    completed_work: int
    failed_work: int


class GlobalDistributedExecutionFabric:
    """
    Global distributed execution control layer.

    Core properties:

    - deterministic workload partitioning
    - partition-aware execution
    - worker generations
    - fencing tokens
    - durable leases
    - heartbeat supervision
    - expired lease recovery
    - retry-safe execution
    - bounded local batches without global workload ceilings
    - durable checkpoints
    - capacity-aware admission
    - partition statistics
    - worker statistics
    - epoch-based execution accounting

    The number of physical machines/workers is deliberately not encoded
    as a fixed architectural constant.
    """

    def __init__(
        self,
        storage_root: str,
        partition_count: int = DEFAULT_PARTITION_COUNT,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT,
        checkpoint_interval: float = DEFAULT_CHECKPOINT_INTERVAL,
        max_attempts: int = 8,
        admission_limit: Optional[int] = None,
    ) -> None:
        if partition_count <= 0:
            raise ValueError(
                "partition_count must be positive"
            )

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive"
            )

        if heartbeat_timeout <= 0:
            raise ValueError(
                "heartbeat_timeout must be positive"
            )

        if checkpoint_interval <= 0:
            raise ValueError(
                "checkpoint_interval must be positive"
            )

        if max_attempts <= 0:
            raise ValueError(
                "max_attempts must be positive"
            )

        if admission_limit is not None and admission_limit <= 0:
            raise ValueError(
                "admission_limit must be positive"
            )

        self.storage_root = storage_root
        self.partition_count = partition_count
        self.lease_seconds = lease_seconds
        self.heartbeat_timeout = heartbeat_timeout
        self.checkpoint_interval = checkpoint_interval
        self.max_attempts = max_attempts
        self.admission_limit = admission_limit

        self.db_path = (
            f"{storage_root}/"
            "global_distributed_execution_fabric.db"
        )

        self._lock = threading.RLock()
        self._running = False
        self._epoch = 0
        self._last_checkpoint_at: Optional[float] = None

        self.stats_data = {
            "version": VERSION,
            "epochs": 0,
            "cycles": 0,
            "workers_registered": 0,
            "workers_expired": 0,
            "worker_fences": 0,
            "partitions_created": 0,
            "work_enqueued": 0,
            "work_claimed": 0,
            "work_completed": 0,
            "work_failed": 0,
            "lease_renewals": 0,
            "lease_recoveries": 0,
            "checkpoints": 0,
            "runtime_errors": 0,
            "fixed_worker_limit": False,
            "fixed_global_execution_limit": False,
        }

        self._initialize()

    # ------------------------------------------------------------------
    # Durable database
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
            timeout=30.0,
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

        return connection

    def _initialize(self) -> None:
        connection = self._connect()

        try:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS execution_workers (
                    worker_id TEXT PRIMARY KEY,
                    generation INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    capacity INTEGER NOT NULL,
                    active_work INTEGER NOT NULL DEFAULT 0,
                    registered_at REAL NOT NULL,
                    last_heartbeat REAL NOT NULL,
                    heartbeat_deadline REAL NOT NULL,
                    fencing_epoch INTEGER NOT NULL,
                    region TEXT,
                    zone TEXT,
                    endpoint TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                idx_execution_workers_state
                ON execution_workers(
                    state,
                    last_heartbeat
                );

                CREATE TABLE IF NOT EXISTS execution_partitions (
                    partition_id INTEGER PRIMARY KEY,
                    namespace TEXT NOT NULL,
                    state TEXT NOT NULL,
                    owner_id TEXT,
                    owner_generation INTEGER,
                    fencing_epoch INTEGER NOT NULL,
                    workload_count INTEGER NOT NULL DEFAULT 0,
                    active_work INTEGER NOT NULL DEFAULT 0,
                    completed_work INTEGER NOT NULL DEFAULT 0,
                    failed_work INTEGER NOT NULL DEFAULT 0,
                    last_scheduled_at REAL,
                    updated_at REAL
                );

                CREATE INDEX IF NOT EXISTS
                idx_execution_partitions_owner
                ON execution_partitions(
                    owner_id,
                    state
                );

                CREATE INDEX IF NOT EXISTS
                idx_execution_partitions_state
                ON execution_partitions(
                    state,
                    updated_at
                );

                CREATE TABLE IF NOT EXISTS execution_work (
                    work_id TEXT PRIMARY KEY,
                    partition_id INTEGER NOT NULL,
                    workload_type TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    priority REAL NOT NULL,
                    created_at REAL NOT NULL,
                    available_at REAL NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    worker_id TEXT,
                    worker_generation INTEGER,
                    fencing_token INTEGER,
                    lease_until REAL,
                    completed_at REAL,
                    failed_at REAL,
                    next_attempt_at REAL,
                    last_error TEXT
                );

                CREATE INDEX IF NOT EXISTS
                idx_execution_work_ready
                ON execution_work(
                    status,
                    available_at,
                    next_attempt_at,
                    priority DESC,
                    created_at,
                    work_id
                );

                CREATE INDEX IF NOT EXISTS
                idx_execution_work_partition
                ON execution_work(
                    partition_id,
                    status,
                    priority DESC
                );

                CREATE INDEX IF NOT EXISTS
                idx_execution_work_worker
                ON execution_work(
                    worker_id,
                    status
                );

                CREATE INDEX IF NOT EXISTS
                idx_execution_work_lease
                ON execution_work(
                    status,
                    lease_until
                );

                CREATE TABLE IF NOT EXISTS execution_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    work_id TEXT,
                    partition_id INTEGER,
                    worker_id TEXT,
                    worker_generation INTEGER,
                    event TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    metadata_json TEXT
                );

                CREATE INDEX IF NOT EXISTS
                idx_execution_history_work
                ON execution_history(
                    work_id,
                    timestamp
                );

                CREATE TABLE IF NOT EXISTS execution_checkpoints (
                    controller_id TEXT PRIMARY KEY,
                    epoch INTEGER NOT NULL,
                    partition_cursor INTEGER NOT NULL,
                    workload_cursor TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS execution_epochs (
                    epoch INTEGER PRIMARY KEY,
                    started_at REAL NOT NULL,
                    completed_at REAL,
                    recovered_leases INTEGER NOT NULL DEFAULT 0,
                    expired_workers INTEGER NOT NULL DEFAULT 0,
                    partitions_observed INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS execution_fencing (
                    resource_id TEXT PRIMARY KEY,
                    fencing_epoch INTEGER NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )

            connection.commit()

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Deterministic identities
    # ------------------------------------------------------------------

    @staticmethod
    def _digest(value: str) -> bytes:
        return hashlib.sha256(
            value.encode("utf-8")
        ).digest()

    def partition_for(
        self,
        value: str,
    ) -> int:
        normalized = (
            str(value)
            .strip()
            .lower()
        )

        digest = self._digest(
            normalized
        )

        return (
            int.from_bytes(
                digest[:8],
                "big",
            )
            % self.partition_count
        )

    @staticmethod
    def work_id_for(
        partition_id: int,
        workload_type: str,
        payload_hash: str,
    ) -> str:
        raw = (
            f"{partition_id}|"
            f"{workload_type}|"
            f"{payload_hash}"
        )

        return hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

    @staticmethod
    def payload_hash(
        payload: Any,
    ) -> str:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )

        return hashlib.sha256(
            encoded.encode("utf-8")
        ).hexdigest()

    # ------------------------------------------------------------------
    # Epochs
    # ------------------------------------------------------------------

    def begin_epoch(
        self,
        epoch: Optional[int] = None,
    ) -> int:
        with self._lock:
            if epoch is None:
                self._epoch += 1
            else:
                self._epoch = max(
                    self._epoch,
                    int(epoch),
                )

            current = self._epoch

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT OR IGNORE INTO execution_epochs (
                    epoch,
                    started_at
                )
                VALUES (?, ?)
                """,
                (
                    current,
                    time.time(),
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data["epochs"] += 1

        return current

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    def _new_worker_id() -> str:
        return (
            "worker-"
            + uuid.uuid4().hex
        )

    def register_worker(
        self,
        worker_id: Optional[str] = None,
        capacity: int = 1,
        region: Optional[str] = None,
        zone: Optional[str] = None,
        endpoint: Optional[str] = None,
    ) -> ExecutionWorker:
        if capacity <= 0:
            raise ValueError(
                "capacity must be positive"
            )

        if not worker_id:
            worker_id = (
                self._new_worker_id()
            )

        now = time.time()
        deadline = (
            now + self.heartbeat_timeout
        )

        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT
                    generation,
                    fencing_epoch
                FROM execution_workers
                WHERE worker_id = ?
                """,
                (worker_id,),
            ).fetchone()

            if row is None:
                generation = 1
                fencing_epoch = 1

                connection.execute(
                    """
                    INSERT INTO execution_workers (
                        worker_id,
                        generation,
                        state,
                        capacity,
                        active_work,
                        registered_at,
                        last_heartbeat,
                        heartbeat_deadline,
                        fencing_epoch,
                        region,
                        zone,
                        endpoint,
                        updated_at
                    )
                    VALUES (
                        ?, ?, 'active', ?, 0,
                        ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        worker_id,
                        generation,
                        capacity,
                        now,
                        now,
                        deadline,
                        fencing_epoch,
                        region,
                        zone,
                        endpoint,
                        now,
                    ),
                )

            else:
                generation = (
                    int(row["generation"])
                    + 1
                )

                fencing_epoch = (
                    int(row["fencing_epoch"])
                    + 1
                )

                connection.execute(
                    """
                    UPDATE execution_workers
                    SET
                        generation = ?,
                        state = 'active',
                        capacity = ?,
                        active_work = 0,
                        registered_at = ?,
                        last_heartbeat = ?,
                        heartbeat_deadline = ?,
                        fencing_epoch = ?,
                        region = ?,
                        zone = ?,
                        endpoint = ?,
                        updated_at = ?
                    WHERE worker_id = ?
                    """,
                    (
                        generation,
                        capacity,
                        now,
                        now,
                        deadline,
                        fencing_epoch,
                        region,
                        zone,
                        endpoint,
                        now,
                        worker_id,
                    ),
                )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "workers_registered"
        ] += 1

        return self.get_worker(
            worker_id
        )

    def get_worker(
        self,
        worker_id: str,
    ) -> Optional[ExecutionWorker]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM execution_workers
                WHERE worker_id = ?
                """,
                (worker_id,),
            ).fetchone()

        finally:
            connection.close()

        if row is None:
            return None

        return ExecutionWorker(
            worker_id=str(
                row["worker_id"]
            ),
            generation=int(
                row["generation"]
            ),
            state=str(
                row["state"]
            ),
            capacity=int(
                row["capacity"]
            ),
            active_work=int(
                row["active_work"]
            ),
            registered_at=float(
                row["registered_at"]
            ),
            last_heartbeat=float(
                row["last_heartbeat"]
            ),
            heartbeat_deadline=float(
                row["heartbeat_deadline"]
            ),
            fencing_epoch=int(
                row["fencing_epoch"]
            ),
            region=row["region"],
            zone=row["zone"],
            endpoint=row["endpoint"],
        )

    def heartbeat(
        self,
        worker_id: str,
        generation: int,
    ) -> bool:
        now = time.time()
        deadline = (
            now + self.heartbeat_timeout
        )

        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE execution_workers
                SET
                    state = 'active',
                    last_heartbeat = ?,
                    heartbeat_deadline = ?,
                    updated_at = ?
                WHERE
                    worker_id = ?
                    AND generation = ?
                    AND state != 'fenced'
                """,
                (
                    now,
                    deadline,
                    now,
                    worker_id,
                    generation,
                ),
            ).rowcount

            connection.commit()

        finally:
            connection.close()

        return updated == 1

    def fence_worker(
        self,
        worker_id: str,
        generation: Optional[int] = None,
    ) -> bool:
        connection = self._connect()

        try:
            if generation is None:
                updated = connection.execute(
                    """
                    UPDATE execution_workers
                    SET
                        state = 'fenced',
                        fencing_epoch =
                            fencing_epoch + 1,
                        updated_at = ?
                    WHERE
                        worker_id = ?
                        AND state != 'fenced'
                    """,
                    (
                        time.time(),
                        worker_id,
                    ),
                ).rowcount

            else:
                updated = connection.execute(
                    """
                    UPDATE execution_workers
                    SET
                        state = 'fenced',
                        fencing_epoch =
                            fencing_epoch + 1,
                        updated_at = ?
                    WHERE
                        worker_id = ?
                        AND generation = ?
                        AND state != 'fenced'
                    """,
                    (
                        time.time(),
                        worker_id,
                        generation,
                    ),
                ).rowcount

            connection.commit()

        finally:
            connection.close()

        if updated == 1:
            self.stats_data[
                "worker_fences"
            ] += 1

        return updated == 1

    def expire_workers(
        self,
        now: Optional[float] = None,
    ) -> int:
        if now is None:
            now = time.time()

        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT
                    worker_id,
                    generation
                FROM execution_workers
                WHERE
                    state = 'active'
                    AND heartbeat_deadline <= ?
                """,
                (now,),
            ).fetchall()

            expired = 0

            for row in rows:
                updated = connection.execute(
                    """
                    UPDATE execution_workers
                    SET
                        state = 'expired',
                        fencing_epoch =
                            fencing_epoch + 1,
                        updated_at = ?
                    WHERE
                        worker_id = ?
                        AND generation = ?
                        AND state = 'active'
                    """,
                    (
                        now,
                        str(row["worker_id"]),
                        int(row["generation"]),
                    ),
                ).rowcount

                expired += updated

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "workers_expired"
        ] += expired

        return expired

    # ------------------------------------------------------------------
    # Partition lifecycle
    # ------------------------------------------------------------------

    def ensure_partition(
        self,
        partition_id: int,
        namespace: str = "global",
    ) -> ExecutionPartition:
        if not (
            0 <= partition_id
            < self.partition_count
        ):
            raise ValueError(
                "partition_id outside configured namespace"
            )

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT OR IGNORE INTO execution_partitions (
                    partition_id,
                    namespace,
                    state,
                    owner_id,
                    owner_generation,
                    fencing_epoch,
                    workload_count,
                    active_work,
                    completed_work,
                    failed_work,
                    last_scheduled_at,
                    updated_at
                )
                VALUES (
                    ?, ?, 'active',
                    NULL, NULL, 0,
                    0, 0, 0, 0, NULL, ?
                )
                """,
                (
                    partition_id,
                    namespace,
                    time.time(),
                ),
            )

            connection.commit()

        finally:
            connection.close()

        return self.get_partition(
            partition_id
        )

    def get_partition(
        self,
        partition_id: int,
    ) -> Optional[ExecutionPartition]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM execution_partitions
                WHERE partition_id = ?
                """,
                (partition_id,),
            ).fetchone()

        finally:
            connection.close()

        if row is None:
            return None

        return ExecutionPartition(
            partition_id=int(
                row["partition_id"]
            ),
            namespace=str(
                row["namespace"]
            ),
            state=str(
                row["state"]
            ),
            owner_id=row["owner_id"],
            owner_generation=(
                row["owner_generation"]
            ),
            fencing_epoch=int(
                row["fencing_epoch"]
            ),
            workload_count=int(
                row["workload_count"]
            ),
            active_work=int(
                row["active_work"]
            ),
            completed_work=int(
                row["completed_work"]
            ),
            failed_work=int(
                row["failed_work"]
            ),
            last_scheduled_at=(
                row["last_scheduled_at"]
            ),
            updated_at=(
                row["updated_at"]
            ),
        )

    def assign_partition(
        self,
        partition_id: int,
        worker_id: str,
        generation: int,
    ) -> bool:
        self.ensure_partition(
            partition_id
        )

        worker = self.get_worker(
            worker_id
        )

        if (
            worker is None
            or worker.generation != generation
            or worker.state != "active"
        ):
            return False

        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE execution_partitions
                SET
                    owner_id = ?,
                    owner_generation = ?,
                    fencing_epoch =
                        fencing_epoch + 1,
                    updated_at = ?
                WHERE
                    partition_id = ?
                    AND state = 'active'
                """,
                (
                    worker_id,
                    generation,
                    time.time(),
                    partition_id,
                ),
            ).rowcount

            connection.commit()

        finally:
            connection.close()

        return updated == 1

    def release_partition(
        self,
        partition_id: int,
        worker_id: str,
        generation: int,
    ) -> bool:
        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE execution_partitions
                SET
                    owner_id = NULL,
                    owner_generation = NULL,
                    fencing_epoch =
                        fencing_epoch + 1,
                    updated_at = ?
                WHERE
                    partition_id = ?
                    AND owner_id = ?
                    AND owner_generation = ?
                """,
                (
                    time.time(),
                    partition_id,
                    worker_id,
                    generation,
                ),
            ).rowcount

            connection.commit()

        finally:
            connection.close()

        return updated == 1

    # ------------------------------------------------------------------
    # Work admission
    # ------------------------------------------------------------------

    def _active_work_count(
        self,
    ) -> int:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM execution_work
                WHERE status = 'processing'
                """
            ).fetchone()

            return int(row[0])

        finally:
            connection.close()

    def capacity(
        self,
    ) -> ExecutionCapacity:
        connection = self._connect()

        try:
            worker_row = connection.execute(
                """
                SELECT
                    COALESCE(
                        SUM(
                            CASE
                                WHEN state = 'active'
                                THEN capacity
                                ELSE 0
                            END
                        ),
                        0
                    ),
                    COUNT(
                        CASE
                            WHEN state = 'active'
                            THEN 1
                        END
                    )
                FROM execution_workers
                """
            ).fetchone()

            queued = connection.execute(
                """
                SELECT COUNT(*)
                FROM execution_work
                WHERE status = 'queued'
                """
            ).fetchone()[0]

        finally:
            connection.close()

        worker_capacity = int(
            worker_row[0]
        )

        active_workers = int(
            worker_row[1]
        )

        active_work = (
            self._active_work_count()
        )

        available_slots = max(
            0,
            worker_capacity - active_work,
        )

        if self.admission_limit is not None:
            available_slots = min(
                available_slots,
                max(
                    0,
                    self.admission_limit
                    - active_work,
                ),
            )

        return ExecutionCapacity(
            worker_capacity=worker_capacity,
            active_workers=active_workers,
            active_work=active_work,
            available_worker_slots=(
                available_slots
            ),
            queued_work=int(queued),
            can_accept=(
                available_slots > 0
            ),
        )

    def can_accept_work(self) -> bool:
        if self.admission_limit is None:
            return True

        return (
            self._active_work_count()
            < self.admission_limit
        )

    def enqueue(
        self,
        payload: Any,
        workload_type: str,
        priority: float = 50.0,
        partition_key: Optional[str] = None,
        available_at: Optional[float] = None,
    ) -> Optional[ExecutionWork]:
        if not workload_type:
            raise ValueError(
                "workload_type must not be empty"
            )

        if not self.can_accept_work():
            return None

        if partition_key is None:
            partition_key = (
                self.payload_hash(payload)
            )

        partition_id = self.partition_for(
            partition_key
        )

        self.ensure_partition(
            partition_id
        )

        payload_hash = self.payload_hash(
            payload
        )

        work_id = self.work_id_for(
            partition_id,
            workload_type,
            payload_hash,
        )

        now = time.time()

        if available_at is None:
            available_at = now

        connection = self._connect()

        try:
            inserted = connection.execute(
                """
                INSERT OR IGNORE INTO execution_work (
                    work_id,
                    partition_id,
                    workload_type,
                    payload_hash,
                    payload_json,
                    priority,
                    created_at,
                    available_at,
                    status,
                    attempts,
                    worker_id,
                    worker_generation,
                    fencing_token,
                    lease_until,
                    completed_at,
                    failed_at,
                    next_attempt_at,
                    last_error
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    'queued', 0,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL
                )
                """,
                (
                    work_id,
                    partition_id,
                    workload_type,
                    payload_hash,
                    json.dumps(
                        payload,
                        sort_keys=True,
                        default=str,
                    ),
                    float(priority),
                    now,
                    float(available_at),
                ),
            ).rowcount

            if inserted == 1:
                connection.execute(
                    """
                    UPDATE execution_partitions
                    SET
                        workload_count =
                            workload_count + 1,
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        now,
                        partition_id,
                    ),
                )

                self._history(
                    connection,
                    work_id,
                    partition_id,
                    None,
                    None,
                    "enqueued",
                )

            connection.commit()

        finally:
            connection.close()

        if inserted != 1:
            return None

        self.stats_data[
            "work_enqueued"
        ] += 1

        return self.get_work(
            work_id
        )

    def enqueue_many(
        self,
        items: Iterable[
            tuple[Any, str, float, Optional[str]]
        ],
    ) -> int:
        inserted_count = 0

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            now = time.time()

            for payload, workload_type, priority, partition_key in items:
                if not self.can_accept_work():
                    break

                if not workload_type:
                    continue

                if partition_key is None:
                    partition_key = (
                        self.payload_hash(payload)
                    )

                partition_id = self.partition_for(
                    partition_key
                )

                connection.execute(
                    """
                    INSERT OR IGNORE INTO execution_partitions (
                        partition_id,
                        namespace,
                        state,
                        fencing_epoch,
                        updated_at
                    )
                    VALUES (
                        ?, 'global', 'active', 0, ?
                    )
                    """,
                    (
                        partition_id,
                        now,
                    ),
                )

                payload_hash = (
                    self.payload_hash(
                        payload
                    )
                )

                work_id = self.work_id_for(
                    partition_id,
                    workload_type,
                    payload_hash,
                )

                inserted = connection.execute(
                    """
                    INSERT OR IGNORE INTO execution_work (
                        work_id,
                        partition_id,
                        workload_type,
                        payload_hash,
                        payload_json,
                        priority,
                        created_at,
                        available_at,
                        status,
                        attempts
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?,
                        'queued', 0
                    )
                    """,
                    (
                        work_id,
                        partition_id,
                        workload_type,
                        payload_hash,
                        json.dumps(
                            payload,
                            sort_keys=True,
                            default=str,
                        ),
                        float(priority),
                        now,
                        now,
                    ),
                ).rowcount

                if inserted == 1:
                    connection.execute(
                        """
                        UPDATE execution_partitions
                        SET
                            workload_count =
                                workload_count + 1,
                            updated_at = ?
                        WHERE partition_id = ?
                        """,
                        (
                            now,
                            partition_id,
                        ),
                    )

                    inserted_count += 1

                    self._history(
                        connection,
                        work_id,
                        partition_id,
                        None,
                        None,
                        "enqueued",
                    )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "work_enqueued"
        ] += inserted_count

        return inserted_count

    # ------------------------------------------------------------------
    # Work claiming
    # ------------------------------------------------------------------

    def claim(
        self,
        worker_id: str,
        generation: int,
        limit: int = 1,
        partition_id: Optional[int] = None,
    ) -> list[ExecutionWork]:
        if limit <= 0:
            return []

        worker = self.get_worker(
            worker_id
        )

        if (
            worker is None
            or worker.generation != generation
            or worker.state != "active"
        ):
            return []

        available_capacity = max(
            0,
            worker.capacity
            - worker.active_work,
        )

        limit = min(
            limit,
            available_capacity,
        )

        if limit <= 0:
            return []

        now = time.time()

        connection = self._connect()

        claimed: list[
            ExecutionWork
        ] = []

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            query = """
                SELECT *
                FROM execution_work
                WHERE
                    status = 'queued'
                    AND available_at <= ?
                    AND (
                        next_attempt_at IS NULL
                        OR next_attempt_at <= ?
                    )
            """

            params: list[Any] = [
                now,
                now,
            ]

            if partition_id is not None:
                query += """
                    AND partition_id = ?
                """
                params.append(
                    int(partition_id)
                )

            query += """
                ORDER BY
                    priority DESC,
                    created_at ASC,
                    work_id ASC
                LIMIT ?
            """

            params.append(limit)

            rows = connection.execute(
                query,
                tuple(params),
            ).fetchall()

            fencing_token = (
                time.time_ns()
            )

            lease_until = (
                now + self.lease_seconds
            )

            for row in rows:
                updated = connection.execute(
                    """
                    UPDATE execution_work
                    SET
                        status = 'processing',
                        attempts = attempts + 1,
                        worker_id = ?,
                        worker_generation = ?,
                        fencing_token = ?,
                        lease_until = ?,
                        last_error = NULL
                    WHERE
                        work_id = ?
                        AND status = 'queued'
                    """,
                    (
                        worker_id,
                        generation,
                        fencing_token,
                        lease_until,
                        str(row["work_id"]),
                    ),
                ).rowcount

                if updated != 1:
                    continue

                attempts = (
                    int(row["attempts"])
                    + 1
                )

                item = ExecutionWork(
                    work_id=str(
                        row["work_id"]
                    ),
                    partition_id=int(
                        row["partition_id"]
                    ),
                    workload_type=str(
                        row["workload_type"]
                    ),
                    payload_hash=str(
                        row["payload_hash"]
                    ),
                    priority=float(
                        row["priority"]
                    ),
                    created_at=float(
                        row["created_at"]
                    ),
                    available_at=float(
                        row["available_at"]
                    ),
                    status="processing",
                    attempts=attempts,
                    worker_id=worker_id,
                    worker_generation=generation,
                    fencing_token=(
                        fencing_token
                    ),
                    lease_until=(
                        lease_until
                    ),
                    next_attempt_at=(
                        row["next_attempt_at"]
                    ),
                    last_error=None,
                )

                claimed.append(item)

                connection.execute(
                    """
                    UPDATE execution_workers
                    SET
                        active_work =
                            active_work + 1,
                        updated_at = ?
                    WHERE
                        worker_id = ?
                        AND generation = ?
                    """,
                    (
                        now,
                        worker_id,
                        generation,
                    ),
                )

                connection.execute(
                    """
                    UPDATE execution_partitions
                    SET
                        active_work =
                            active_work + 1,
                        last_scheduled_at = ?,
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        now,
                        now,
                        item.partition_id,
                    ),
                )

                self._history(
                    connection,
                    item.work_id,
                    item.partition_id,
                    worker_id,
                    generation,
                    "claimed",
                    {
                        "fencing_token":
                            fencing_token,
                    },
                )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "work_claimed"
        ] += len(claimed)

        return claimed

    # ------------------------------------------------------------------
    # Lease operations
    # ------------------------------------------------------------------

    def lease_for(
        self,
        work: ExecutionWork,
    ) -> ExecutionLease:
        if (
            work.worker_id is None
            or work.worker_generation is None
            or work.fencing_token is None
            or work.lease_until is None
        ):
            raise ValueError(
                "work does not contain an active lease"
            )

        return ExecutionLease(
            work_id=work.work_id,
            worker_id=work.worker_id,
            worker_generation=(
                work.worker_generation
            ),
            fencing_token=(
                work.fencing_token
            ),
            leased_at=(
                work.lease_until
                - self.lease_seconds
            ),
            lease_until=(
                work.lease_until
            ),
        )

    def renew(
        self,
        work: ExecutionWork,
    ) -> bool:
        if (
            work.worker_id is None
            or work.worker_generation is None
            or work.fencing_token is None
        ):
            return False

        now = time.time()
        lease_until = (
            now + self.lease_seconds
        )

        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE execution_work
                SET
                    lease_until = ?
                WHERE
                    work_id = ?
                    AND status = 'processing'
                    AND worker_id = ?
                    AND worker_generation = ?
                    AND fencing_token = ?
                """,
                (
                    lease_until,
                    work.work_id,
                    work.worker_id,
                    work.worker_generation,
                    work.fencing_token,
                ),
            ).rowcount

            connection.commit()

        finally:
            connection.close()

        if updated == 1:
            self.stats_data[
                "lease_renewals"
            ] += 1

        return updated == 1

    # ------------------------------------------------------------------
    # Completion
    # ------------------------------------------------------------------

    def complete(
        self,
        work: ExecutionWork,
        metadata: Optional[
            dict[str, Any]
        ] = None,
    ) -> bool:
        if (
            work.worker_id is None
            or work.worker_generation is None
            or work.fencing_token is None
        ):
            return False

        now = time.time()

        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE execution_work
                SET
                    status = 'completed',
                    worker_id = NULL,
                    worker_generation = NULL,
                    fencing_token = NULL,
                    lease_until = NULL,
                    completed_at = ?,
                    next_attempt_at = NULL,
                    last_error = NULL
                WHERE
                    work_id = ?
                    AND status = 'processing'
                    AND worker_id = ?
                    AND worker_generation = ?
                    AND fencing_token = ?
                """,
                (
                    now,
                    work.work_id,
                    work.worker_id,
                    work.worker_generation,
                    work.fencing_token,
                ),
            ).rowcount

            if updated == 1:
                connection.execute(
                    """
                    UPDATE execution_workers
                    SET
                        active_work =
                            MAX(
                                0,
                                active_work - 1
                            ),
                        updated_at = ?
                    WHERE
                        worker_id = ?
                        AND generation = ?
                    """,
                    (
                        now,
                        work.worker_id,
                        work.worker_generation,
                    ),
                )

                connection.execute(
                    """
                    UPDATE execution_partitions
                    SET
                        active_work =
                            MAX(
                                0,
                                active_work - 1
                            ),
                        completed_work =
                            completed_work + 1,
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        now,
                        work.partition_id,
                    ),
                )

                self._history(
                    connection,
                    work.work_id,
                    work.partition_id,
                    work.worker_id,
                    work.worker_generation,
                    "completed",
                    metadata,
                )

            connection.commit()

        finally:
            connection.close()

        if updated != 1:
            return False

        self.stats_data[
            "work_completed"
        ] += 1

        return True

    # ------------------------------------------------------------------
    # Failure / retry
    # ------------------------------------------------------------------

    def fail(
        self,
        work: ExecutionWork,
        error: str,
    ) -> bool:
        if (
            work.worker_id is None
            or work.worker_generation is None
            or work.fencing_token is None
        ):
            return False

        now = time.time()

        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT attempts
                FROM execution_work
                WHERE work_id = ?
                """,
                (work.work_id,),
            ).fetchone()

            if row is None:
                connection.rollback()
                return False

            attempts = int(
                row["attempts"]
            )

            if attempts >= self.max_attempts:
                status = "failed"
                next_attempt = None
            else:
                status = "queued"

                backoff = min(
                    3600.0,
                    2.0 ** min(
                        attempts,
                        12,
                    ),
                )

                next_attempt = (
                    now + backoff
                )

            updated = connection.execute(
                """
                UPDATE execution_work
                SET
                    status = ?,
                    worker_id = NULL,
                    worker_generation = NULL,
                    fencing_token = NULL,
                    lease_until = NULL,
                    failed_at = ?,
                    next_attempt_at = ?,
                    last_error = ?
                WHERE
                    work_id = ?
                    AND status = 'processing'
                    AND worker_id = ?
                    AND worker_generation = ?
                    AND fencing_token = ?
                """,
                (
                    status,
                    now,
                    next_attempt,
                    str(error)[:4000],
                    work.work_id,
                    work.worker_id,
                    work.worker_generation,
                    work.fencing_token,
                ),
            ).rowcount

            if updated == 1:
                connection.execute(
                    """
                    UPDATE execution_workers
                    SET
                        active_work =
                            MAX(
                                0,
                                active_work - 1
                            ),
                        updated_at = ?
                    WHERE
                        worker_id = ?
                        AND generation = ?
                    """,
                    (
                        now,
                        work.worker_id,
                        work.worker_generation,
                    ),
                )

                connection.execute(
                    """
                    UPDATE execution_partitions
                    SET
                        active_work =
                            MAX(
                                0,
                                active_work - 1
                            ),
                        failed_work =
                            failed_work + 1,
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        now,
                        work.partition_id,
                    ),
                )

                self._history(
                    connection,
                    work.work_id,
                    work.partition_id,
                    work.worker_id,
                    work.worker_generation,
                    "failed",
                    {
                        "status": status,
                        "error": str(error)[:4000],
                    },
                )

            connection.commit()

        finally:
            connection.close()

        if updated != 1:
            return False

        self.stats_data[
            "work_failed"
        ] += 1

        return True

    # ------------------------------------------------------------------
    # Lease recovery
    # ------------------------------------------------------------------

    def recover_expired_leases(
        self,
        now: Optional[float] = None,
    ) -> int:
        if now is None:
            now = time.time()

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            rows = connection.execute(
                """
                SELECT
                    work_id,
                    partition_id,
                    attempts,
                    worker_id,
                    worker_generation
                FROM execution_work
                WHERE
                    status = 'processing'
                    AND lease_until IS NOT NULL
                    AND lease_until <= ?
                """,
                (now,),
            ).fetchall()

            recovered = 0

            for row in rows:
                attempts = int(
                    row["attempts"]
                )

                if attempts >= self.max_attempts:
                    status = "failed"
                    next_attempt = None
                else:
                    status = "queued"

                    backoff = min(
                        3600.0,
                        2.0 ** min(
                            attempts,
                            12,
                        ),
                    )

                    next_attempt = (
                        now + backoff
                    )

                updated = connection.execute(
                    """
                    UPDATE execution_work
                    SET
                        status = ?,
                        worker_id = NULL,
                        worker_generation = NULL,
                        fencing_token = NULL,
                        lease_until = NULL,
                        failed_at = ?,
                        next_attempt_at = ?,
                        last_error = ?
                    WHERE
                        work_id = ?
                        AND status = 'processing'
                    """,
                    (
                        status,
                        now,
                        next_attempt,
                        "expired execution lease recovered",
                        str(row["work_id"]),
                    ),
                ).rowcount

                if updated != 1:
                    continue

                recovered += 1

                if row["worker_id"] is not None:
                    connection.execute(
                        """
                        UPDATE execution_workers
                        SET
                            active_work =
                                MAX(
                                    0,
                                    active_work - 1
                                ),
                            updated_at = ?
                        WHERE
                            worker_id = ?
                            AND generation = ?
                        """,
                        (
                            now,
                            str(row["worker_id"]),
                            row[
                                "worker_generation"
                            ],
                        ),
                    )

                connection.execute(
                    """
                    UPDATE execution_partitions
                    SET
                        active_work =
                            MAX(
                                0,
                                active_work - 1
                            ),
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        now,
                        int(row["partition_id"]),
                    ),
                )

                self._history(
                    connection,
                    str(row["work_id"]),
                    int(row["partition_id"]),
                    row["worker_id"],
                    row["worker_generation"],
                    "lease_recovered",
                )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "lease_recoveries"
        ] += recovered

        return recovered

    # ------------------------------------------------------------------
    # Work inspection
    # ------------------------------------------------------------------

    def get_work(
        self,
        work_id: str,
    ) -> Optional[ExecutionWork]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM execution_work
                WHERE work_id = ?
                """,
                (work_id,),
            ).fetchone()

        finally:
            connection.close()

        if row is None:
            return None

        return self._work_from_row(
            row
        )

    @staticmethod
    def _work_from_row(
        row: sqlite3.Row,
    ) -> ExecutionWork:
        return ExecutionWork(
            work_id=str(
                row["work_id"]
            ),
            partition_id=int(
                row["partition_id"]
            ),
            workload_type=str(
                row["workload_type"]
            ),
            payload_hash=str(
                row["payload_hash"]
            ),
            priority=float(
                row["priority"]
            ),
            created_at=float(
                row["created_at"]
            ),
            available_at=float(
                row["available_at"]
            ),
            status=str(
                row["status"]
            ),
            attempts=int(
                row["attempts"]
            ),
            worker_id=row[
                "worker_id"
            ],
            worker_generation=row[
                "worker_generation"
            ],
            fencing_token=row[
                "fencing_token"
            ],
            lease_until=row[
                "lease_until"
            ],
            completed_at=row[
                "completed_at"
            ],
            failed_at=row[
                "failed_at"
            ],
            next_attempt_at=row[
                "next_attempt_at"
            ],
            last_error=row[
                "last_error"
            ],
        )

    def list_work(
        self,
        status: Optional[str] = None,
        partition_id: Optional[int] = None,
        worker_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[ExecutionWork]:
        if limit <= 0:
            return []

        connection = self._connect()

        try:
            query = """
                SELECT *
                FROM execution_work
                WHERE 1 = 1
            """

            params: list[Any] = []

            if status is not None:
                query += """
                    AND status = ?
                """
                params.append(status)

            if partition_id is not None:
                query += """
                    AND partition_id = ?
                """
                params.append(
                    int(partition_id)
                )

            if worker_id is not None:
                query += """
                    AND worker_id = ?
                """
                params.append(worker_id)

            query += """
                ORDER BY
                    priority DESC,
                    created_at ASC,
                    work_id ASC
                LIMIT ?
            """

            params.append(limit)

            rows = connection.execute(
                query,
                tuple(params),
            ).fetchall()

        finally:
            connection.close()

        return [
            self._work_from_row(row)
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Durable history
    # ------------------------------------------------------------------

    def _history(
        self,
        connection: sqlite3.Connection,
        work_id: Optional[str],
        partition_id: Optional[int],
        worker_id: Optional[str],
        worker_generation: Optional[int],
        event: str,
        metadata: Optional[
            dict[str, Any]
        ] = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO execution_history (
                work_id,
                partition_id,
                worker_id,
                worker_generation,
                event,
                timestamp,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                work_id,
                partition_id,
                worker_id,
                worker_generation,
                event,
                time.time(),
                json.dumps(
                    metadata or {},
                    sort_keys=True,
                    default=str,
                ),
            ),
        )

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def checkpoint(
        self,
        controller_id: str = "global",
        partition_cursor: int = 0,
        workload_cursor: Optional[str] = None,
    ) -> ExecutionCheckpoint:
        checkpoint = ExecutionCheckpoint(
            controller_id=controller_id,
            epoch=self._epoch,
            partition_cursor=int(
                partition_cursor
            ),
            workload_cursor=(
                workload_cursor
            ),
            updated_at=time.time(),
        )

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO execution_checkpoints (
                    controller_id,
                    epoch,
                    partition_cursor,
                    workload_cursor,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(controller_id)
                DO UPDATE SET
                    epoch =
                        excluded.epoch,
                    partition_cursor =
                        excluded.partition_cursor,
                    workload_cursor =
                        excluded.workload_cursor,
                    updated_at =
                        excluded.updated_at
                """,
                (
                    checkpoint.controller_id,
                    checkpoint.epoch,
                    checkpoint.partition_cursor,
                    checkpoint.workload_cursor,
                    checkpoint.updated_at,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self._last_checkpoint_at = (
            checkpoint.updated_at
        )

        self.stats_data[
            "checkpoints"
        ] += 1

        return checkpoint

    def read_checkpoint(
        self,
        controller_id: str = "global",
    ) -> Optional[
        ExecutionCheckpoint
    ]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM execution_checkpoints
                WHERE controller_id = ?
                """,
                (controller_id,),
            ).fetchone()

        finally:
            connection.close()

        if row is None:
            return None

        return ExecutionCheckpoint(
            controller_id=str(
                row["controller_id"]
            ),
            epoch=int(
                row["epoch"]
            ),
            partition_cursor=int(
                row["partition_cursor"]
            ),
            workload_cursor=(
                row["workload_cursor"]
            ),
            updated_at=float(
                row["updated_at"]
            ),
        )

    # ------------------------------------------------------------------
    # Rebalancing primitives
    # ------------------------------------------------------------------

    def healthy_workers(
        self,
    ) -> list[ExecutionWorker]:
        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT *
                FROM execution_workers
                WHERE state = 'active'
                ORDER BY
                    active_work ASC,
                    worker_id ASC
                """
            ).fetchall()

        finally:
            connection.close()

        return [
            ExecutionWorker(
                worker_id=str(
                    row["worker_id"]
                ),
                generation=int(
                    row["generation"]
                ),
                state=str(
                    row["state"]
                ),
                capacity=int(
                    row["capacity"]
                ),
                active_work=int(
                    row["active_work"]
                ),
                registered_at=float(
                    row["registered_at"]
                ),
                last_heartbeat=float(
                    row["last_heartbeat"]
                ),
                heartbeat_deadline=float(
                    row["heartbeat_deadline"]
                ),
                fencing_epoch=int(
                    row["fencing_epoch"]
                ),
                region=row["region"],
                zone=row["zone"],
                endpoint=row["endpoint"],
            )
            for row in rows
        ]

    def overloaded_partitions(
        self,
        limit: int = 100,
    ) -> list[ExecutionPartition]:
        if limit <= 0:
            return []

        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT *
                FROM execution_partitions
                WHERE
                    active_work > 0
                ORDER BY
                    active_work DESC,
                    workload_count DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        finally:
            connection.close()

        return [
            ExecutionPartition(
                partition_id=int(
                    row["partition_id"]
                ),
                namespace=str(
                    row["namespace"]
                ),
                state=str(
                    row["state"]
                ),
                owner_id=row["owner_id"],
                owner_generation=(
                    row["owner_generation"]
                ),
                fencing_epoch=int(
                    row["fencing_epoch"]
                ),
                workload_count=int(
                    row["workload_count"]
                ),
                active_work=int(
                    row["active_work"]
                ),
                completed_work=int(
                    row["completed_work"]
                ),
                failed_work=int(
                    row["failed_work"]
                ),
                last_scheduled_at=(
                    row["last_scheduled_at"]
                ),
                updated_at=(
                    row["updated_at"]
                ),
            )
            for row in rows
        ]

    def suggest_partition_worker(
        self,
        partition_id: int,
    ) -> Optional[str]:
        workers = self.healthy_workers()

        if not workers:
            return None

        partition = self.get_partition(
            partition_id
        )

        if (
            partition is not None
            and partition.owner_id is not None
        ):
            owner = self.get_worker(
                partition.owner_id
            )

            if (
                owner is not None
                and owner.state == "active"
                and owner.generation
                == partition.owner_generation
                and owner.active_work
                < owner.capacity
            ):
                return owner.worker_id

        eligible = [
            worker
            for worker in workers
            if worker.active_work
            < worker.capacity
        ]

        if not eligible:
            return None

        return min(
            eligible,
            key=lambda worker: (
                worker.active_work
                / max(
                    worker.capacity,
                    1,
                ),
                worker.worker_id,
            ),
        ).worker_id

    # ------------------------------------------------------------------
    # Execution cycle
    # ------------------------------------------------------------------

    def cycle(
        self,
        partition_ids: Optional[
            Iterable[int]
        ] = None,
        checkpoint: bool = True,
    ) -> ExecutionCycleResult:
        epoch = (
            self._epoch
            or self.begin_epoch()
        )

        expired_workers = (
            self.expire_workers()
        )

        recovered = (
            self.recover_expired_leases()
        )

        if partition_ids is None:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT partition_id
                    FROM execution_partitions
                    ORDER BY partition_id ASC
                    LIMIT 10000
                    """
                ).fetchall()

            finally:
                connection.close()

            observed = [
                int(row[0])
                for row in rows
            ]

        else:
            observed = list(
                dict.fromkeys(
                    int(item)
                    for item in partition_ids
                )
            )

        for partition_id in observed:
            try:
                self.ensure_partition(
                    partition_id
                )

                worker_id = (
                    self.suggest_partition_worker(
                        partition_id
                    )
                )

                if worker_id is not None:
                    worker = self.get_worker(
                        worker_id
                    )

                    if worker is not None:
                        self.assign_partition(
                            partition_id,
                            worker.worker_id,
                            worker.generation,
                        )

            except Exception:
                self.stats_data[
                    "runtime_errors"
                ] += 1

        if checkpoint:
            self.checkpoint(
                controller_id="global",
                partition_cursor=(
                    observed[-1]
                    if observed
                    else 0
                ),
            )

        now = time.time()

        connection = self._connect()

        try:
            connection.execute(
                """
                UPDATE execution_epochs
                SET
                    completed_at = ?,
                    recovered_leases = ?,
                    expired_workers = ?,
                    partitions_observed = ?
                WHERE epoch = ?
                """,
                (
                    now,
                    recovered,
                    expired_workers,
                    len(observed),
                    epoch,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "cycles"
        ] += 1

        return ExecutionCycleResult(
            epoch=epoch,
            recovered_leases=recovered,
            expired_workers=expired_workers,
            partitions_observed=len(
                observed
            ),
            queued_work=self.count(
                "queued"
            ),
            processing_work=self.count(
                "processing"
            ),
            completed_work=self.count(
                "completed"
            ),
            failed_work=self.count(
                "failed"
            ),
        )

    # ------------------------------------------------------------------
    # Continuous execution control
    # ------------------------------------------------------------------

    def run(
        self,
        interval: Optional[float] = None,
    ) -> None:
        if interval is None:
            interval = self.checkpoint_interval

        if interval <= 0:
            raise ValueError(
                "interval must be positive"
            )

        with self._lock:
            if self._running:
                return

            self._running = True

        try:
            while self._running:
                try:
                    self.cycle()
                except Exception:
                    self.stats_data[
                        "runtime_errors"
                    ] += 1

                time.sleep(interval)

        finally:
            with self._lock:
                self._running = False

    def stop(self) -> None:
        with self._lock:
            self._running = False

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    # ------------------------------------------------------------------
    # Counts
    # ------------------------------------------------------------------

    def count(
        self,
        status: Optional[str] = None,
    ) -> int:
        connection = self._connect()

        try:
            if status is None:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM execution_work
                    """
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM execution_work
                    WHERE status = ?
                    """,
                    (status,),
                ).fetchone()

            return int(row[0])

        finally:
            connection.close()

    def worker_count(
        self,
        state: Optional[str] = None,
    ) -> int:
        connection = self._connect()

        try:
            if state is None:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM execution_workers
                    """
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM execution_workers
                    WHERE state = ?
                    """,
                    (state,),
                ).fetchone()

            return int(row[0])

        finally:
            connection.close()

    def partition_count_active(self) -> int:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM execution_partitions
                WHERE state = 'active'
                """
            ).fetchone()

            return int(row[0])

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Final status
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        capacity = self.capacity()

        return {
            **self.stats_data,
            "epoch": self._epoch,
            "partition_namespace_size": (
                self.partition_count
            ),
            "workers_total": (
                self.worker_count()
            ),
            "workers_active": (
                self.worker_count("active")
            ),
            "workers_expired": (
                self.worker_count("expired")
            ),
            "workers_fenced": (
                self.worker_count("fenced")
            ),
            "partitions_active": (
                self.partition_count_active()
            ),
            "work_total": self.count(),
            "work_queued": self.count(
                "queued"
            ),
            "work_processing": self.count(
                "processing"
            ),
            "work_completed": self.count(
                "completed"
            ),
            "work_failed": self.count(
                "failed"
            ),
            "capacity": asdict(
                capacity
            ),
            "last_checkpoint_at": (
                self._last_checkpoint_at
            ),
            "enormous_scale_target": True,
            "billions_to_trillions_target": True,
            "fixed_worker_limit": False,
            "fixed_global_execution_limit": False,
        }


__all__ = [
    "VERSION",
    "ExecutionPartition",
    "ExecutionWorker",
    "ExecutionWork",
    "ExecutionLease",
    "ExecutionCheckpoint",
    "ExecutionCapacity",
    "ExecutionCycleResult",
    "GlobalDistributedExecutionFabric",
]
