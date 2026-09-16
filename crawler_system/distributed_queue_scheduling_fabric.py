from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Optional


class DistributedSchedulingError(RuntimeError):
    """Base error for the distributed scheduling fabric."""


@dataclass(frozen=True)
class SchedulingWorkItem:
    work_id: str
    partition_id: int
    shard_id: Optional[str]
    hostname: str
    url: str
    source: str
    priority: float
    available_at: float
    deadline: Optional[float]
    attempt: int
    metadata: dict[str, Any]


@dataclass(frozen=True)
class SchedulingLease:
    work_id: str
    scheduler_id: str
    worker_id: str
    generation: int
    fencing_epoch: int
    leased_at: float
    lease_until: float


@dataclass(frozen=True)
class QueueState:
    queue_id: str
    partition_id: int
    priority: float
    queued: int
    processing: int
    completed: int
    failed: int
    oldest_available_at: Optional[float]
    scheduling_epoch: int


@dataclass(frozen=True)
class SchedulingAssignment:
    assignment_id: str
    work_id: str
    scheduler_id: str
    worker_id: str
    partition_id: int
    shard_id: Optional[str]
    generation: int
    fencing_epoch: int
    assigned_at: float
    lease_until: float
    state: str


@dataclass(frozen=True)
class SchedulingDecision:
    decision_id: str
    scheduler_id: str
    work_id: str
    partition_id: int
    worker_id: str
    shard_id: Optional[str]
    score: float
    reason: str
    created_at: float


@dataclass(frozen=True)
class SchedulingCapacity:
    queues: int
    queued_work: int
    processing_work: int
    completed_work: int
    failed_work: int
    active_schedulers: int
    active_assignments: int
    expired_leases: int
    ready_partitions: int


class DistributedQueueSchedulingFabric:
    """
    Distributed queue and scheduling control plane.

    This brick sits between the durable discovery/work fabric and the
    enormous worker fleet.

    Logical architecture:

        Work Queue
             ↓
        Fair Scheduler
             ↓
        Partition Scheduler
             ↓
        Shard Scheduler
             ↓
        Worker Selection
             ↓
        Fenced Assignment
             ↓
        Worker Fleet

    Design goals:

      * enormous distributed work queues
      * deterministic queue identity
      * partition-aware scheduling
      * shard-aware scheduling
      * worker-aware scheduling
      * priority scheduling
      * aging/fairness
      * deadline awareness
      * source fairness
      * backpressure
      * durable leases
      * generation/fencing protection
      * retry and exponential backoff
      * scheduler failover
      * expired lease recovery
      * durable scheduling checkpoints
      * scheduling epochs
      * hot-partition protection
      * starvation prevention
      * no fixed small-Web execution ceiling

    SQLite is only a durable local/control-plane implementation here.
    The logical design intentionally separates queue identity, scheduling
    state, ownership, placement, and worker assignment so the system can
    later be distributed across many metadata/queue partitions.
    """

    VERSION = "distributed-queue-scheduling-fabric.v1"

    DEFAULT_LEASE_SECONDS = 300.0
    DEFAULT_MAX_ATTEMPTS = 8
    DEFAULT_MAX_BATCH = 10000
    DEFAULT_AGING_RATE = 0.05
    DEFAULT_HOT_PARTITION_THRESHOLD = 10000

    def __init__(
        self,
        storage_root: str,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        max_batch_size: int = DEFAULT_MAX_BATCH,
        aging_rate: float = DEFAULT_AGING_RATE,
        hot_partition_threshold: int =
            DEFAULT_HOT_PARTITION_THRESHOLD,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be > 0"
            )

        if max_attempts < 1:
            raise ValueError(
                "max_attempts must be >= 1"
            )

        if max_batch_size < 1:
            raise ValueError(
                "max_batch_size must be >= 1"
            )

        if aging_rate < 0:
            raise ValueError(
                "aging_rate must be >= 0"
            )

        if hot_partition_threshold < 1:
            raise ValueError(
                "hot_partition_threshold must be >= 1"
            )

        self.storage_root = storage_root
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.max_batch_size = max_batch_size
        self.aging_rate = aging_rate
        self.hot_partition_threshold = (
            hot_partition_threshold
        )

        self._lock = threading.RLock()
        self._running = False
        self._stop_event = threading.Event()

        self._db_path = (
            f"{storage_root.rstrip('/')}/"
            "distributed_queue_scheduling_fabric.db"
        )

        self._initialize_database()

    # ================================================================
    # Database
    # ================================================================

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._db_path,
            timeout=30.0,
        )

        connection.row_factory = sqlite3.Row

        connection.execute(
            "PRAGMA journal_mode=WAL"
        )
        connection.execute(
            "PRAGMA synchronous=NORMAL"
        )

        return connection

    def _initialize_database(self) -> None:
        connection = self._connect()

        try:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS scheduling_queues (
                    queue_id TEXT PRIMARY KEY,
                    partition_id INTEGER NOT NULL,
                    priority REAL NOT NULL DEFAULT 50.0,
                    source TEXT NOT NULL DEFAULT '',
                    state TEXT NOT NULL DEFAULT 'active',
                    scheduling_epoch INTEGER NOT NULL DEFAULT 1,
                    fairness_cursor INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_queues_partition
                ON scheduling_queues(partition_id);

                CREATE INDEX IF NOT EXISTS
                    idx_sched_queues_priority
                ON scheduling_queues(priority DESC);

                CREATE INDEX IF NOT EXISTS
                    idx_sched_queues_state
                ON scheduling_queues(state);


                CREATE TABLE IF NOT EXISTS scheduling_work (
                    work_id TEXT PRIMARY KEY,
                    queue_id TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    shard_id TEXT,
                    hostname TEXT NOT NULL,
                    url TEXT NOT NULL,
                    source TEXT NOT NULL,
                    priority REAL NOT NULL DEFAULT 50.0,
                    available_at REAL NOT NULL,
                    deadline REAL,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    metadata_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    leased_at REAL,
                    lease_until REAL,
                    scheduler_id TEXT,
                    worker_id TEXT,
                    generation INTEGER,
                    fencing_epoch INTEGER,
                    completed_at REAL,
                    failed_at REAL,
                    last_error TEXT
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_work_ready
                ON scheduling_work(
                    status,
                    available_at,
                    priority DESC
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_work_partition
                ON scheduling_work(
                    partition_id,
                    status,
                    available_at
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_work_queue
                ON scheduling_work(
                    queue_id,
                    status,
                    available_at
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_work_worker
                ON scheduling_work(
                    worker_id,
                    status
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_work_lease
                ON scheduling_work(
                    lease_until
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_work_deadline
                ON scheduling_work(
                    deadline
                );


                CREATE TABLE IF NOT EXISTS scheduling_leases (
                    work_id TEXT PRIMARY KEY,
                    scheduler_id TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    fencing_epoch INTEGER NOT NULL,
                    leased_at REAL NOT NULL,
                    lease_until REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_leases_expiry
                ON scheduling_leases(lease_until);

                CREATE INDEX IF NOT EXISTS
                    idx_sched_leases_worker
                ON scheduling_leases(worker_id);


                CREATE TABLE IF NOT EXISTS scheduling_assignments (
                    assignment_id TEXT PRIMARY KEY,
                    work_id TEXT NOT NULL,
                    scheduler_id TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    shard_id TEXT,
                    generation INTEGER NOT NULL,
                    fencing_epoch INTEGER NOT NULL,
                    assigned_at REAL NOT NULL,
                    lease_until REAL NOT NULL,
                    state TEXT NOT NULL DEFAULT 'active',
                    completed_at REAL,
                    failed_at REAL,
                    error TEXT
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_assignments_worker
                ON scheduling_assignments(worker_id, state);

                CREATE INDEX IF NOT EXISTS
                    idx_sched_assignments_partition
                ON scheduling_assignments(partition_id, state);

                CREATE INDEX IF NOT EXISTS
                    idx_sched_assignments_state
                ON scheduling_assignments(state);


                CREATE TABLE IF NOT EXISTS scheduling_decisions (
                    decision_id TEXT PRIMARY KEY,
                    scheduler_id TEXT NOT NULL,
                    work_id TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    worker_id TEXT NOT NULL,
                    shard_id TEXT,
                    score REAL NOT NULL,
                    reason TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_decisions_work
                ON scheduling_decisions(work_id);

                CREATE INDEX IF NOT EXISTS
                    idx_sched_decisions_scheduler
                ON scheduling_decisions(scheduler_id);


                CREATE TABLE IF NOT EXISTS scheduler_nodes (
                    scheduler_id TEXT PRIMARY KEY,
                    generation INTEGER NOT NULL,
                    fencing_epoch INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'active',
                    capacity INTEGER NOT NULL DEFAULT 1,
                    active_assignments INTEGER NOT NULL DEFAULT 0,
                    started_at REAL NOT NULL,
                    last_heartbeat REAL NOT NULL,
                    heartbeat_deadline REAL NOT NULL,
                    scheduling_epoch INTEGER NOT NULL DEFAULT 1
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_nodes_state
                ON scheduler_nodes(state);

                CREATE INDEX IF NOT EXISTS
                    idx_sched_nodes_heartbeat
                ON scheduler_nodes(heartbeat_deadline);


                CREATE TABLE IF NOT EXISTS source_fairness (
                    source TEXT PRIMARY KEY,
                    priority REAL NOT NULL DEFAULT 50.0,
                    queued INTEGER NOT NULL DEFAULT 0,
                    active INTEGER NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    failed INTEGER NOT NULL DEFAULT 0,
                    fairness_cursor INTEGER NOT NULL DEFAULT 0,
                    last_scheduled_at REAL,
                    next_eligible_at REAL NOT NULL DEFAULT 0.0,
                    backoff_until REAL NOT NULL DEFAULT 0.0,
                    updated_at REAL NOT NULL
                );


                CREATE TABLE IF NOT EXISTS partition_fairness (
                    partition_id INTEGER PRIMARY KEY,
                    priority REAL NOT NULL DEFAULT 50.0,
                    queued INTEGER NOT NULL DEFAULT 0,
                    active INTEGER NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    failed INTEGER NOT NULL DEFAULT 0,
                    hotness REAL NOT NULL DEFAULT 0.0,
                    age_score REAL NOT NULL DEFAULT 0.0,
                    fairness_cursor INTEGER NOT NULL DEFAULT 0,
                    last_scheduled_at REAL,
                    next_eligible_at REAL NOT NULL DEFAULT 0.0,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_partition_ready
                ON partition_fairness(
                    next_eligible_at,
                    priority DESC,
                    age_score DESC
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_partition_hot
                ON partition_fairness(hotness DESC);


                CREATE TABLE IF NOT EXISTS scheduling_checkpoints (
                    scheduler_id TEXT PRIMARY KEY,
                    scheduling_epoch INTEGER NOT NULL,
                    queue_cursor TEXT,
                    partition_cursor TEXT,
                    source_cursor TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );


                CREATE TABLE IF NOT EXISTS scheduling_epochs (
                    epoch INTEGER PRIMARY KEY,
                    reason TEXT NOT NULL,
                    created_at REAL NOT NULL
                );


                CREATE TABLE IF NOT EXISTS scheduling_events (
                    event_id TEXT PRIMARY KEY,
                    scheduler_id TEXT,
                    work_id TEXT,
                    partition_id INTEGER,
                    worker_id TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT,
                    created_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_events_created
                ON scheduling_events(created_at);

                CREATE INDEX IF NOT EXISTS
                    idx_sched_events_partition
                ON scheduling_events(partition_id);


                CREATE TABLE IF NOT EXISTS retry_backoff (
                    work_id TEXT PRIMARY KEY,
                    attempt INTEGER NOT NULL,
                    retry_at REAL NOT NULL,
                    last_error TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_retry_backoff_ready
                ON retry_backoff(retry_at);


                CREATE TABLE IF NOT EXISTS scheduler_partition_claims (
                    partition_id INTEGER PRIMARY KEY,
                    scheduler_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    fencing_epoch INTEGER NOT NULL,
                    lease_until REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_sched_partition_claim_expiry
                ON scheduler_partition_claims(lease_until);
                """
            )

            row = connection.execute(
                """
                SELECT MAX(epoch)
                FROM scheduling_epochs
                """
            ).fetchone()

            if row is None or row[0] is None:
                connection.execute(
                    """
                    INSERT INTO scheduling_epochs(
                        epoch,
                        reason,
                        created_at
                    )
                    VALUES(1, 'initial', ?)
                    """,
                    (time.time(),),
                )

            connection.commit()

        finally:
            connection.close()

    # ================================================================
    # Stable identities
    # ================================================================

    @staticmethod
    def stable_hash(value: str) -> int:
        return int.from_bytes(
            hashlib.sha256(
                value.encode("utf-8")
            ).digest()[:16],
            "big",
        )

    @staticmethod
    def queue_id_for(
        partition_id: int,
        source: str,
    ) -> str:
        value = (
            f"{partition_id}:{source}"
        )

        return hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()

    @staticmethod
    def work_id_for(
        hostname: str,
        url: str,
        source: str,
    ) -> str:
        value = (
            f"{hostname}|{url}|{source}"
        )

        return hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()

    # ================================================================
    # Scheduler lifecycle
    # ================================================================

    def register_scheduler(
        self,
        scheduler_id: str,
        capacity: int = 1,
        heartbeat_timeout: float = 90.0,
    ) -> dict[str, Any]:
        if not scheduler_id:
            raise ValueError(
                "scheduler_id must not be empty"
            )

        if capacity < 1:
            raise ValueError(
                "capacity must be >= 1"
            )

        if heartbeat_timeout <= 0:
            raise ValueError(
                "heartbeat_timeout must be > 0"
            )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM scheduler_nodes
                    WHERE scheduler_id = ?
                    """,
                    (scheduler_id,),
                ).fetchone()

                if row is None:
                    generation = 1
                    fencing_epoch = 1
                    scheduling_epoch = (
                        self.current_scheduling_epoch(
                            connection
                        )
                    )

                    connection.execute(
                        """
                        INSERT INTO scheduler_nodes(
                            scheduler_id,
                            generation,
                            fencing_epoch,
                            state,
                            capacity,
                            active_assignments,
                            started_at,
                            last_heartbeat,
                            heartbeat_deadline,
                            scheduling_epoch
                        )
                        VALUES(
                            ?, ?, ?, 'active', ?, 0,
                            ?, ?, ?, ?
                        )
                        """,
                        (
                            scheduler_id,
                            generation,
                            fencing_epoch,
                            capacity,
                            now,
                            now,
                            now + heartbeat_timeout,
                            scheduling_epoch,
                        ),
                    )

                else:
                    generation = (
                        int(row["generation"]) + 1
                    )

                    fencing_epoch = (
                        int(row["fencing_epoch"]) + 1
                    )

                    scheduling_epoch = (
                        int(row["scheduling_epoch"])
                    )

                    connection.execute(
                        """
                        UPDATE scheduler_nodes
                        SET
                            generation = ?,
                            fencing_epoch = ?,
                            state = 'active',
                            capacity = ?,
                            last_heartbeat = ?,
                            heartbeat_deadline = ?
                        WHERE scheduler_id = ?
                        """,
                        (
                            generation,
                            fencing_epoch,
                            capacity,
                            now,
                            now + heartbeat_timeout,
                            scheduler_id,
                        ),
                    )

                self._event(
                    connection,
                    scheduler_id,
                    None,
                    None,
                    "scheduler_registered",
                    {
                        "generation": generation,
                        "fencing_epoch": fencing_epoch,
                    },
                )

                connection.commit()

                return {
                    "scheduler_id": scheduler_id,
                    "generation": generation,
                    "fencing_epoch": fencing_epoch,
                    "capacity": capacity,
                    "scheduling_epoch":
                        scheduling_epoch,
                }

            finally:
                connection.close()

    def heartbeat_scheduler(
        self,
        scheduler_id: str,
        generation: int,
        fencing_epoch: int,
        heartbeat_timeout: float = 90.0,
    ) -> bool:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE scheduler_nodes
                    SET
                        last_heartbeat = ?,
                        heartbeat_deadline = ?
                    WHERE scheduler_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND state = 'active'
                    """,
                    (
                        now,
                        now + heartbeat_timeout,
                        scheduler_id,
                        generation,
                        fencing_epoch,
                    ),
                )

                connection.commit()

                return cursor.rowcount > 0

            finally:
                connection.close()

    def fence_scheduler(
        self,
        scheduler_id: str,
        reason: str = "scheduler_fault",
    ) -> bool:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT fencing_epoch
                    FROM scheduler_nodes
                    WHERE scheduler_id = ?
                    """,
                    (scheduler_id,),
                ).fetchone()

                if row is None:
                    return False

                fencing_epoch = (
                    int(row["fencing_epoch"]) + 1
                )

                connection.execute(
                    """
                    UPDATE scheduler_nodes
                    SET
                        state = 'fenced',
                        fencing_epoch = ?
                    WHERE scheduler_id = ?
                    """,
                    (
                        fencing_epoch,
                        scheduler_id,
                    ),
                )

                self._event(
                    connection,
                    scheduler_id,
                    None,
                    None,
                    "scheduler_fenced",
                    {
                        "reason": reason,
                        "fencing_epoch":
                            fencing_epoch,
                    },
                )

                connection.commit()

                return True

            finally:
                connection.close()

    def expire_schedulers(
        self,
        now: Optional[float] = None,
    ) -> int:
        now = time.time() if now is None else now

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT scheduler_id
                    FROM scheduler_nodes
                    WHERE state = 'active'
                      AND heartbeat_deadline <= ?
                    """,
                    (now,),
                ).fetchall()

                for row in rows:
                    scheduler_id = row[
                        "scheduler_id"
                    ]

                    connection.execute(
                        """
                        UPDATE scheduler_nodes
                        SET state = 'expired',
                            fencing_epoch =
                                fencing_epoch + 1
                        WHERE scheduler_id = ?
                        """,
                        (scheduler_id,),
                    )

                    self._event(
                        connection,
                        scheduler_id,
                        None,
                        None,
                        "scheduler_expired",
                        {},
                    )

                connection.commit()

                return len(rows)

            finally:
                connection.close()

    # ================================================================
    # Scheduling epochs
    # ================================================================

    def current_scheduling_epoch(
        self,
        connection: Optional[sqlite3.Connection] = None,
    ) -> int:
        own_connection = connection is None

        if own_connection:
            connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT MAX(epoch)
                FROM scheduling_epochs
                """
            ).fetchone()

            return int(row[0] or 1)

        finally:
            if own_connection:
                connection.close()

    def begin_scheduling_epoch(
        self,
        reason: str,
    ) -> int:
        with self._lock:
            connection = self._connect()

            try:
                epoch = (
                    self.current_scheduling_epoch(
                        connection
                    ) + 1
                )

                connection.execute(
                    """
                    INSERT INTO scheduling_epochs(
                        epoch,
                        reason,
                        created_at
                    )
                    VALUES(?, ?, ?)
                    """,
                    (
                        epoch,
                        reason,
                        time.time(),
                    ),
                )

                connection.commit()

                return epoch

            finally:
                connection.close()

    # ================================================================
    # Queue registration / ingestion
    # ================================================================

    def ensure_queue(
        self,
        partition_id: int,
        source: str,
        priority: float = 50.0,
    ) -> str:
        queue_id = self.queue_id_for(
            partition_id,
            source,
        )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO scheduling_queues(
                        queue_id,
                        partition_id,
                        priority,
                        source,
                        state,
                        scheduling_epoch,
                        fairness_cursor,
                        created_at,
                        updated_at
                    )
                    VALUES(
                        ?, ?, ?, ?, 'active', 1, 0, ?, ?
                    )
                    """,
                    (
                        queue_id,
                        partition_id,
                        float(priority),
                        source,
                        now,
                        now,
                    ),
                )

                connection.execute(
                    """
                    INSERT OR IGNORE INTO source_fairness(
                        source,
                        priority,
                        updated_at
                    )
                    VALUES(?, ?, ?)
                    """,
                    (
                        source,
                        float(priority),
                        now,
                    ),
                )

                connection.execute(
                    """
                    INSERT OR IGNORE INTO partition_fairness(
                        partition_id,
                        priority,
                        updated_at
                    )
                    VALUES(?, ?, ?)
                    """,
                    (
                        partition_id,
                        float(priority),
                        now,
                    ),
                )

                connection.commit()

                return queue_id

            finally:
                connection.close()

    def enqueue(
        self,
        hostname: str,
        url: str,
        source: str,
        partition_id: int,
        priority: float = 50.0,
        available_at: Optional[float] = None,
        deadline: Optional[float] = None,
        metadata: Optional[dict[str, Any]] = None,
        max_attempts: Optional[int] = None,
        shard_id: Optional[str] = None,
    ) -> str:
        if not hostname:
            raise ValueError(
                "hostname must not be empty"
            )

        if not url:
            raise ValueError(
                "url must not be empty"
            )

        if not source:
            raise ValueError(
                "source must not be empty"
            )

        now = time.time()

        if available_at is None:
            available_at = now

        attempts_limit = (
            self.max_attempts
            if max_attempts is None
            else max_attempts
        )

        if attempts_limit < 1:
            raise ValueError(
                "max_attempts must be >= 1"
            )

        queue_id = self.ensure_queue(
            partition_id,
            source,
            priority,
        )

        work_id = self.work_id_for(
            hostname,
            url,
            source,
        )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO scheduling_work(
                        work_id,
                        queue_id,
                        partition_id,
                        shard_id,
                        hostname,
                        url,
                        source,
                        priority,
                        available_at,
                        deadline,
                        attempt,
                        max_attempts,
                        status,
                        metadata_json,
                        created_at,
                        updated_at
                    )
                    VALUES(
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        0, ?, 'queued', ?, ?, ?
                    )
                    """,
                    (
                        work_id,
                        queue_id,
                        partition_id,
                        shard_id,
                        hostname,
                        url,
                        source,
                        float(priority),
                        float(available_at),
                        deadline,
                        attempts_limit,
                        json.dumps(
                            metadata or {},
                            sort_keys=True,
                        ),
                        now,
                        now,
                    ),
                )

                if cursor.rowcount:
                    connection.execute(
                        """
                        UPDATE partition_fairness
                        SET queued = queued + 1,
                            priority = MAX(
                                priority,
                                ?
                            ),
                            updated_at = ?
                        WHERE partition_id = ?
                        """,
                        (
                            float(priority),
                            now,
                            partition_id,
                        ),
                    )

                    connection.execute(
                        """
                        UPDATE source_fairness
                        SET queued = queued + 1,
                            priority = MAX(
                                priority,
                                ?
                            ),
                            updated_at = ?
                        WHERE source = ?
                        """,
                        (
                            float(priority),
                            now,
                            source,
                        ),
                    )

                connection.commit()

                return work_id

            finally:
                connection.close()

    def enqueue_many(
        self,
        items: Iterable[dict[str, Any]],
    ) -> int:
        prepared = list(items)

        if not prepared:
            return 0

        count = 0

        for item in prepared:
            self.enqueue(
                hostname=item["hostname"],
                url=item["url"],
                source=item["source"],
                partition_id=item["partition_id"],
                priority=item.get(
                    "priority",
                    50.0,
                ),
                available_at=item.get(
                    "available_at"
                ),
                deadline=item.get(
                    "deadline"
                ),
                metadata=item.get(
                    "metadata"
                ),
                max_attempts=item.get(
                    "max_attempts"
                ),
                shard_id=item.get(
                    "shard_id"
                ),
            )

            count += 1

        return count

    # ================================================================
    # Scheduling score
    # ================================================================

    def _age_score(
        self,
        created_at: float,
        now: float,
    ) -> float:
        age = max(
            0.0,
            now - created_at,
        )

        return min(
            100.0,
            age * self.aging_rate,
        )

    def _deadline_score(
        self,
        deadline: Optional[float],
        now: float,
    ) -> float:
        if deadline is None:
            return 0.0

        remaining = (
            deadline - now
        )

        if remaining <= 0:
            return 1000.0

        if remaining < 60:
            return 500.0

        if remaining < 300:
            return 200.0

        if remaining < 3600:
            return 50.0

        return 0.0

    def _partition_score(
        self,
        row: sqlite3.Row,
        now: float,
    ) -> float:
        age = self._age_score(
            row["created_at"],
            now,
        )

        hotness = float(
            row["hotness"]
        )

        hot_penalty = (
            min(
                100.0,
                hotness * 25.0,
            )
            if hotness >= self.hot_partition_threshold
            else 0.0
        )

        return (
            float(row["priority"])
            + float(row["age_score"])
            + age
            - hot_penalty
        )

    def _work_score(
        self,
        row: sqlite3.Row,
        now: float,
    ) -> float:
        age = self._age_score(
            row["created_at"],
            now,
        )

        deadline = self._deadline_score(
            row["deadline"],
            now,
        )

        attempt_penalty = (
            float(row["attempt"]) * 2.0
        )

        return (
            float(row["priority"])
            + age
            + deadline
            - attempt_penalty
        )

    # ================================================================
    # Partition scheduling
    # ================================================================

    def claim_partition(
        self,
        partition_id: int,
        scheduler_id: str,
        generation: int,
        fencing_epoch: int,
        lease_seconds: Optional[float] = None,
    ) -> bool:
        lease_seconds = (
            self.lease_seconds
            if lease_seconds is None
            else lease_seconds
        )

        now = time.time()
        lease_until = (
            now + lease_seconds
        )

        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM scheduler_partition_claims
                    WHERE partition_id = ?
                    """,
                    (partition_id,),
                ).fetchone()

                if (
                    row is not None
                    and row["lease_until"] > now
                    and (
                        row["scheduler_id"]
                        != scheduler_id
                    )
                ):
                    return False

                connection.execute(
                    """
                    INSERT OR REPLACE INTO
                        scheduler_partition_claims(
                            partition_id,
                            scheduler_id,
                            generation,
                            fencing_epoch,
                            lease_until,
                            updated_at
                        )
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        partition_id,
                        scheduler_id,
                        generation,
                        fencing_epoch,
                        lease_until,
                        now,
                    ),
                )

                connection.commit()

                return True

            finally:
                connection.close()

    def renew_partition_claim(
        self,
        partition_id: int,
        scheduler_id: str,
        generation: int,
        fencing_epoch: int,
        lease_seconds: Optional[float] = None,
    ) -> bool:
        lease_seconds = (
            self.lease_seconds
            if lease_seconds is None
            else lease_seconds
        )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE scheduler_partition_claims
                    SET lease_until = ?,
                        updated_at = ?
                    WHERE partition_id = ?
                      AND scheduler_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND lease_until > ?
                    """,
                    (
                        now + lease_seconds,
                        now,
                        partition_id,
                        scheduler_id,
                        generation,
                        fencing_epoch,
                        now,
                    ),
                )

                connection.commit()

                return cursor.rowcount > 0

            finally:
                connection.close()

    def recover_partition_claims(
        self,
        now: Optional[float] = None,
    ) -> int:
        now = time.time() if now is None else now

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    DELETE FROM
                        scheduler_partition_claims
                    WHERE lease_until <= ?
                    """,
                    (now,),
                )

                connection.commit()

                return cursor.rowcount

            finally:
                connection.close()

    # ================================================================
    # Work selection
    # ================================================================

    def _select_ready_work(
        self,
        connection: sqlite3.Connection,
        limit: int,
        partition_id: Optional[int] = None,
        source: Optional[str] = None,
    ) -> list[sqlite3.Row]:
        now = time.time()

        clauses = [
            "status = 'queued'",
            "available_at <= ?",
        ]

        parameters: list[Any] = [
            now
        ]

        if partition_id is not None:
            clauses.append(
                "partition_id = ?"
            )
            parameters.append(
                partition_id
            )

        if source is not None:
            clauses.append(
                "source = ?"
            )
            parameters.append(
                source
            )

        parameters.append(
            max(
                1,
                min(
                    limit,
                    self.max_batch_size,
                ),
            )
        )

        query = f"""
            SELECT *
            FROM scheduling_work
            WHERE {" AND ".join(clauses)}
            ORDER BY
                priority DESC,
                deadline ASC,
                created_at ASC,
                work_id ASC
            LIMIT ?
        """

        return connection.execute(
            query,
            tuple(parameters),
        ).fetchall()

    def ready_work(
        self,
        limit: int = 100,
        partition_id: Optional[int] = None,
        source: Optional[str] = None,
    ) -> list[SchedulingWorkItem]:
        connection = self._connect()

        try:
            rows = self._select_ready_work(
                connection,
                limit,
                partition_id,
                source,
            )

            return [
                self._work_from_row(row)
                for row in rows
            ]

        finally:
            connection.close()

    # ================================================================
    # Worker selection
    # ================================================================

    def select_worker(
        self,
        work: SchedulingWorkItem,
        workers: Iterable[Any],
    ) -> Optional[Any]:
        candidates = list(workers)

        if not candidates:
            return None

        scored: list[tuple[float, Any]] = []

        for worker in candidates:
            state = getattr(
                worker,
                "state",
                "active",
            )

            if state != "active":
                continue

            capacity = float(
                getattr(
                    worker,
                    "capacity",
                    1.0,
                )
            )

            active_work = float(
                getattr(
                    worker,
                    "active_work",
                    0.0,
                )
            )

            if capacity <= active_work:
                continue

            worker_id = str(
                getattr(
                    worker,
                    "worker_id",
                    "",
                )
            )

            if not worker_id:
                continue

            region = str(
                getattr(
                    worker,
                    "region",
                    "",
                )
            )

            zone = str(
                getattr(
                    worker,
                    "zone",
                    "",
                )
            )

            endpoint = str(
                getattr(
                    worker,
                    "endpoint",
                    "",
                )
            )

            locality_bonus = 0.0

            if work.shard_id:
                if work.shard_id == worker_id:
                    locality_bonus += 100.0

            stable = self.stable_hash(
                f"{work.work_id}:"
                f"{worker_id}"
            )

            deterministic_bonus = (
                stable
                % 100000
            ) / 100000.0

            utilization = (
                active_work
                / max(
                    1.0,
                    capacity,
                )
            )

            load_penalty = (
                utilization * 50.0
            )

            score = (
                locality_bonus
                + deterministic_bonus
                - load_penalty
            )

            if region:
                score += 0.01

            if zone:
                score += 0.01

            if endpoint:
                score += 0.01

            scored.append(
                (score, worker)
            )

        if not scored:
            return None

        scored.sort(
            key=lambda item: item[0],
            reverse=True,
        )

        return scored[0][1]

    # ================================================================
    # Durable assignment
    # ================================================================

    def acquire_work(
        self,
        scheduler_id: str,
        worker_id: str,
        generation: int,
        fencing_epoch: int,
        workers: Optional[Iterable[Any]] = None,
        partition_id: Optional[int] = None,
        limit: int = 1,
    ) -> list[SchedulingAssignment]:
        if limit < 1:
            return []

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                scheduler = connection.execute(
                    """
                    SELECT *
                    FROM scheduler_nodes
                    WHERE scheduler_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND state = 'active'
                    """,
                    (
                        scheduler_id,
                        generation,
                        fencing_epoch,
                    ),
                ).fetchone()

                if scheduler is None:
                    raise DistributedSchedulingError(
                        "scheduler is not active"
                    )

                if (
                    int(
                        scheduler["active_assignments"]
                    )
                    >= int(
                        scheduler["capacity"]
                    )
                ):
                    return []

                available_scheduler_capacity = (
                    int(scheduler["capacity"])
                    - int(
                        scheduler[
                            "active_assignments"
                        ]
                    )
                )

                target = min(
                    limit,
                    available_scheduler_capacity,
                    self.max_batch_size,
                )

                rows = self._select_ready_work(
                    connection,
                    target,
                    partition_id,
                )

                assignments: list[
                    SchedulingAssignment
                ] = []

                for row in rows:
                    current = connection.execute(
                        """
                        SELECT status
                        FROM scheduling_work
                        WHERE work_id = ?
                        """,
                        (row["work_id"],),
                    ).fetchone()

                    if (
                        current is None
                        or current["status"] != "queued"
                    ):
                        continue

                    assignment_id = uuid.uuid4().hex
                    lease_until = (
                        now + self.lease_seconds
                    )

                    connection.execute(
                        """
                        UPDATE scheduling_work
                        SET
                            status = 'processing',
                            attempt = attempt + 1,
                            leased_at = ?,
                            lease_until = ?,
                            scheduler_id = ?,
                            worker_id = ?,
                            generation = ?,
                            fencing_epoch = ?,
                            updated_at = ?
                        WHERE work_id = ?
                          AND status = 'queued'
                        """,
                        (
                            now,
                            lease_until,
                            scheduler_id,
                            worker_id,
                            generation,
                            fencing_epoch,
                            now,
                            row["work_id"],
                        ),
                    )

                    connection.execute(
                        """
                        INSERT INTO scheduling_leases(
                            work_id,
                            scheduler_id,
                            worker_id,
                            generation,
                            fencing_epoch,
                            leased_at,
                            lease_until
                        )
                        VALUES(?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row["work_id"],
                            scheduler_id,
                            worker_id,
                            generation,
                            fencing_epoch,
                            now,
                            lease_until,
                        ),
                    )

                    connection.execute(
                        """
                        INSERT INTO scheduling_assignments(
                            assignment_id,
                            work_id,
                            scheduler_id,
                            worker_id,
                            partition_id,
                            shard_id,
                            generation,
                            fencing_epoch,
                            assigned_at,
                            lease_until,
                            state
                        )
                        VALUES(
                            ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, 'active'
                        )
                        """,
                        (
                            assignment_id,
                            row["work_id"],
                            scheduler_id,
                            worker_id,
                            row["partition_id"],
                            row["shard_id"],
                            generation,
                            fencing_epoch,
                            now,
                            lease_until,
                        ),
                    )

                    connection.execute(
                        """
                        UPDATE scheduler_nodes
                        SET active_assignments =
                            active_assignments + 1
                        WHERE scheduler_id = ?
                          AND generation = ?
                          AND fencing_epoch = ?
                        """,
                        (
                            scheduler_id,
                            generation,
                            fencing_epoch,
                        ),
                    )

                    connection.execute(
                        """
                        UPDATE partition_fairness
                        SET queued = MAX(0, queued - 1),
                            active = active + 1,
                            last_scheduled_at = ?,
                            fairness_cursor =
                                fairness_cursor + 1,
                            updated_at = ?
                        WHERE partition_id = ?
                        """,
                        (
                            now,
                            now,
                            row["partition_id"],
                        ),
                    )

                    connection.execute(
                        """
                        UPDATE source_fairness
                        SET queued = MAX(0, queued - 1),
                            active = active + 1,
                            last_scheduled_at = ?,
                            fairness_cursor =
                                fairness_cursor + 1,
                            updated_at = ?
                        WHERE source = ?
                        """,
                        (
                            now,
                            now,
                            row["source"],
                        ),
                    )

                    score = self._work_score(
                        row,
                        now,
                    )

                    decision_id = uuid.uuid4().hex

                    connection.execute(
                        """
                        INSERT INTO scheduling_decisions(
                            decision_id,
                            scheduler_id,
                            work_id,
                            partition_id,
                            worker_id,
                            shard_id,
                            score,
                            reason,
                            created_at
                        )
                        VALUES(
                            ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        (
                            decision_id,
                            scheduler_id,
                            row["work_id"],
                            row["partition_id"],
                            worker_id,
                            row["shard_id"],
                            score,
                            "priority-aging-deadline",
                            now,
                        ),
                    )

                    self._event(
                        connection,
                        scheduler_id,
                        row["work_id"],
                        row["partition_id"],
                        "work_assigned",
                        {
                            "worker_id": worker_id,
                            "score": score,
                        },
                    )

                    assignments.append(
                        SchedulingAssignment(
                            assignment_id=
                                assignment_id,
                            work_id=row["work_id"],
                            scheduler_id=
                                scheduler_id,
                            worker_id=worker_id,
                            partition_id=
                                row["partition_id"],
                            shard_id=row["shard_id"],
                            generation=generation,
                            fencing_epoch=
                                fencing_epoch,
                            assigned_at=now,
                            lease_until=
                                lease_until,
                            state="active",
                        )
                    )

                connection.commit()

                return assignments

            finally:
                connection.close()

    # ================================================================
    # Lease operations
    # ================================================================

    def renew_lease(
        self,
        lease: SchedulingLease,
        lease_seconds: Optional[float] = None,
    ) -> SchedulingLease:
        lease_seconds = (
            self.lease_seconds
            if lease_seconds is None
            else lease_seconds
        )

        now = time.time()
        lease_until = (
            now + lease_seconds
        )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE scheduling_leases
                    SET lease_until = ?
                    WHERE work_id = ?
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND lease_until > ?
                    """,
                    (
                        lease_until,
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                        now,
                    ),
                )

                if cursor.rowcount == 0:
                    raise DistributedSchedulingError(
                        "stale or expired scheduling lease"
                    )

                connection.execute(
                    """
                    UPDATE scheduling_work
                    SET lease_until = ?,
                        updated_at = ?
                    WHERE work_id = ?
                      AND status = 'processing'
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                    """,
                    (
                        lease_until,
                        now,
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                connection.execute(
                    """
                    UPDATE scheduling_assignments
                    SET lease_until = ?
                    WHERE work_id = ?
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND state = 'active'
                    """,
                    (
                        lease_until,
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                connection.commit()

                return SchedulingLease(
                    work_id=lease.work_id,
                    scheduler_id=lease.scheduler_id,
                    worker_id=lease.worker_id,
                    generation=lease.generation,
                    fencing_epoch=lease.fencing_epoch,
                    leased_at=lease.leased_at,
                    lease_until=lease_until,
                )

            finally:
                connection.close()

    # ================================================================
    # Completion
    # ================================================================

    def complete(
        self,
        lease: SchedulingLease,
    ) -> bool:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM scheduling_leases
                    WHERE work_id = ?
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                    """,
                    (
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                ).fetchone()

                if row is None:
                    return False

                if row["lease_until"] < now:
                    return False

                cursor = connection.execute(
                    """
                    UPDATE scheduling_work
                    SET status = 'completed',
                        completed_at = ?,
                        updated_at = ?
                    WHERE work_id = ?
                      AND status = 'processing'
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                    """,
                    (
                        now,
                        now,
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                if cursor.rowcount == 0:
                    return False

                work_row = connection.execute(
                    """
                    SELECT partition_id, source
                    FROM scheduling_work
                    WHERE work_id = ?
                    """,
                    (lease.work_id,),
                ).fetchone()

                if work_row is not None:
                    connection.execute(
                        """
                        UPDATE partition_fairness
                        SET active = MAX(0, active - 1),
                            completed = completed + 1,
                            updated_at = ?
                        WHERE partition_id = ?
                        """,
                        (
                            now,
                            work_row["partition_id"],
                        ),
                    )

                    connection.execute(
                        """
                        UPDATE source_fairness
                        SET active = MAX(0, active - 1),
                            completed = completed + 1,
                            updated_at = ?
                        WHERE source = ?
                        """,
                        (
                            now,
                            work_row["source"],
                        ),
                    )

                connection.execute(
                    """
                    UPDATE scheduling_assignments
                    SET state = 'completed',
                        completed_at = ?
                    WHERE work_id = ?
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND state = 'active'
                    """,
                    (
                        now,
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                connection.execute(
                    """
                    DELETE FROM scheduling_leases
                    WHERE work_id = ?
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                    """,
                    (
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                connection.execute(
                    """
                    UPDATE scheduler_nodes
                    SET active_assignments =
                        MAX(
                            0,
                            active_assignments - 1
                        )
                    WHERE scheduler_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                    """,
                    (
                        lease.scheduler_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                self._event(
                    connection,
                    lease.scheduler_id,
                    lease.work_id,
                    None,
                    "work_completed",
                    {
                        "worker_id":
                            lease.worker_id
                    },
                )

                connection.commit()

                return True

            finally:
                connection.close()

    # ================================================================
    # Failure / retry
    # ================================================================

    def fail(
        self,
        lease: SchedulingLease,
        error: str,
        retry_delay: Optional[float] = None,
    ) -> bool:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM scheduling_leases
                    WHERE work_id = ?
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                    """,
                    (
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                ).fetchone()

                if row is None:
                    return False

                if row["lease_until"] < now:
                    return False

                work = connection.execute(
                    """
                    SELECT *
                    FROM scheduling_work
                    WHERE work_id = ?
                      AND status = 'processing'
                    """,
                    (lease.work_id,),
                ).fetchone()

                if work is None:
                    return False

                next_attempt = (
                    int(work["attempt"])
                )

                exhausted = (
                    next_attempt
                    >= int(work["max_attempts"])
                )

                if retry_delay is None:
                    retry_delay = min(
                        3600.0,
                        5.0
                        * (
                            2 ** max(
                                0,
                                next_attempt - 1,
                            )
                        ),
                    )

                retry_at = (
                    now + retry_delay
                )

                if exhausted:
                    connection.execute(
                        """
                        UPDATE scheduling_work
                        SET status = 'failed',
                            failed_at = ?,
                            last_error = ?,
                            updated_at = ?
                        WHERE work_id = ?
                          AND status = 'processing'
                          AND scheduler_id = ?
                          AND worker_id = ?
                          AND generation = ?
                          AND fencing_epoch = ?
                        """,
                        (
                            now,
                            error,
                            now,
                            lease.work_id,
                            lease.scheduler_id,
                            lease.worker_id,
                            lease.generation,
                            lease.fencing_epoch,
                        ),
                    )

                    event_type = (
                        "work_failed_permanently"
                    )

                else:
                    connection.execute(
                        """
                        UPDATE scheduling_work
                        SET status = 'queued',
                            available_at = ?,
                            leased_at = NULL,
                            lease_until = NULL,
                            scheduler_id = NULL,
                            worker_id = NULL,
                            generation = NULL,
                            fencing_epoch = NULL,
                            last_error = ?,
                            updated_at = ?
                        WHERE work_id = ?
                          AND status = 'processing'
                          AND scheduler_id = ?
                          AND worker_id = ?
                          AND generation = ?
                          AND fencing_epoch = ?
                        """,
                        (
                            retry_at,
                            error,
                            now,
                            lease.work_id,
                            lease.scheduler_id,
                            lease.worker_id,
                            lease.generation,
                            lease.fencing_epoch,
                        ),
                    )

                    connection.execute(
                        """
                        INSERT OR REPLACE INTO retry_backoff(
                            work_id,
                            attempt,
                            retry_at,
                            last_error,
                            updated_at
                        )
                        VALUES(?, ?, ?, ?, ?)
                        """,
                        (
                            lease.work_id,
                            next_attempt,
                            retry_at,
                            error,
                            now,
                        ),
                    )

                    event_type = (
                        "work_failed_retry_scheduled"
                    )

                connection.execute(
                    """
                    DELETE FROM scheduling_leases
                    WHERE work_id = ?
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                    """,
                    (
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                connection.execute(
                    """
                    UPDATE scheduling_assignments
                    SET state = 'failed',
                        failed_at = ?,
                        error = ?
                    WHERE work_id = ?
                      AND scheduler_id = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND state = 'active'
                    """,
                    (
                        now,
                        error,
                        lease.work_id,
                        lease.scheduler_id,
                        lease.worker_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                connection.execute(
                    """
                    UPDATE scheduler_nodes
                    SET active_assignments =
                        MAX(
                            0,
                            active_assignments - 1
                        )
                    WHERE scheduler_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                    """,
                    (
                        lease.scheduler_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                connection.execute(
                    """
                    UPDATE partition_fairness
                    SET active = MAX(0, active - 1),
                        failed = failed + 1,
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        now,
                        work["partition_id"],
                    ),
                )

                connection.execute(
                    """
                    UPDATE source_fairness
                    SET active = MAX(0, active - 1),
                        failed = failed + 1,
                        updated_at = ?
                    WHERE source = ?
                    """,
                    (
                        now,
                        work["source"],
                    ),
                )

                self._event(
                    connection,
                    lease.scheduler_id,
                    lease.work_id,
                    work["partition_id"],
                    event_type,
                    {
                        "worker_id":
                            lease.worker_id,
                        "error": error,
                        "retry_at":
                            None
                            if exhausted
                            else retry_at,
                    },
                )

                connection.commit()

                return True

            finally:
                connection.close()

    # ================================================================
    # Expired work recovery
    # ================================================================

    def recover_expired_leases(
        self,
        now: Optional[float] = None,
    ) -> int:
        now = time.time() if now is None else now

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM scheduling_work
                    WHERE status = 'processing'
                      AND lease_until <= ?
                    """,
                    (now,),
                ).fetchall()

                recovered = 0

                for work in rows:
                    attempt = int(
                        work["attempt"]
                    )

                    exhausted = (
                        attempt
                        >= int(
                            work["max_attempts"]
                        )
                    )

                    if exhausted:
                        connection.execute(
                            """
                            UPDATE scheduling_work
                            SET status = 'failed',
                                failed_at = ?,
                                last_error =
                                    'expired lease',
                                updated_at = ?
                            WHERE work_id = ?
                              AND status = 'processing'
                            """,
                            (
                                now,
                                now,
                                work["work_id"],
                            ),
                        )

                        event_type = (
                            "expired_work_failed"
                        )

                    else:
                        retry_at = (
                            now
                            + min(
                                3600.0,
                                5.0
                                * (
                                    2
                                    ** max(
                                        0,
                                        attempt - 1,
                                    )
                                ),
                            )
                        )

                        connection.execute(
                            """
                            UPDATE scheduling_work
                            SET status = 'queued',
                                available_at = ?,
                                leased_at = NULL,
                                lease_until = NULL,
                                scheduler_id = NULL,
                                worker_id = NULL,
                                generation = NULL,
                                fencing_epoch = NULL,
                                last_error =
                                    'expired lease',
                                updated_at = ?
                            WHERE work_id = ?
                              AND status = 'processing'
                            """,
                            (
                                retry_at,
                                now,
                                work["work_id"],
                            ),
                        )

                        connection.execute(
                            """
                            INSERT OR REPLACE INTO
                                retry_backoff(
                                    work_id,
                                    attempt,
                                    retry_at,
                                    last_error,
                                    updated_at
                                )
                            VALUES(
                                ?, ?, ?,
                                'expired lease', ?
                            )
                            """,
                            (
                                work["work_id"],
                                attempt,
                                retry_at,
                                now,
                            ),
                        )

                        event_type = (
                            "expired_work_requeued"
                        )

                    connection.execute(
                        """
                        DELETE FROM scheduling_leases
                        WHERE work_id = ?
                        """,
                        (work["work_id"],),
                    )

                    connection.execute(
                        """
                        UPDATE scheduling_assignments
                        SET state = 'recovered',
                            failed_at = ?
                        WHERE work_id = ?
                          AND state = 'active'
                        """,
                        (
                            now,
                            work["work_id"],
                        ),
                    )

                    if work["scheduler_id"]:
                        connection.execute(
                            """
                            UPDATE scheduler_nodes
                            SET active_assignments =
                                MAX(
                                    0,
                                    active_assignments - 1
                                )
                            WHERE scheduler_id = ?
                            """,
                            (
                                work["scheduler_id"],
                            ),
                        )

                    connection.execute(
                        """
                        UPDATE partition_fairness
                        SET active =
                                MAX(0, active - 1),
                            updated_at = ?
                        WHERE partition_id = ?
                        """,
                        (
                            now,
                            work["partition_id"],
                        ),
                    )

                    connection.execute(
                        """
                        UPDATE source_fairness
                        SET active =
                                MAX(0, active - 1),
                            updated_at = ?
                        WHERE source = ?
                        """,
                        (
                            now,
                            work["source"],
                        ),
                    )

                    self._event(
                        connection,
                        work["scheduler_id"],
                        work["work_id"],
                        work["partition_id"],
                        event_type,
                        {},
                    )

                    recovered += 1

                connection.commit()

                return recovered

            finally:
                connection.close()

    # ================================================================
    # Fairness / hot partition state
    # ================================================================

    def refresh_fairness_state(self) -> None:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                partition_rows = connection.execute(
                    """
                    SELECT
                        partition_id,
                        COUNT(*) AS queued_count
                    FROM scheduling_work
                    WHERE status = 'queued'
                    GROUP BY partition_id
                    """
                ).fetchall()

                for row in partition_rows:
                    partition_id = int(
                        row["partition_id"]
                    )

                    queued_count = int(
                        row["queued_count"]
                    )

                    connection.execute(
                        """
                        UPDATE partition_fairness
                        SET
                            queued = ?,
                            hotness = ?,
                            age_score = MIN(
                                100.0,
                                age_score + ?
                            ),
                            updated_at = ?
                        WHERE partition_id = ?
                        """,
                        (
                            queued_count,
                            float(queued_count),
                            self.aging_rate,
                            now,
                            partition_id,
                        ),
                    )

                source_rows = connection.execute(
                    """
                    SELECT
                        source,
                        COUNT(*) AS queued_count
                    FROM scheduling_work
                    WHERE status = 'queued'
                    GROUP BY source
                    """
                ).fetchall()

                for row in source_rows:
                    connection.execute(
                        """
                        UPDATE source_fairness
                        SET queued = ?,
                            updated_at = ?
                        WHERE source = ?
                        """,
                        (
                            int(
                                row["queued_count"]
                            ),
                            now,
                            row["source"],
                        ),
                    )

                connection.commit()

            finally:
                connection.close()

    def hot_partitions(
        self,
        limit: int = 1000,
    ) -> list[int]:
        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT partition_id
                FROM partition_fairness
                WHERE hotness >= ?
                ORDER BY
                    hotness DESC,
                    priority DESC,
                    partition_id
                LIMIT ?
                """,
                (
                    float(
                        self.hot_partition_threshold
                    ),
                    limit,
                ),
            ).fetchall()

            return [
                int(row["partition_id"])
                for row in rows
            ]

        finally:
            connection.close()

    # ================================================================
    # Checkpoints
    # ================================================================

    def checkpoint(
        self,
        scheduler_id: str,
        queue_cursor: Optional[str] = None,
        partition_cursor: Optional[str] = None,
        source_cursor: Optional[str] = None,
    ) -> dict[str, Any]:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                epoch = (
                    self.current_scheduling_epoch(
                        connection
                    )
                )

                connection.execute(
                    """
                    INSERT OR REPLACE INTO
                        scheduling_checkpoints(
                            scheduler_id,
                            scheduling_epoch,
                            queue_cursor,
                            partition_cursor,
                            source_cursor,
                            created_at,
                            updated_at
                        )
                    VALUES(
                        ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        scheduler_id,
                        epoch,
                        queue_cursor,
                        partition_cursor,
                        source_cursor,
                        now,
                        now,
                    ),
                )

                connection.commit()

                return {
                    "scheduler_id":
                        scheduler_id,
                    "scheduling_epoch":
                        epoch,
                    "queue_cursor":
                        queue_cursor,
                    "partition_cursor":
                        partition_cursor,
                    "source_cursor":
                        source_cursor,
                    "updated_at":
                        now,
                }

            finally:
                connection.close()

    def read_checkpoint(
        self,
        scheduler_id: str,
    ) -> Optional[dict[str, Any]]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM scheduling_checkpoints
                WHERE scheduler_id = ?
                """,
                (scheduler_id,),
            ).fetchone()

            if row is None:
                return None

            return {
                "scheduler_id":
                    row["scheduler_id"],
                "scheduling_epoch":
                    row["scheduling_epoch"],
                "queue_cursor":
                    row["queue_cursor"],
                "partition_cursor":
                    row["partition_cursor"],
                "source_cursor":
                    row["source_cursor"],
                "created_at":
                    row["created_at"],
                "updated_at":
                    row["updated_at"],
            }

        finally:
            connection.close()

    # ================================================================
    # Event log
    # ================================================================

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        scheduler_id: Optional[str],
        work_id: Optional[str],
        partition_id: Optional[int],
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        connection.execute(
            """
            INSERT INTO scheduling_events(
                event_id,
                scheduler_id,
                work_id,
                partition_id,
                worker_id,
                event_type,
                payload_json,
                created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                scheduler_id,
                work_id,
                partition_id,
                payload.get("worker_id"),
                event_type,
                json.dumps(
                    payload,
                    sort_keys=True,
                ),
                time.time(),
            ),
        )

    def recent_events(
        self,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            return []

        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT *
                FROM scheduling_events
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            return [
                {
                    "event_id":
                        row["event_id"],
                    "scheduler_id":
                        row["scheduler_id"],
                    "work_id":
                        row["work_id"],
                    "partition_id":
                        row["partition_id"],
                    "worker_id":
                        row["worker_id"],
                    "event_type":
                        row["event_type"],
                    "payload":
                        json.loads(
                            row["payload_json"]
                            or "{}"
                        ),
                    "created_at":
                        row["created_at"],
                }
                for row in rows
            ]

        finally:
            connection.close()

    # ================================================================
    # Queue / assignment inspection
    # ================================================================

    def _work_from_row(
        self,
        row: sqlite3.Row,
    ) -> SchedulingWorkItem:
        return SchedulingWorkItem(
            work_id=row["work_id"],
            partition_id=row["partition_id"],
            shard_id=row["shard_id"],
            hostname=row["hostname"],
            url=row["url"],
            source=row["source"],
            priority=row["priority"],
            available_at=row["available_at"],
            deadline=row["deadline"],
            attempt=row["attempt"],
            metadata=json.loads(
                row["metadata_json"]
                or "{}"
            ),
        )

    def get_work(
        self,
        work_id: str,
    ) -> Optional[SchedulingWorkItem]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM scheduling_work
                WHERE work_id = ?
                """,
                (work_id,),
            ).fetchone()

            if row is None:
                return None

            return self._work_from_row(row)

        finally:
            connection.close()

    def queue_state(
        self,
        queue_id: str,
    ) -> Optional[QueueState]:
        connection = self._connect()

        try:
            queue = connection.execute(
                """
                SELECT *
                FROM scheduling_queues
                WHERE queue_id = ?
                """,
                (queue_id,),
            ).fetchone()

            if queue is None:
                return None

            counts = connection.execute(
                """
                SELECT
                    SUM(
                        CASE
                            WHEN status = 'queued'
                            THEN 1 ELSE 0
                        END
                    ) AS queued,
                    SUM(
                        CASE
                            WHEN status = 'processing'
                            THEN 1 ELSE 0
                        END
                    ) AS processing,
                    SUM(
                        CASE
                            WHEN status = 'completed'
                            THEN 1 ELSE 0
                        END
                    ) AS completed,
                    SUM(
                        CASE
                            WHEN status = 'failed'
                            THEN 1 ELSE 0
                        END
                    ) AS failed,
                    MIN(
                        CASE
                            WHEN status = 'queued'
                            THEN available_at
                        END
                    ) AS oldest_available_at
                FROM scheduling_work
                WHERE queue_id = ?
                """,
                (queue_id,),
            ).fetchone()

            return QueueState(
                queue_id=queue["queue_id"],
                partition_id=queue["partition_id"],
                priority=queue["priority"],
                queued=int(
                    counts["queued"] or 0
                ),
                processing=int(
                    counts["processing"] or 0
                ),
                completed=int(
                    counts["completed"] or 0
                ),
                failed=int(
                    counts["failed"] or 0
                ),
                oldest_available_at=
                    counts[
                        "oldest_available_at"
                    ],
                scheduling_epoch=
                    queue[
                        "scheduling_epoch"
                    ],
            )

        finally:
            connection.close()

    def active_assignments(
        self,
        worker_id: Optional[str] = None,
        partition_id: Optional[int] = None,
        limit: int = 10000,
    ) -> list[SchedulingAssignment]:
        connection = self._connect()

        try:
            clauses = [
                "state = 'active'"
            ]

            values: list[Any] = []

            if worker_id is not None:
                clauses.append(
                    "worker_id = ?"
                )
                values.append(
                    worker_id
                )

            if partition_id is not None:
                clauses.append(
                    "partition_id = ?"
                )
                values.append(
                    partition_id
                )

            values.append(
                max(1, limit)
            )

            rows = connection.execute(
                f"""
                SELECT *
                FROM scheduling_assignments
                WHERE {" AND ".join(clauses)}
                ORDER BY assigned_at ASC
                LIMIT ?
                """,
                tuple(values),
            ).fetchall()

            return [
                SchedulingAssignment(
                    assignment_id=
                        row["assignment_id"],
                    work_id=row["work_id"],
                    scheduler_id=
                        row["scheduler_id"],
                    worker_id=row["worker_id"],
                    partition_id=
                        row["partition_id"],
                    shard_id=row["shard_id"],
                    generation=
                        row["generation"],
                    fencing_epoch=
                        row["fencing_epoch"],
                    assigned_at=
                        row["assigned_at"],
                    lease_until=
                        row["lease_until"],
                    state=row["state"],
                )
                for row in rows
            ]

        finally:
            connection.close()

    # ================================================================
    # Capacity / statistics
    # ================================================================

    def capacity(self) -> SchedulingCapacity:
        connection = self._connect()

        try:
            queues = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM scheduling_queues
                    """
                ).fetchone()[0]
            )

            queued_work = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM scheduling_work
                    WHERE status = 'queued'
                    """
                ).fetchone()[0]
            )

            processing_work = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM scheduling_work
                    WHERE status = 'processing'
                    """
                ).fetchone()[0]
            )

            completed_work = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM scheduling_work
                    WHERE status = 'completed'
                    """
                ).fetchone()[0]
            )

            failed_work = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM scheduling_work
                    WHERE status = 'failed'
                    """
                ).fetchone()[0]
            )

            active_schedulers = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM scheduler_nodes
                    WHERE state = 'active'
                    """
                ).fetchone()[0]
            )

            active_assignments = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM scheduling_assignments
                    WHERE state = 'active'
                    """
                ).fetchone()[0]
            )

            expired_leases = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM scheduling_work
                    WHERE status = 'processing'
                      AND lease_until <= ?
                    """,
                    (time.time(),),
                ).fetchone()[0]
            )

            ready_partitions = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM partition_fairness
                    WHERE queued > 0
                      AND next_eligible_at <= ?
                    """,
                    (time.time(),),
                ).fetchone()[0]
            )

            return SchedulingCapacity(
                queues=queues,
                queued_work=queued_work,
                processing_work=
                    processing_work,
                completed_work=
                    completed_work,
                failed_work=failed_work,
                active_schedulers=
                    active_schedulers,
                active_assignments=
                    active_assignments,
                expired_leases=
                    expired_leases,
                ready_partitions=
                    ready_partitions,
            )

        finally:
            connection.close()

    def stats(self) -> dict[str, Any]:
        capacity = self.capacity()

        return {
            "version": self.VERSION,

            "lease_seconds":
                self.lease_seconds,

            "max_attempts":
                self.max_attempts,

            "max_batch_size":
                self.max_batch_size,

            "aging_rate":
                self.aging_rate,

            "hot_partition_threshold":
                self.hot_partition_threshold,

            "queues":
                capacity.queues,

            "queued_work":
                capacity.queued_work,

            "processing_work":
                capacity.processing_work,

            "completed_work":
                capacity.completed_work,

            "failed_work":
                capacity.failed_work,

            "active_schedulers":
                capacity.active_schedulers,

            "active_assignments":
                capacity.active_assignments,

            "expired_leases":
                capacity.expired_leases,

            "ready_partitions":
                capacity.ready_partitions,

            "scheduling_epoch":
                self.current_scheduling_epoch(),

            "fixed_global_execution_limit":
                False,

            "fixed_queue_count_limit":
                False,

            "fixed_partition_execution_limit":
                False,

            "elastic_queue_architecture":
                True,

            "priority_scheduling":
                True,

            "aging_fairness":
                True,

            "deadline_awareness":
                True,

            "source_fairness":
                True,

            "partition_fairness":
                True,

            "hot_partition_protection":
                True,

            "durable_leases":
                True,

            "generation_fencing":
                True,

            "retry_backoff":
                True,

            "expired_lease_recovery":
                True,

            "scheduler_failover":
                True,

            "durable_checkpoints":
                True,

            "scheduling_epochs":
                True,

            "partition_claims":
                True,

            "worker_aware_selection":
                True,

            "shard_aware_selection":
                True,

            "enormous_scale_target":
                True,

            "billions_to_trillions_target":
                True,
        }

    # ================================================================
    # Continuous scheduler supervision
    # ================================================================

    def tick(self) -> dict[str, Any]:
        expired_schedulers = (
            self.expire_schedulers()
        )

        recovered_leases = (
            self.recover_expired_leases()
        )

        recovered_partition_claims = (
            self.recover_partition_claims()
        )

        self.refresh_fairness_state()

        return {
            "expired_schedulers":
                expired_schedulers,
            "recovered_leases":
                recovered_leases,
            "recovered_partition_claims":
                recovered_partition_claims,
            "capacity":
                self.capacity(),
        }

    def run(
        self,
        interval: float = 5.0,
    ) -> None:
        if interval <= 0:
            raise ValueError(
                "interval must be > 0"
            )

        with self._lock:
            if self._running:
                return

            self._running = True
            self._stop_event.clear()

        try:
            while not self._stop_event.is_set():
                self.tick()
                self._stop_event.wait(interval)

        finally:
            with self._lock:
                self._running = False

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running
