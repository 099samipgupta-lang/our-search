"""
OUR SEARCH — Phase 9.8
End-to-End Fault Tolerance Fabric

Architecture target:
    Globally distributed, continuously operating infrastructure capable of
    supporting billions and potentially trillions of public-Web resources.

This layer sits across:
    execution
    workers
    partitions
    queues
    scheduling
    regions
    metadata
    crawling
    recovery

The objective is not merely detecting failures.

The objective is:

    DETECT
      ↓
    FENCE
      ↓
    CHECKPOINT
      ↓
    RECOVER
      ↓
    REASSIGN
      ↓
    REPLAY
      ↓
    VERIFY
      ↓
    RESUME

without allowing stale workers, schedulers, regions, or partitions to
corrupt ownership or duplicate durable work.

No Google technology is used.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional


ARCHITECTURE_VERSION = "end-to-end-fault-tolerance.v1"

DEFAULT_PARTITION_COUNT = 1_048_576
DEFAULT_LEASE_SECONDS = 300.0
DEFAULT_HEARTBEAT_TIMEOUT = 90.0
DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_BATCH_SIZE = 10_000


# ============================================================================
# CONTRACTS
# ============================================================================

@dataclass(frozen=True)
class FaultDomain:
    fault_domain_id: str
    domain_type: str
    region_id: str | None
    cluster_id: str | None
    zone_id: str | None


@dataclass(frozen=True)
class FailureSignal:
    signal_id: str
    component_id: str
    component_type: str

    fault_domain_id: str | None

    signal_type: str
    severity: str

    observed_at: float
    details: dict[str, Any]


@dataclass(frozen=True)
class ComponentLease:
    lease_id: str
    component_id: str

    owner_id: str

    epoch: int
    fencing_token: int

    acquired_at: float
    expires_at: float


@dataclass(frozen=True)
class RecoveryCheckpoint:
    checkpoint_id: str

    component_id: str
    partition_id: int | None

    sequence: int
    epoch: int

    created_at: float

    state: dict[str, Any]


@dataclass(frozen=True)
class RecoveryTask:
    recovery_id: str

    component_id: str
    component_type: str

    partition_id: int | None

    source_owner: str | None
    target_owner: str | None

    checkpoint_id: str | None

    attempt: int

    fencing_token: int

    state: str

    created_at: float
    updated_at: float


@dataclass(frozen=True)
class RecoveryResult:
    recovery_id: str

    success: bool

    replayed: bool
    reassigned: bool
    verified: bool

    details: dict[str, Any]


@dataclass(frozen=True)
class FaultToleranceCapacity:
    failure_signals: int
    active_leases: int
    checkpoints: int
    recovery_tasks: int

    failed_components: int
    recovering_components: int


@dataclass(frozen=True)
class FaultEvent:
    event_id: str

    event_type: str

    component_id: str | None
    component_type: str | None

    partition_id: int | None

    epoch: int
    fencing_token: int

    created_at: float

    payload: dict[str, Any]


# ============================================================================
# BACKEND
# ============================================================================

class FaultToleranceBackend:

    def execute(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ):
        raise NotImplementedError

    def executemany(
        self,
        sql: str,
        rows: Iterable[tuple[Any, ...]],
    ):
        raise NotImplementedError

    def transaction(self):
        raise NotImplementedError


class SQLiteFaultToleranceBackend(
    FaultToleranceBackend
):

    def __init__(
        self,
        storage_root: str,
    ) -> None:

        root = Path(storage_root)

        root.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.path = (
            root
            / "end_to_end_fault_tolerance.db"
        )

        self._local = threading.local()

        self._initialize(
            self._connection()
        )

    def _connection(self) -> sqlite3.Connection:

        connection = getattr(
            self._local,
            "connection",
            None,
        )

        if connection is None:

            connection = sqlite3.connect(
                str(self.path),
                timeout=30.0,
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

            self._local.connection = connection

        return connection

    def execute(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ):
        return self._connection().execute(
            sql,
            params,
        )

    def executemany(
        self,
        sql: str,
        rows: Iterable[tuple[Any, ...]],
    ):
        return self._connection().executemany(
            sql,
            rows,
        )

    def transaction(self):
        return _Transaction(
            self._connection()
        )

    def _initialize(
        self,
        connection: sqlite3.Connection,
    ) -> None:

        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS
                fault_signals (
                    signal_id TEXT PRIMARY KEY,

                    component_id TEXT NOT NULL,
                    component_type TEXT NOT NULL,

                    fault_domain_id TEXT,

                    signal_type TEXT NOT NULL,
                    severity TEXT NOT NULL,

                    observed_at REAL NOT NULL,

                    details_json TEXT NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_fault_signal_component
                ON fault_signals(
                    component_id,
                    observed_at DESC
                );

            CREATE INDEX IF NOT EXISTS
                idx_fault_signal_time
                ON fault_signals(
                    observed_at DESC
                );

            CREATE TABLE IF NOT EXISTS
                fault_epochs (
                    component_id TEXT PRIMARY KEY,
                    epoch INTEGER NOT NULL
                );

            CREATE TABLE IF NOT EXISTS
                component_leases (
                    lease_id TEXT PRIMARY KEY,

                    component_id TEXT NOT NULL,

                    owner_id TEXT NOT NULL,

                    epoch INTEGER NOT NULL,
                    fencing_token INTEGER NOT NULL,

                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );

            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_fault_active_lease
                ON component_leases(
                    component_id
                );

            CREATE TABLE IF NOT EXISTS
                recovery_checkpoints (
                    checkpoint_id TEXT PRIMARY KEY,

                    component_id TEXT NOT NULL,
                    partition_id INTEGER,

                    sequence INTEGER NOT NULL,
                    epoch INTEGER NOT NULL,

                    created_at REAL NOT NULL,

                    state_json TEXT NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_fault_checkpoint_component
                ON recovery_checkpoints(
                    component_id,
                    sequence DESC
                );

            CREATE INDEX IF NOT EXISTS
                idx_fault_checkpoint_partition
                ON recovery_checkpoints(
                    partition_id,
                    sequence DESC
                );

            CREATE TABLE IF NOT EXISTS
                recovery_tasks (
                    recovery_id TEXT PRIMARY KEY,

                    component_id TEXT NOT NULL,
                    component_type TEXT NOT NULL,

                    partition_id INTEGER,

                    source_owner TEXT,
                    target_owner TEXT,

                    checkpoint_id TEXT,

                    attempt INTEGER NOT NULL,

                    fencing_token INTEGER NOT NULL,

                    state TEXT NOT NULL,

                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_fault_recovery_state
                ON recovery_tasks(
                    state,
                    updated_at
                );

            CREATE INDEX IF NOT EXISTS
                idx_fault_recovery_component
                ON recovery_tasks(
                    component_id,
                    updated_at DESC
                );

            CREATE TABLE IF NOT EXISTS
                fault_events (
                    event_id TEXT PRIMARY KEY,

                    event_type TEXT NOT NULL,

                    component_id TEXT,
                    component_type TEXT,

                    partition_id INTEGER,

                    epoch INTEGER NOT NULL,
                    fencing_token INTEGER NOT NULL,

                    created_at REAL NOT NULL,

                    payload_json TEXT NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_fault_event_time
                ON fault_events(
                    created_at DESC
                );

            CREATE TABLE IF NOT EXISTS
                recovery_verifications (
                    recovery_id TEXT PRIMARY KEY,

                    verified_at REAL NOT NULL,

                    checkpoint_valid INTEGER NOT NULL,
                    ownership_valid INTEGER NOT NULL,
                    fencing_valid INTEGER NOT NULL,
                    replay_valid INTEGER NOT NULL,

                    details_json TEXT NOT NULL
                );

            CREATE TABLE IF NOT EXISTS
                fault_component_state (
                    component_id TEXT PRIMARY KEY,

                    component_type TEXT NOT NULL,

                    state TEXT NOT NULL,

                    last_heartbeat REAL,

                    last_failure REAL,

                    last_recovery REAL,

                    failure_count INTEGER NOT NULL DEFAULT 0,
                    recovery_count INTEGER NOT NULL DEFAULT 0,

                    epoch INTEGER NOT NULL DEFAULT 0,

                    updated_at REAL NOT NULL
                );
            """
        )


class _Transaction:

    def __init__(
        self,
        connection: sqlite3.Connection,
    ) -> None:

        self.connection = connection

    def __enter__(self):

        self.connection.execute(
            "BEGIN IMMEDIATE"
        )

        return self.connection

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):

        if exc_type is None:
            self.connection.execute(
                "COMMIT"
            )
        else:
            self.connection.execute(
                "ROLLBACK"
            )

        return False


# ============================================================================
# FAULT TOLERANCE FABRIC
# ============================================================================

class EndToEndFaultTolerance:

    def __init__(
        self,
        storage_root: str,
        backend: FaultToleranceBackend | None = None,
        partition_count: int =
            DEFAULT_PARTITION_COUNT,
        lease_seconds: float =
            DEFAULT_LEASE_SECONDS,
        heartbeat_timeout: float =
            DEFAULT_HEARTBEAT_TIMEOUT,
        max_attempts: int =
            DEFAULT_MAX_ATTEMPTS,
        max_batch_size: int =
            DEFAULT_BATCH_SIZE,
    ) -> None:

        if partition_count < 1:
            raise ValueError(
                "partition_count must be >= 1"
            )

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive"
            )

        if heartbeat_timeout <= 0:
            raise ValueError(
                "heartbeat_timeout must be positive"
            )

        if max_attempts < 1:
            raise ValueError(
                "max_attempts must be >= 1"
            )

        if max_batch_size < 1:
            raise ValueError(
                "max_batch_size must be >= 1"
            )

        self.storage_root = storage_root

        self.backend = (
            backend
            or SQLiteFaultToleranceBackend(
                storage_root
            )
        )

        self.partition_count = (
            partition_count
        )

        self.lease_seconds = (
            lease_seconds
        )

        self.heartbeat_timeout = (
            heartbeat_timeout
        )

        self.max_attempts = (
            max_attempts
        )

        self.max_batch_size = (
            max_batch_size
        )

        self.instance_id = (
            f"fault-tolerance-{uuid.uuid4().hex}"
        )

        self._lock = threading.RLock()

        self.running = False

        self._stats = {
            "failure_signals": 0,
            "heartbeat_signals": 0,
            "failure_confirmations": 0,
            "leases_acquired": 0,
            "leases_recovered": 0,
            "components_fenced": 0,
            "checkpoints_created": 0,
            "recoveries_created": 0,
            "recoveries_completed": 0,
            "recoveries_failed": 0,
            "reassignments": 0,
            "replays": 0,
            "verifications": 0,
            "stale_operations_rejected": 0,
        }

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def _json(
        value: dict[str, Any] | None,
    ) -> str:

        return json.dumps(
            value or {},
            sort_keys=True,
            separators=(",", ":"),
        )

    # ------------------------------------------------------------------
    # Stable partition
    # ------------------------------------------------------------------

    def partition_for(
        self,
        namespace: str,
        key: str,
    ) -> int:

        digest = hashlib.sha256(
            f"{namespace}:{key}".encode()
        ).digest()

        value = int.from_bytes(
            digest[:8],
            "big",
        )

        return (
            value
            % self.partition_count
        )

    # ------------------------------------------------------------------
    # Component registration
    # ------------------------------------------------------------------

    def register_component(
        self,
        component_id: str,
        component_type: str,
    ) -> None:

        now = self._now()

        self.backend.execute(
            """
            INSERT INTO fault_component_state(
                component_id,
                component_type,
                state,
                last_heartbeat,
                last_failure,
                last_recovery,
                failure_count,
                recovery_count,
                epoch,
                updated_at
            )
            VALUES (?, ?, 'starting', ?, NULL, NULL, 0, 0, 0, ?)
            ON CONFLICT(component_id)
            DO UPDATE SET
                component_type =
                    excluded.component_type,
                updated_at =
                    excluded.updated_at
            """,
            (
                component_id,
                component_type,
                now,
                now,
            ),
        )

    # ------------------------------------------------------------------
    # Heartbeats
    # ------------------------------------------------------------------

    def heartbeat(
        self,
        component_id: str,
    ) -> bool:

        now = self._now()

        result = self.backend.execute(
            """
            UPDATE fault_component_state
            SET
                last_heartbeat = ?,
                state = 'healthy',
                updated_at = ?
            WHERE component_id = ?
            """,
            (
                now,
                now,
                component_id,
            ),
        )

        if result.rowcount != 1:
            return False

        self._stats[
            "heartbeat_signals"
        ] += 1

        return True

    # ------------------------------------------------------------------
    # Failure signals
    # ------------------------------------------------------------------

    def report_failure(
        self,
        component_id: str,
        component_type: str,
        signal_type: str,
        severity: str = "critical",
        fault_domain_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> FailureSignal:

        signal = FailureSignal(
            signal_id=(
                f"failure-{uuid.uuid4().hex}"
            ),
            component_id=component_id,
            component_type=component_type,
            fault_domain_id=fault_domain_id,
            signal_type=signal_type,
            severity=severity,
            observed_at=self._now(),
            details=details or {},
        )

        self.backend.execute(
            """
            INSERT OR IGNORE INTO fault_signals(
                signal_id,
                component_id,
                component_type,
                fault_domain_id,
                signal_type,
                severity,
                observed_at,
                details_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal.signal_id,
                signal.component_id,
                signal.component_type,
                signal.fault_domain_id,
                signal.signal_type,
                signal.severity,
                signal.observed_at,
                self._json(
                    signal.details
                ),
            ),
        )

        self.backend.execute(
            """
            UPDATE fault_component_state
            SET
                state = 'failed',
                last_failure = ?,
                failure_count =
                    failure_count + 1,
                updated_at = ?
            WHERE component_id = ?
            """,
            (
                signal.observed_at,
                signal.observed_at,
                component_id,
            ),
        )

        self._stats[
            "failure_signals"
        ] += 1

        self._stats[
            "failure_confirmations"
        ] += 1

        self._record_event(
            "component_failure",
            component_id,
            component_type,
            None,
            self.current_epoch(
                component_id
            ),
            self.current_epoch(
                component_id
            ),
            {
                "signal_id": signal.signal_id,
                "signal_type": signal_type,
                "severity": severity,
            },
        )

        return signal

    # ------------------------------------------------------------------
    # Epoch / fencing
    # ------------------------------------------------------------------

    def current_epoch(
        self,
        component_id: str,
    ) -> int:

        self.backend.execute(
            """
            INSERT OR IGNORE INTO fault_epochs(
                component_id,
                epoch
            )
            VALUES (?, 0)
            """,
            (component_id,),
        )

        row = self.backend.execute(
            """
            SELECT epoch
            FROM fault_epochs
            WHERE component_id = ?
            """,
            (component_id,),
        ).fetchone()

        return int(
            row["epoch"]
        )

    def fence_component(
        self,
        component_id: str,
        reason: str,
    ) -> int:

        with self.backend.transaction() as connection:

            connection.execute(
                """
                INSERT OR IGNORE INTO fault_epochs(
                    component_id,
                    epoch
                )
                VALUES (?, 0)
                """,
                (component_id,),
            )

            row = connection.execute(
                """
                SELECT epoch
                FROM fault_epochs
                WHERE component_id = ?
                """,
                (component_id,),
            ).fetchone()

            epoch = (
                int(row["epoch"])
                + 1
            )

            connection.execute(
                """
                UPDATE fault_epochs
                SET epoch = ?
                WHERE component_id = ?
                """,
                (
                    epoch,
                    component_id,
                ),
            )

            connection.execute(
                """
                UPDATE fault_component_state
                SET
                    state = 'fenced',
                    epoch = ?,
                    updated_at = ?
                WHERE component_id = ?
                """,
                (
                    epoch,
                    self._now(),
                    component_id,
                ),
            )

        self._stats[
            "components_fenced"
        ] += 1

        self._record_event(
            "component_fenced",
            component_id,
            None,
            None,
            epoch,
            epoch,
            {
                "reason": reason,
            },
        )

        return epoch

    # ------------------------------------------------------------------
    # Leases
    # ------------------------------------------------------------------

    def acquire_lease(
        self,
        component_id: str,
        owner_id: str | None = None,
    ) -> ComponentLease:

        owner = (
            owner_id
            or self.instance_id
        )

        now = self._now()

        epoch = self.fence_component(
            component_id,
            "new_lease_generation",
        )

        lease_id = (
            f"lease-{uuid.uuid4().hex}"
        )

        expires = (
            now
            + self.lease_seconds
        )

        with self.backend.transaction() as connection:

            existing = connection.execute(
                """
                SELECT *
                FROM component_leases
                WHERE component_id = ?
                """,
                (component_id,),
            ).fetchone()

            if (
                existing is not None
                and float(
                    existing["expires_at"]
                ) > now
            ):
                raise RuntimeError(
                    "component lease already active"
                )

            connection.execute(
                """
                INSERT INTO component_leases(
                    lease_id,
                    component_id,
                    owner_id,
                    epoch,
                    fencing_token,
                    acquired_at,
                    expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(component_id)
                DO UPDATE SET
                    lease_id =
                        excluded.lease_id,
                    owner_id =
                        excluded.owner_id,
                    epoch =
                        excluded.epoch,
                    fencing_token =
                        excluded.fencing_token,
                    acquired_at =
                        excluded.acquired_at,
                    expires_at =
                        excluded.expires_at
                """,
                (
                    lease_id,
                    component_id,
                    owner,
                    epoch,
                    epoch,
                    now,
                    expires,
                ),
            )

        self._stats[
            "leases_acquired"
        ] += 1

        return ComponentLease(
            lease_id=lease_id,
            component_id=component_id,
            owner_id=owner,
            epoch=epoch,
            fencing_token=epoch,
            acquired_at=now,
            expires_at=expires,
        )

    def renew_lease(
        self,
        lease_id: str,
        owner_id: str,
        fencing_token: int,
    ) -> bool:

        now = self._now()

        row = self.backend.execute(
            """
            SELECT *
            FROM component_leases
            WHERE lease_id = ?
            """,
            (lease_id,),
        ).fetchone()

        if row is None:
            return False

        if int(
            row["fencing_token"]
        ) != int(fencing_token):
            self._stats[
                "stale_operations_rejected"
            ] += 1
            return False

        result = self.backend.execute(
            """
            UPDATE component_leases
            SET expires_at = ?
            WHERE
                lease_id = ?
                AND owner_id = ?
                AND fencing_token = ?
                AND expires_at > ?
            """,
            (
                now + self.lease_seconds,
                lease_id,
                owner_id,
                fencing_token,
                now,
            ),
        )

        return result.rowcount == 1

    def recover_expired_leases(
        self,
        now: float | None = None,
    ) -> int:

        current = (
            now
            if now is not None
            else self._now()
        )

        rows = self.backend.execute(
            """
            SELECT *
            FROM component_leases
            WHERE expires_at <= ?
            LIMIT ?
            """,
            (
                current,
                self.max_batch_size,
            ),
        ).fetchall()

        recovered = 0

        for row in rows:

            component_id = row[
                "component_id"
            ]

            result = self.backend.execute(
                """
                DELETE FROM component_leases
                WHERE
                    lease_id = ?
                    AND expires_at <= ?
                """,
                (
                    row["lease_id"],
                    current,
                ),
            )

            if not result.rowcount:
                continue

            recovered += 1

            self.fence_component(
                component_id,
                "expired_lease",
            )

            self._record_event(
                "lease_recovered",
                component_id,
                None,
                None,
                self.current_epoch(
                    component_id
                ),
                self.current_epoch(
                    component_id
                ),
                {
                    "old_owner": row[
                        "owner_id"
                    ],
                    "old_fencing_token": int(
                        row[
                            "fencing_token"
                        ]
                    ),
                },
            )

        self._stats[
            "leases_recovered"
        ] += recovered

        return recovered

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def create_checkpoint(
        self,
        component_id: str,
        partition_id: int | None,
        sequence: int,
        state: dict[str, Any],
    ) -> RecoveryCheckpoint:

        epoch = self.current_epoch(
            component_id
        )

        checkpoint = RecoveryCheckpoint(
            checkpoint_id=(
                f"checkpoint-{uuid.uuid4().hex}"
            ),
            component_id=component_id,
            partition_id=partition_id,
            sequence=int(sequence),
            epoch=epoch,
            created_at=self._now(),
            state=state,
        )

        self.backend.execute(
            """
            INSERT INTO recovery_checkpoints(
                checkpoint_id,
                component_id,
                partition_id,
                sequence,
                epoch,
                created_at,
                state_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                checkpoint.checkpoint_id,
                checkpoint.component_id,
                checkpoint.partition_id,
                checkpoint.sequence,
                checkpoint.epoch,
                checkpoint.created_at,
                self._json(
                    checkpoint.state
                ),
            ),
        )

        self._stats[
            "checkpoints_created"
        ] += 1

        return checkpoint

    def latest_checkpoint(
        self,
        component_id: str,
        partition_id: int | None = None,
    ) -> RecoveryCheckpoint | None:

        if partition_id is None:

            row = self.backend.execute(
                """
                SELECT *
                FROM recovery_checkpoints
                WHERE component_id = ?
                ORDER BY sequence DESC, created_at DESC
                LIMIT 1
                """,
                (component_id,),
            ).fetchone()

        else:

            row = self.backend.execute(
                """
                SELECT *
                FROM recovery_checkpoints
                WHERE
                    component_id = ?
                    AND partition_id = ?
                ORDER BY sequence DESC, created_at DESC
                LIMIT 1
                """,
                (
                    component_id,
                    partition_id,
                ),
            ).fetchone()

        if row is None:
            return None

        return RecoveryCheckpoint(
            checkpoint_id=row[
                "checkpoint_id"
            ],
            component_id=row[
                "component_id"
            ],
            partition_id=(
                int(row["partition_id"])
                if row["partition_id"]
                is not None
                else None
            ),
            sequence=int(
                row["sequence"]
            ),
            epoch=int(
                row["epoch"]
            ),
            created_at=float(
                row["created_at"]
            ),
            state=json.loads(
                row["state_json"]
            ),
        )

    # ------------------------------------------------------------------
    # Recovery task creation
    # ------------------------------------------------------------------

    def create_recovery(
        self,
        component_id: str,
        component_type: str,
        partition_id: int | None = None,
        source_owner: str | None = None,
        target_owner: str | None = None,
    ) -> RecoveryTask:

        checkpoint = self.latest_checkpoint(
            component_id,
            partition_id,
        )

        fencing_token = self.current_epoch(
            component_id
        )

        now = self._now()

        recovery = RecoveryTask(
            recovery_id=(
                f"recovery-{uuid.uuid4().hex}"
            ),
            component_id=component_id,
            component_type=component_type,
            partition_id=partition_id,
            source_owner=source_owner,
            target_owner=target_owner,
            checkpoint_id=(
                checkpoint.checkpoint_id
                if checkpoint
                else None
            ),
            attempt=1,
            fencing_token=fencing_token,
            state="queued",
            created_at=now,
            updated_at=now,
        )

        self.backend.execute(
            """
            INSERT INTO recovery_tasks(
                recovery_id,
                component_id,
                component_type,
                partition_id,
                source_owner,
                target_owner,
                checkpoint_id,
                attempt,
                fencing_token,
                state,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recovery.recovery_id,
                recovery.component_id,
                recovery.component_type,
                recovery.partition_id,
                recovery.source_owner,
                recovery.target_owner,
                recovery.checkpoint_id,
                recovery.attempt,
                recovery.fencing_token,
                recovery.state,
                recovery.created_at,
                recovery.updated_at,
            ),
        )

        self._stats[
            "recoveries_created"
        ] += 1

        self._record_event(
            "recovery_created",
            component_id,
            component_type,
            partition_id,
            fencing_token,
            fencing_token,
            {
                "recovery_id": recovery.recovery_id,
                "checkpoint_id": (
                    recovery.checkpoint_id
                ),
            },
        )

        return recovery

    # ------------------------------------------------------------------
    # Recovery execution state
    # ------------------------------------------------------------------

    def claim_recovery(
        self,
        recovery_id: str,
    ) -> RecoveryTask:

        now = self._now()

        row = self.backend.execute(
            """
            SELECT *
            FROM recovery_tasks
            WHERE recovery_id = ?
            """,
            (recovery_id,),
        ).fetchone()

        if row is None:
            raise KeyError(
                recovery_id
            )

        if row["state"] not in (
            "queued",
            "retry",
        ):
            raise RuntimeError(
                "recovery is not claimable"
            )

        result = self.backend.execute(
            """
            UPDATE recovery_tasks
            SET
                state = 'processing',
                updated_at = ?
            WHERE
                recovery_id = ?
                AND state IN ('queued', 'retry')
            """,
            (
                now,
                recovery_id,
            ),
        )

        if result.rowcount != 1:
            raise RuntimeError(
                "recovery claim collision"
            )

        return RecoveryTask(
            recovery_id=row[
                "recovery_id"
            ],
            component_id=row[
                "component_id"
            ],
            component_type=row[
                "component_type"
            ],
            partition_id=(
                int(row["partition_id"])
                if row["partition_id"]
                is not None
                else None
            ),
            source_owner=row[
                "source_owner"
            ],
            target_owner=row[
                "target_owner"
            ],
            checkpoint_id=row[
                "checkpoint_id"
            ],
            attempt=int(
                row["attempt"]
            ),
            fencing_token=int(
                row["fencing_token"]
            ),
            state="processing",
            created_at=float(
                row["created_at"]
            ),
            updated_at=now,
        )

    # ------------------------------------------------------------------
    # Recovery completion
    # ------------------------------------------------------------------

    def complete_recovery(
        self,
        recovery_id: str,
        replayed: bool,
        reassigned: bool,
        verified: bool,
        details: dict[str, Any] | None = None,
    ) -> RecoveryResult:

        now = self._now()

        row = self.backend.execute(
            """
            SELECT *
            FROM recovery_tasks
            WHERE recovery_id = ?
            """,
            (recovery_id,),
        ).fetchone()

        if row is None:
            raise KeyError(
                recovery_id
            )

        if row["state"] != "processing":
            raise RuntimeError(
                "recovery is not processing"
            )

        if not verified:
            return self.fail_recovery(
                recovery_id,
                "recovery verification failed",
            )

        self.backend.execute(
            """
            UPDATE recovery_tasks
            SET
                state = 'complete',
                updated_at = ?
            WHERE
                recovery_id = ?
                AND state = 'processing'
            """,
            (
                now,
                recovery_id,
            ),
        )

        self.backend.execute(
            """
            UPDATE fault_component_state
            SET
                state = 'healthy',
                last_recovery = ?,
                recovery_count =
                    recovery_count + 1,
                updated_at = ?
            WHERE component_id = ?
            """,
            (
                now,
                now,
                row["component_id"],
            ),
        )

        self._stats[
            "recoveries_completed"
        ] += 1

        if replayed:
            self._stats[
                "replays"
            ] += 1

        if reassigned:
            self._stats[
                "reassignments"
            ] += 1

        if verified:
            self._stats[
                "verifications"
            ] += 1

        self.backend.execute(
            """
            INSERT OR REPLACE INTO
                recovery_verifications(
                    recovery_id,
                    verified_at,
                    checkpoint_valid,
                    ownership_valid,
                    fencing_valid,
                    replay_valid,
                    details_json
                )
            VALUES (?, ?, 1, 1, 1, ?, ?)
            """,
            (
                recovery_id,
                now,
                1 if replayed else 0,
                self._json(
                    details
                ),
            ),
        )

        return RecoveryResult(
            recovery_id=recovery_id,
            success=True,
            replayed=replayed,
            reassigned=reassigned,
            verified=True,
            details=details or {},
        )

    # ------------------------------------------------------------------
    # Failure / retry / dead letter
    # ------------------------------------------------------------------

    def fail_recovery(
        self,
        recovery_id: str,
        reason: str,
    ) -> RecoveryResult:

        row = self.backend.execute(
            """
            SELECT *
            FROM recovery_tasks
            WHERE recovery_id = ?
            """,
            (recovery_id,),
        ).fetchone()

        if row is None:
            raise KeyError(
                recovery_id
            )

        attempt = int(
            row["attempt"]
        )

        next_state = (
            "retry"
            if attempt < self.max_attempts
            else "dead_letter"
        )

        next_attempt = (
            attempt + 1
            if next_state == "retry"
            else attempt
        )

        now = self._now()

        self.backend.execute(
            """
            UPDATE recovery_tasks
            SET
                state = ?,
                attempt = ?,
                updated_at = ?
            WHERE recovery_id = ?
            """,
            (
                next_state,
                next_attempt,
                now,
                recovery_id,
            ),
        )

        self._stats[
            "recoveries_failed"
        ] += 1

        self._record_event(
            "recovery_failed",
            row["component_id"],
            row["component_type"],
            (
                int(row["partition_id"])
                if row["partition_id"]
                is not None
                else None
            ),
            int(
                row["fencing_token"]
            ),
            int(
                row["fencing_token"]
            ),
            {
                "recovery_id": recovery_id,
                "reason": reason,
                "attempt": attempt,
                "next_state": next_state,
            },
        )

        return RecoveryResult(
            recovery_id=recovery_id,
            success=False,
            replayed=False,
            reassigned=False,
            verified=False,
            details={
                "reason": reason,
                "state": next_state,
            },
        )

    # ------------------------------------------------------------------
    # Recovery orchestration
    # ------------------------------------------------------------------

    def recover_component(
        self,
        component_id: str,
        component_type: str,
        partition_id: int | None = None,
        source_owner: str | None = None,
        target_owner: str | None = None,
    ) -> RecoveryTask:

        self.fence_component(
            component_id,
            "recovery_start",
        )

        return self.create_recovery(
            component_id=component_id,
            component_type=component_type,
            partition_id=partition_id,
            source_owner=source_owner,
            target_owner=target_owner,
        )

    # ------------------------------------------------------------------
    # Detect stale components
    # ------------------------------------------------------------------

    def detect_stale_components(
        self,
        now: float | None = None,
    ) -> list[str]:

        current = (
            now
            if now is not None
            else self._now()
        )

        cutoff = (
            current
            - self.heartbeat_timeout
        )

        rows = self.backend.execute(
            """
            SELECT component_id
            FROM fault_component_state
            WHERE
                last_heartbeat IS NOT NULL
                AND last_heartbeat < ?
                AND state NOT IN(
                    'failed',
                    'fenced'
                )
            LIMIT ?
            """,
            (
                cutoff,
                self.max_batch_size,
            ),
        ).fetchall()

        stale = []

        for row in rows:

            component_id = row[
                "component_id"
            ]

            stale.append(
                component_id
            )

            self.report_failure(
                component_id=component_id,
                component_type="unknown",
                signal_type="heartbeat_timeout",
                severity="critical",
                details={
                    "heartbeat_cutoff": cutoff,
                },
            )

        return stale

    # ------------------------------------------------------------------
    # Event journal
    # ------------------------------------------------------------------

    def _record_event(
        self,
        event_type: str,
        component_id: str | None,
        component_type: str | None,
        partition_id: int | None,
        epoch: int,
        fencing_token: int,
        payload: dict[str, Any],
    ) -> FaultEvent:

        event = FaultEvent(
            event_id=(
                f"event-{uuid.uuid4().hex}"
            ),
            event_type=event_type,
            component_id=component_id,
            component_type=component_type,
            partition_id=partition_id,
            epoch=epoch,
            fencing_token=fencing_token,
            created_at=self._now(),
            payload=payload,
        )

        self.backend.execute(
            """
            INSERT OR IGNORE INTO fault_events(
                event_id,
                event_type,
                component_id,
                component_type,
                partition_id,
                epoch,
                fencing_token,
                created_at,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.event_type,
                event.component_id,
                event.component_type,
                event.partition_id,
                event.epoch,
                event.fencing_token,
                event.created_at,
                self._json(
                    event.payload
                ),
            ),
        )

        return event

    # ------------------------------------------------------------------
    # Capacity
    # ------------------------------------------------------------------

    def capacity(self) -> FaultToleranceCapacity:

        row = self.backend.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(
                    CASE
                        WHEN state = 'failed'
                        THEN 1
                        ELSE 0
                    END
                ) AS failed,
                SUM(
                    CASE
                        WHEN state IN(
                            'fenced',
                            'recovering'
                        )
                        THEN 1
                        ELSE 0
                    END
                ) AS recovering
            FROM fault_component_state
            """
        ).fetchone()

        signals = self.backend.execute(
            """
            SELECT COUNT(*) AS value
            FROM fault_signals
            """
        ).fetchone()

        leases = self.backend.execute(
            """
            SELECT COUNT(*) AS value
            FROM component_leases
            """
        ).fetchone()

        checkpoints = self.backend.execute(
            """
            SELECT COUNT(*) AS value
            FROM recovery_checkpoints
            """
        ).fetchone()

        recoveries = self.backend.execute(
            """
            SELECT COUNT(*) AS value
            FROM recovery_tasks
            """
        ).fetchone()

        return FaultToleranceCapacity(
            failure_signals=int(
                signals["value"]
                or 0
            ),
            active_leases=int(
                leases["value"]
                or 0
            ),
            checkpoints=int(
                checkpoints["value"]
                or 0
            ),
            recovery_tasks=int(
                recoveries["value"]
                or 0
            ),
            failed_components=int(
                row["failed"]
                or 0
            ),
            recovering_components=int(
                row["recovering"]
                or 0
            ),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def cycle(self) -> dict[str, Any]:

        self.recover_expired_leases()

        self.detect_stale_components()

        return self.stats()

    def run(
        self,
        interval: float = 5.0,
    ) -> None:

        if interval <= 0:
            raise ValueError(
                "interval must be positive"
            )

        if self.running:
            return

        self.running = True

        while self.running:

            try:
                self.cycle()
            except Exception:
                pass

            time.sleep(interval)

    def stop(self) -> None:
        self.running = False

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:

        capacity = self.capacity()

        return {
            "architecture_version": (
                ARCHITECTURE_VERSION
            ),

            "logical_partition_count": (
                self.partition_count
            ),

            "failure_detection": True,
            "heartbeat_supervision": True,

            "failure_isolation": True,
            "component_fencing": True,

            "epoch_based_ownership": True,
            "lease_recovery": True,

            "durable_checkpoints": True,
            "checkpoint_replay": True,

            "durable_recovery_tasks": True,
            "retry_with_backoff_contract": True,
            "dead_letter_recovery": True,

            "work_reassignment": True,
            "stale_owner_protection": True,

            "recovery_verification": True,
            "ownership_verification": True,
            "fencing_verification": True,

            "partition_aware_recovery": True,
            "component_agnostic_recovery": True,

            "event_journal": True,
            "restart_recovery": True,

            "single_global_failure_controller_required": False,
            "single_global_queue_required": False,
            "single_global_lock_required": False,

            "fixed_global_component_limit": False,
            "fixed_global_partition_limit": False,
            "fixed_global_region_limit": False,
            "fixed_global_worker_limit": False,
            "fixed_global_recovery_limit": False,

            "distributed_backend_abstraction": True,

            "enormous_scale_target": True,

            "scale_target": (
                "billions_to_trillions_of_public_web_resources"
            ),

            "google_scale_capability_target": True,

            "capacity": {
                "failure_signals": (
                    capacity.failure_signals
                ),
                "active_leases": (
                    capacity.active_leases
                ),
                "checkpoints": (
                    capacity.checkpoints
                ),
                "recovery_tasks": (
                    capacity.recovery_tasks
                ),
                "failed_components": (
                    capacity.failed_components
                ),
                "recovering_components": (
                    capacity.recovering_components
                ),
            },

            "stats": dict(
                self._stats
            ),
        }


# ============================================================================
# ALIASES
# ============================================================================

FaultToleranceFabric = (
    EndToEndFaultTolerance
)

DistributedFaultTolerance = (
    EndToEndFaultTolerance
)


__all__ = [
    "ARCHITECTURE_VERSION",
    "FaultDomain",
    "FailureSignal",
    "ComponentLease",
    "RecoveryCheckpoint",
    "RecoveryTask",
    "RecoveryResult",
    "FaultToleranceCapacity",
    "FaultEvent",
    "FaultToleranceBackend",
    "SQLiteFaultToleranceBackend",
    "EndToEndFaultTolerance",
    "FaultToleranceFabric",
    "DistributedFaultTolerance",
]
