"""
OUR SEARCH — Phase 9.5
Cross-Region Coordination & Failover

Architecture target:
    Giant globally distributed Web-scale infrastructure capable of supporting
    billions and potentially trillions of public-Web resources.

This module provides the cross-region control architecture connecting:

    9.1 Distributed Execution Fabric
             ↓
    9.2 Massive Worker Fleet
             ↓
    9.3 Global Partition / Shard Management
             ↓
    9.4 Distributed Queue / Scheduling Fabric
             ↓
    9.5 Cross-Region Coordination & Failover
             ↓
    Existing Whole-Web Crawler

Important:
    This is a coordination/failover architecture layer.

    It intentionally does NOT pretend that a local SQLite database is a
    globally distributed consensus system. The metadata backend is abstracted
    so production deployments can use a distributed strongly-consistent
    metadata/consensus service.

Design goals:
    - multi-region execution
    - region health monitoring
    - region membership
    - generation/epoch fencing
    - split-brain protection
    - partition ownership
    - cross-region handoff
    - scheduler failover
    - worker failover
    - durable recovery checkpoints
    - retry-safe operations
    - idempotent handoffs
    - graceful draining
    - disaster recovery
    - capacity-aware failover
    - no fixed small global execution ceiling
    - deterministic state transitions
    - stale-owner fencing
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARCHITECTURE_VERSION = "cross-region-coordination-failover.v1"

DEFAULT_HEARTBEAT_TIMEOUT = 90.0
DEFAULT_FAILURE_CONFIRMATION_TIMEOUT = 180.0
DEFAULT_LEASE_SECONDS = 300.0
DEFAULT_HANDOFF_TIMEOUT = 600.0
DEFAULT_MAX_ATTEMPTS = 8


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class RegionState(str, Enum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    DRAINING = "draining"
    ISOLATED = "isolated"
    FAILED = "failed"
    RECOVERING = "recovering"
    STANDBY = "standby"
    REMOVED = "removed"


class RegionHealth(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    SUSPECTED = "suspected"
    FAILED = "failed"


class OwnershipState(str, Enum):
    ACTIVE = "active"
    HANDING_OFF = "handing_off"
    FENCED = "fenced"
    RECOVERING = "recovering"
    RELEASED = "released"


class HandoffState(str, Enum):
    PREPARING = "preparing"
    TRANSFERRING = "transferring"
    COMMITTED = "committed"
    ABORTED = "aborted"
    RECOVERING = "recovering"


# ---------------------------------------------------------------------------
# Data contracts
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RegionIdentity:
    region_id: str
    zone_id: str
    cluster_id: str
    endpoint: str | None = None
    capacity_units: float = 0.0
    metadata: dict[str, Any] | None = None


@dataclass
class RegionRecord:
    region_id: str
    zone_id: str
    cluster_id: str
    endpoint: str | None
    state: str
    health: str
    capacity_units: float
    used_capacity_units: float
    generation: int
    fencing_epoch: int
    last_heartbeat: float
    registered_at: float
    updated_at: float
    metadata_json: str

    @property
    def available_capacity_units(self) -> float:
        return max(
            0.0,
            self.capacity_units - self.used_capacity_units,
        )


@dataclass(frozen=True)
class RegionLease:
    region_id: str
    generation: int
    fencing_epoch: int
    lease_until: float
    owner_id: str


@dataclass
class PartitionOwnership:
    partition_id: str
    region_id: str
    state: str
    ownership_epoch: int
    fencing_epoch: int
    owner_id: str
    acquired_at: float
    updated_at: float
    lease_until: float
    source_region_id: str | None = None
    target_region_id: str | None = None


@dataclass
class CrossRegionHandoff:
    handoff_id: str
    partition_id: str
    source_region_id: str
    target_region_id: str
    state: str
    source_epoch: int
    target_epoch: int
    fencing_epoch: int
    checkpoint: str | None
    created_at: float
    updated_at: float
    lease_until: float
    attempts: int
    last_error: str | None = None


@dataclass(frozen=True)
class RecoveryCheckpoint:
    checkpoint_id: str
    partition_id: str
    region_id: str
    ownership_epoch: int
    fencing_epoch: int
    cursor: str | None
    sequence: int
    created_at: float
    payload: dict[str, Any]


@dataclass(frozen=True)
class RegionCapacity:
    region_id: str
    capacity_units: float
    used_capacity_units: float
    available_capacity_units: float
    active_partitions: int
    handing_off_partitions: int
    health: str
    state: str


@dataclass(frozen=True)
class FailoverDecision:
    partition_id: str
    failed_region_id: str
    target_region_id: str | None
    reason: str
    fencing_epoch: int
    created_at: float


@dataclass(frozen=True)
class CoordinationEvent:
    event_id: str
    event_type: str
    region_id: str | None
    partition_id: str | None
    handoff_id: str | None
    fencing_epoch: int
    created_at: float
    payload: dict[str, Any]


# ---------------------------------------------------------------------------
# Metadata backend contract
# ---------------------------------------------------------------------------

class CrossRegionMetadataBackend:
    """
    Backend contract.

    The local SQLite implementation below is deliberately only a reference
    durable backend.

    A real globally distributed deployment should replace this backend with
    a distributed metadata/consensus system providing:

        - strongly ordered epochs
        - durable membership
        - atomic ownership transitions
        - fencing
        - quorum semantics
        - geographically replicated metadata
        - failure-domain awareness

    This abstraction prevents the architecture from treating one SQLite file
    as the global control-plane ceiling.
    """

    def transaction(self):
        raise NotImplementedError

    def execute(self, *args, **kwargs):
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Durable reference backend
# ---------------------------------------------------------------------------

class SQLiteCrossRegionMetadataBackend(CrossRegionMetadataBackend):

    def __init__(self, storage_root: str):
        root = Path(storage_root)
        root.mkdir(parents=True, exist_ok=True)

        self.path = root / "cross_region_coordination.db"

        self._local = threading.local()

        connection = self._connection()
        self._initialize(connection)

    def _connection(self) -> sqlite3.Connection:
        connection = getattr(self._local, "connection", None)

        if connection is None:
            connection = sqlite3.connect(
                str(self.path),
                timeout=30.0,
                isolation_level=None,
                check_same_thread=False,
            )
            connection.row_factory = sqlite3.Row

            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute("PRAGMA busy_timeout=30000")

            self._local.connection = connection

        return connection

    def transaction(self):
        return _SQLiteTransaction(self._connection())

    def execute(self, sql: str, params: tuple[Any, ...] = ()):
        return self._connection().execute(sql, params)

    def executemany(
        self,
        sql: str,
        rows: Iterable[tuple[Any, ...]],
    ):
        return self._connection().executemany(sql, rows)

    def _initialize(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS global_epochs (
                namespace TEXT PRIMARY KEY,
                epoch INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS regions (
                region_id TEXT PRIMARY KEY,
                zone_id TEXT NOT NULL,
                cluster_id TEXT NOT NULL,
                endpoint TEXT,
                state TEXT NOT NULL,
                health TEXT NOT NULL,
                capacity_units REAL NOT NULL,
                used_capacity_units REAL NOT NULL,
                generation INTEGER NOT NULL,
                fencing_epoch INTEGER NOT NULL,
                last_heartbeat REAL NOT NULL,
                registered_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_regions_state
                ON regions(state);

            CREATE INDEX IF NOT EXISTS idx_regions_health
                ON regions(health);

            CREATE TABLE IF NOT EXISTS region_leases (
                region_id TEXT PRIMARY KEY,
                generation INTEGER NOT NULL,
                fencing_epoch INTEGER NOT NULL,
                lease_until REAL NOT NULL,
                owner_id TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS partition_ownership (
                partition_id TEXT PRIMARY KEY,
                region_id TEXT NOT NULL,
                state TEXT NOT NULL,
                ownership_epoch INTEGER NOT NULL,
                fencing_epoch INTEGER NOT NULL,
                owner_id TEXT NOT NULL,
                acquired_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                lease_until REAL NOT NULL,
                source_region_id TEXT,
                target_region_id TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_partition_region
                ON partition_ownership(region_id);

            CREATE INDEX IF NOT EXISTS idx_partition_state
                ON partition_ownership(state);

            CREATE TABLE IF NOT EXISTS handoffs (
                handoff_id TEXT PRIMARY KEY,
                partition_id TEXT NOT NULL,
                source_region_id TEXT NOT NULL,
                target_region_id TEXT NOT NULL,
                state TEXT NOT NULL,
                source_epoch INTEGER NOT NULL,
                target_epoch INTEGER NOT NULL,
                fencing_epoch INTEGER NOT NULL,
                checkpoint TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                lease_until REAL NOT NULL,
                attempts INTEGER NOT NULL,
                last_error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_handoffs_partition
                ON handoffs(partition_id);

            CREATE INDEX IF NOT EXISTS idx_handoffs_state
                ON handoffs(state);

            CREATE TABLE IF NOT EXISTS recovery_checkpoints (
                checkpoint_id TEXT PRIMARY KEY,
                partition_id TEXT NOT NULL,
                region_id TEXT NOT NULL,
                ownership_epoch INTEGER NOT NULL,
                fencing_epoch INTEGER NOT NULL,
                cursor TEXT,
                sequence INTEGER NOT NULL,
                created_at REAL NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_checkpoints_partition
                ON recovery_checkpoints(partition_id, sequence DESC);

            CREATE TABLE IF NOT EXISTS coordination_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                region_id TEXT,
                partition_id TEXT,
                handoff_id TEXT,
                fencing_epoch INTEGER NOT NULL,
                created_at REAL NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_events_created
                ON coordination_events(created_at);
            """
        )


class _SQLiteTransaction:

    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def __enter__(self):
        self.connection.execute("BEGIN IMMEDIATE")
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.connection.execute("COMMIT")
        else:
            self.connection.execute("ROLLBACK")

        return False


# ---------------------------------------------------------------------------
# Cross-region coordination manager
# ---------------------------------------------------------------------------

class CrossRegionCoordinationFailover:

    def __init__(
        self,
        storage_root: str,
        metadata_backend: CrossRegionMetadataBackend | None = None,
        heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT,
        failure_confirmation_timeout: float = (
            DEFAULT_FAILURE_CONFIRMATION_TIMEOUT
        ),
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        handoff_timeout: float = DEFAULT_HANDOFF_TIMEOUT,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        owner_id: str | None = None,
    ) -> None:

        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be positive")

        if failure_confirmation_timeout <= 0:
            raise ValueError(
                "failure_confirmation_timeout must be positive"
            )

        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")

        if handoff_timeout <= 0:
            raise ValueError("handoff_timeout must be positive")

        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        self.storage_root = storage_root

        self.backend = (
            metadata_backend
            or SQLiteCrossRegionMetadataBackend(storage_root)
        )

        self.heartbeat_timeout = heartbeat_timeout
        self.failure_confirmation_timeout = (
            failure_confirmation_timeout
        )
        self.lease_seconds = lease_seconds
        self.handoff_timeout = handoff_timeout
        self.max_attempts = max_attempts

        self.owner_id = owner_id or (
            f"coord-{uuid.uuid4().hex}"
        )

        self._lock = threading.RLock()

        self.running = False

        self._stats = {
            "region_registrations": 0,
            "region_heartbeats": 0,
            "region_failures": 0,
            "region_recoveries": 0,
            "partition_acquisitions": 0,
            "partition_fences": 0,
            "handoffs_created": 0,
            "handoffs_committed": 0,
            "handoffs_aborted": 0,
            "handoffs_recovered": 0,
            "checkpoints_written": 0,
            "failover_decisions": 0,
            "split_brain_fences": 0,
        }

        self._ensure_epoch_namespace()

    # ------------------------------------------------------------------
    # Identity / deterministic helpers
    # ------------------------------------------------------------------

    @staticmethod
    def new_region_id(prefix: str = "region") -> str:
        return f"{prefix}-{uuid.uuid4().hex}"

    @staticmethod
    def stable_partition_score(
        partition_id: str,
        region_id: str,
    ) -> int:
        digest = hashlib.sha256(
            f"{partition_id}|{region_id}".encode()
        ).digest()

        return int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

    @staticmethod
    def _metadata_json(
        metadata: dict[str, Any] | None,
    ) -> str:
        return json.dumps(
            metadata or {},
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _now() -> float:
        return time.time()

    # ------------------------------------------------------------------
    # Epoch / fencing
    # ------------------------------------------------------------------

    def _ensure_epoch_namespace(self) -> None:
        self.backend.execute(
            """
            INSERT OR IGNORE INTO global_epochs(namespace, epoch)
            VALUES (?, 0)
            """,
            ("cross_region",),
        )

    def current_fencing_epoch(self) -> int:
        row = self.backend.execute(
            """
            SELECT epoch
            FROM global_epochs
            WHERE namespace = ?
            """,
            ("cross_region",),
        ).fetchone()

        if row is None:
            self._ensure_epoch_namespace()
            return 0

        return int(row["epoch"])

    def advance_fencing_epoch(self, reason: str) -> int:
        with self.backend.transaction() as connection:
            row = connection.execute(
                """
                SELECT epoch
                FROM global_epochs
                WHERE namespace = ?
                """,
                ("cross_region",),
            ).fetchone()

            current = int(row["epoch"]) if row else 0
            new_epoch = current + 1

            connection.execute(
                """
                INSERT INTO global_epochs(namespace, epoch)
                VALUES (?, ?)
                ON CONFLICT(namespace)
                DO UPDATE SET epoch = excluded.epoch
                """,
                ("cross_region", new_epoch),
            )

        self._record_event(
            "fencing_epoch_advanced",
            None,
            None,
            None,
            new_epoch,
            {
                "reason": reason,
            },
        )

        return new_epoch

    # ------------------------------------------------------------------
    # Region membership
    # ------------------------------------------------------------------

    def register_region(
        self,
        identity: RegionIdentity,
        state: RegionState = RegionState.ACTIVE,
    ) -> RegionRecord:

        now = self._now()

        with self.backend.transaction() as connection:
            existing = connection.execute(
                """
                SELECT *
                FROM regions
                WHERE region_id = ?
                """,
                (identity.region_id,),
            ).fetchone()

            if existing is None:
                generation = 1
                fencing_epoch = self.current_fencing_epoch()

                connection.execute(
                    """
                    INSERT INTO regions(
                        region_id,
                        zone_id,
                        cluster_id,
                        endpoint,
                        state,
                        health,
                        capacity_units,
                        used_capacity_units,
                        generation,
                        fencing_epoch,
                        last_heartbeat,
                        registered_at,
                        updated_at,
                        metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        identity.region_id,
                        identity.zone_id,
                        identity.cluster_id,
                        identity.endpoint,
                        state.value,
                        RegionHealth.HEALTHY.value,
                        identity.capacity_units,
                        0.0,
                        generation,
                        fencing_epoch,
                        now,
                        now,
                        now,
                        self._metadata_json(identity.metadata),
                    ),
                )

            else:
                generation = int(existing["generation"]) + 1
                fencing_epoch = self.advance_fencing_epoch(
                    f"region_reregister:{identity.region_id}"
                )

                connection.execute(
                    """
                    UPDATE regions
                    SET
                        zone_id = ?,
                        cluster_id = ?,
                        endpoint = ?,
                        state = ?,
                        health = ?,
                        capacity_units = ?,
                        generation = ?,
                        fencing_epoch = ?,
                        last_heartbeat = ?,
                        updated_at = ?,
                        metadata_json = ?
                    WHERE region_id = ?
                    """,
                    (
                        identity.zone_id,
                        identity.cluster_id,
                        identity.endpoint,
                        state.value,
                        RegionHealth.HEALTHY.value,
                        identity.capacity_units,
                        generation,
                        fencing_epoch,
                        now,
                        now,
                        self._metadata_json(identity.metadata),
                        identity.region_id,
                    ),
                )

        self._stats["region_registrations"] += 1

        self._record_event(
            "region_registered",
            identity.region_id,
            None,
            None,
            fencing_epoch,
            {
                "generation": generation,
                "state": state.value,
            },
        )

        return self.get_region(identity.region_id)

    def get_region(
        self,
        region_id: str,
    ) -> RegionRecord | None:

        row = self.backend.execute(
            """
            SELECT *
            FROM regions
            WHERE region_id = ?
            """,
            (region_id,),
        ).fetchone()

        if row is None:
            return None

        return RegionRecord(**dict(row))

    def list_regions(
        self,
        states: set[str] | None = None,
    ) -> list[RegionRecord]:

        if states:
            placeholders = ",".join(
                "?" for _ in states
            )

            rows = self.backend.execute(
                f"""
                SELECT *
                FROM regions
                WHERE state IN ({placeholders})
                ORDER BY region_id
                """,
                tuple(sorted(states)),
            ).fetchall()

        else:
            rows = self.backend.execute(
                """
                SELECT *
                FROM regions
                ORDER BY region_id
                """
            ).fetchall()

        return [
            RegionRecord(**dict(row))
            for row in rows
        ]

    def heartbeat(
        self,
        region_id: str,
        generation: int,
        used_capacity_units: float | None = None,
    ) -> bool:

        now = self._now()

        with self.backend.transaction() as connection:
            row = connection.execute(
                """
                SELECT generation, state
                FROM regions
                WHERE region_id = ?
                """,
                (region_id,),
            ).fetchone()

            if row is None:
                return False

            if int(row["generation"]) != generation:
                self._fence_region_locked(
                    connection,
                    region_id,
                    "stale_generation_heartbeat",
                )
                return False

            if row["state"] in {
                RegionState.FAILED.value,
                RegionState.REMOVED.value,
            }:
                return False

            if used_capacity_units is None:
                connection.execute(
                    """
                    UPDATE regions
                    SET
                        last_heartbeat = ?,
                        health = ?,
                        updated_at = ?
                    WHERE region_id = ?
                    """,
                    (
                        now,
                        RegionHealth.HEALTHY.value,
                        now,
                        region_id,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE regions
                    SET
                        last_heartbeat = ?,
                        health = ?,
                        used_capacity_units = ?,
                        updated_at = ?
                    WHERE region_id = ?
                    """,
                    (
                        now,
                        RegionHealth.HEALTHY.value,
                        max(0.0, used_capacity_units),
                        now,
                        region_id,
                    ),
                )

        self._stats["region_heartbeats"] += 1
        return True

    # ------------------------------------------------------------------
    # Region state
    # ------------------------------------------------------------------

    def set_region_state(
        self,
        region_id: str,
        state: RegionState,
        reason: str,
    ) -> bool:

        now = self._now()

        with self.backend.transaction() as connection:
            row = connection.execute(
                """
                SELECT fencing_epoch
                FROM regions
                WHERE region_id = ?
                """,
                (region_id,),
            ).fetchone()

            if row is None:
                return False

            connection.execute(
                """
                UPDATE regions
                SET
                    state = ?,
                    updated_at = ?
                WHERE region_id = ?
                """,
                (
                    state.value,
                    now,
                    region_id,
                ),
            )

            epoch = int(row["fencing_epoch"])

        self._record_event(
            "region_state_changed",
            region_id,
            None,
            None,
            epoch,
            {
                "state": state.value,
                "reason": reason,
            },
        )

        return True

    def begin_region_drain(
        self,
        region_id: str,
    ) -> bool:

        return self.set_region_state(
            region_id,
            RegionState.DRAINING,
            "operator_or_scheduler_drain",
        )

    def isolate_region(
        self,
        region_id: str,
        reason: str = "network_or_consistency_isolation",
    ) -> bool:

        epoch = self.advance_fencing_epoch(
            f"isolate_region:{region_id}"
        )

        with self.backend.transaction() as connection:
            row = connection.execute(
                """
                SELECT fencing_epoch
                FROM regions
                WHERE region_id = ?
                """,
                (region_id,),
            ).fetchone()

            if row is None:
                return False

            connection.execute(
                """
                UPDATE regions
                SET
                    state = ?,
                    health = ?,
                    fencing_epoch = ?,
                    updated_at = ?
                WHERE region_id = ?
                """,
                (
                    RegionState.ISOLATED.value,
                    RegionHealth.SUSPECTED.value,
                    epoch,
                    self._now(),
                    region_id,
                ),
            )

            self._fence_region_partitions_locked(
                connection,
                region_id,
                epoch,
                reason,
            )

        self._stats["split_brain_fences"] += 1

        self._record_event(
            "region_isolated",
            region_id,
            None,
            None,
            epoch,
            {
                "reason": reason,
            },
        )

        return True

    # ------------------------------------------------------------------
    # Region leases
    # ------------------------------------------------------------------

    def acquire_region_lease(
        self,
        region_id: str,
    ) -> RegionLease | None:

        now = self._now()
        lease_until = now + self.lease_seconds

        with self.backend.transaction() as connection:
            row = connection.execute(
                """
                SELECT
                    generation,
                    fencing_epoch,
                    state
                FROM regions
                WHERE region_id = ?
                """,
                (region_id,),
            ).fetchone()

            if row is None:
                return None

            if row["state"] in {
                RegionState.FAILED.value,
                RegionState.REMOVED.value,
                RegionState.ISOLATED.value,
            }:
                return None

            current = connection.execute(
                """
                SELECT *
                FROM region_leases
                WHERE region_id = ?
                """,
                (region_id,),
            ).fetchone()

            if current is not None:
                if (
                    float(current["lease_until"]) > now
                    and current["owner_id"] != self.owner_id
                ):
                    return None

            connection.execute(
                """
                INSERT INTO region_leases(
                    region_id,
                    generation,
                    fencing_epoch,
                    lease_until,
                    owner_id
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(region_id)
                DO UPDATE SET
                    generation = excluded.generation,
                    fencing_epoch = excluded.fencing_epoch,
                    lease_until = excluded.lease_until,
                    owner_id = excluded.owner_id
                """,
                (
                    region_id,
                    int(row["generation"]),
                    int(row["fencing_epoch"]),
                    lease_until,
                    self.owner_id,
                ),
            )

            return RegionLease(
                region_id=region_id,
                generation=int(row["generation"]),
                fencing_epoch=int(row["fencing_epoch"]),
                lease_until=lease_until,
                owner_id=self.owner_id,
            )

    def renew_region_lease(
        self,
        lease: RegionLease,
    ) -> bool:

        now = self._now()
        lease_until = now + self.lease_seconds

        result = self.backend.execute(
            """
            UPDATE region_leases
            SET lease_until = ?
            WHERE
                region_id = ?
                AND generation = ?
                AND fencing_epoch = ?
                AND owner_id = ?
                AND lease_until > ?
            """,
            (
                lease_until,
                lease.region_id,
                lease.generation,
                lease.fencing_epoch,
                lease.owner_id,
                now,
            ),
        )

        return result.rowcount == 1

    # ------------------------------------------------------------------
    # Partition ownership
    # ------------------------------------------------------------------

    def acquire_partition(
        self,
        partition_id: str,
        region_id: str,
        owner_id: str | None = None,
    ) -> PartitionOwnership | None:

        owner = owner_id or self.owner_id
        now = self._now()
        lease_until = now + self.lease_seconds

        with self.backend.transaction() as connection:
            region = connection.execute(
                """
                SELECT *
                FROM regions
                WHERE region_id = ?
                """,
                (region_id,),
            ).fetchone()

            if region is None:
                return None

            if region["state"] not in {
                RegionState.ACTIVE.value,
                RegionState.DEGRADED.value,
                RegionState.RECOVERING.value,
            }:
                return None

            existing = connection.execute(
                """
                SELECT *
                FROM partition_ownership
                WHERE partition_id = ?
                """,
                (partition_id,),
            ).fetchone()

            if existing is not None:
                if existing["state"] == OwnershipState.ACTIVE.value:
                    if (
                        existing["region_id"] == region_id
                        and existing["owner_id"] == owner
                        and float(existing["lease_until"]) <= now
                    ):
                        pass
                    elif float(existing["lease_until"]) > now:
                        return None

                ownership_epoch = (
                    int(existing["ownership_epoch"]) + 1
                )

            else:
                ownership_epoch = 1

            fencing_epoch = int(
                region["fencing_epoch"]
            )

            connection.execute(
                """
                INSERT INTO partition_ownership(
                    partition_id,
                    region_id,
                    state,
                    ownership_epoch,
                    fencing_epoch,
                    owner_id,
                    acquired_at,
                    updated_at,
                    lease_until,
                    source_region_id,
                    target_region_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                ON CONFLICT(partition_id)
                DO UPDATE SET
                    region_id = excluded.region_id,
                    state = excluded.state,
                    ownership_epoch = excluded.ownership_epoch,
                    fencing_epoch = excluded.fencing_epoch,
                    owner_id = excluded.owner_id,
                    acquired_at = excluded.acquired_at,
                    updated_at = excluded.updated_at,
                    lease_until = excluded.lease_until,
                    source_region_id = excluded.source_region_id,
                    target_region_id = excluded.target_region_id
                """,
                (
                    partition_id,
                    region_id,
                    OwnershipState.ACTIVE.value,
                    ownership_epoch,
                    fencing_epoch,
                    owner,
                    now,
                    now,
                    lease_until,
                ),
            )

        self._stats["partition_acquisitions"] += 1

        return self.get_partition_ownership(
            partition_id
        )

    def get_partition_ownership(
        self,
        partition_id: str,
    ) -> PartitionOwnership | None:

        row = self.backend.execute(
            """
            SELECT *
            FROM partition_ownership
            WHERE partition_id = ?
            """,
            (partition_id,),
        ).fetchone()

        if row is None:
            return None

        return PartitionOwnership(
            **dict(row)
        )

    def list_region_partitions(
        self,
        region_id: str,
    ) -> list[PartitionOwnership]:

        rows = self.backend.execute(
            """
            SELECT *
            FROM partition_ownership
            WHERE region_id = ?
            ORDER BY partition_id
            """,
            (region_id,),
        ).fetchall()

        return [
            PartitionOwnership(**dict(row))
            for row in rows
        ]

    def renew_partition(
        self,
        partition_id: str,
        region_id: str,
        ownership_epoch: int,
        fencing_epoch: int,
        owner_id: str | None = None,
    ) -> bool:

        owner = owner_id or self.owner_id
        now = self._now()
        lease_until = now + self.lease_seconds

        result = self.backend.execute(
            """
            UPDATE partition_ownership
            SET
                updated_at = ?,
                lease_until = ?
            WHERE
                partition_id = ?
                AND region_id = ?
                AND ownership_epoch = ?
                AND fencing_epoch = ?
                AND owner_id = ?
                AND state = ?
                AND lease_until > ?
            """,
            (
                now,
                lease_until,
                partition_id,
                region_id,
                ownership_epoch,
                fencing_epoch,
                owner,
                OwnershipState.ACTIVE.value,
                now,
            ),
        )

        return result.rowcount == 1

    def fence_partition(
        self,
        partition_id: str,
        reason: str,
    ) -> bool:

        epoch = self.advance_fencing_epoch(
            f"fence_partition:{partition_id}"
        )

        with self.backend.transaction() as connection:
            result = connection.execute(
                """
                UPDATE partition_ownership
                SET
                    state = ?,
                    fencing_epoch = ?,
                    updated_at = ?
                WHERE partition_id = ?
                """,
                (
                    OwnershipState.FENCED.value,
                    epoch,
                    self._now(),
                    partition_id,
                ),
            )

        if result.rowcount:
            self._stats["partition_fences"] += 1

            self._record_event(
                "partition_fenced",
                None,
                partition_id,
                None,
                epoch,
                {
                    "reason": reason,
                },
            )

            return True

        return False

    # ------------------------------------------------------------------
    # Handoff
    # ------------------------------------------------------------------

    def prepare_handoff(
        self,
        partition_id: str,
        target_region_id: str,
        checkpoint: str | None = None,
    ) -> CrossRegionHandoff | None:

        now = self._now()

        with self.backend.transaction() as connection:
            ownership = connection.execute(
                """
                SELECT *
                FROM partition_ownership
                WHERE partition_id = ?
                """,
                (partition_id,),
            ).fetchone()

            if ownership is None:
                return None

            source_region_id = ownership["region_id"]

            if source_region_id == target_region_id:
                return None

            target_region = connection.execute(
                """
                SELECT *
                FROM regions
                WHERE region_id = ?
                """,
                (target_region_id,),
            ).fetchone()

            if target_region is None:
                return None

            if target_region["state"] not in {
                RegionState.ACTIVE.value,
                RegionState.DEGRADED.value,
                RegionState.RECOVERING.value,
            }:
                return None

            existing = connection.execute(
                """
                SELECT *
                FROM handoffs
                WHERE partition_id = ?
                  AND state IN (?, ?)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (
                    partition_id,
                    HandoffState.PREPARING.value,
                    HandoffState.TRANSFERRING.value,
                ),
            ).fetchone()

            if existing is not None:
                return CrossRegionHandoff(
                    **dict(existing)
                )

            handoff_id = (
                f"handoff-{uuid.uuid4().hex}"
            )

            fencing_epoch = max(
                int(ownership["fencing_epoch"]),
                int(target_region["fencing_epoch"]),
                self.current_fencing_epoch(),
            )

            target_epoch = int(
                ownership["ownership_epoch"]
            ) + 1

            connection.execute(
                """
                UPDATE partition_ownership
                SET
                    state = ?,
                    updated_at = ?,
                    source_region_id = ?,
                    target_region_id = ?,
                    fencing_epoch = ?
                WHERE partition_id = ?
                """,
                (
                    OwnershipState.HANDING_OFF.value,
                    now,
                    source_region_id,
                    target_region_id,
                    fencing_epoch,
                    partition_id,
                ),
            )

            connection.execute(
                """
                INSERT INTO handoffs(
                    handoff_id,
                    partition_id,
                    source_region_id,
                    target_region_id,
                    state,
                    source_epoch,
                    target_epoch,
                    fencing_epoch,
                    checkpoint,
                    created_at,
                    updated_at,
                    lease_until,
                    attempts,
                    last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
                """,
                (
                    handoff_id,
                    partition_id,
                    source_region_id,
                    target_region_id,
                    HandoffState.PREPARING.value,
                    int(ownership["ownership_epoch"]),
                    target_epoch,
                    fencing_epoch,
                    checkpoint,
                    now,
                    now,
                    now + self.handoff_timeout,
                ),
            )

        self._stats["handoffs_created"] += 1

        self._record_event(
            "handoff_prepared",
            source_region_id,
            partition_id,
            handoff_id,
            fencing_epoch,
            {
                "target_region_id": target_region_id,
            },
        )

        return self.get_handoff(handoff_id)

    def get_handoff(
        self,
        handoff_id: str,
    ) -> CrossRegionHandoff | None:

        row = self.backend.execute(
            """
            SELECT *
            FROM handoffs
            WHERE handoff_id = ?
            """,
            (handoff_id,),
        ).fetchone()

        if row is None:
            return None

        return CrossRegionHandoff(**dict(row))

    def begin_handoff_transfer(
        self,
        handoff_id: str,
    ) -> bool:

        now = self._now()

        result = self.backend.execute(
            """
            UPDATE handoffs
            SET
                state = ?,
                updated_at = ?,
                lease_until = ?,
                attempts = attempts + 1
            WHERE
                handoff_id = ?
                AND state = ?
            """,
            (
                HandoffState.TRANSFERRING.value,
                now,
                now + self.handoff_timeout,
                handoff_id,
                HandoffState.PREPARING.value,
            ),
        )

        return result.rowcount == 1

    def commit_handoff(
        self,
        handoff_id: str,
    ) -> bool:

        now = self._now()

        with self.backend.transaction() as connection:
            handoff = connection.execute(
                """
                SELECT *
                FROM handoffs
                WHERE handoff_id = ?
                """,
                (handoff_id,),
            ).fetchone()

            if handoff is None:
                return False

            if handoff["state"] == HandoffState.COMMITTED.value:
                return True

            if handoff["state"] != HandoffState.TRANSFERRING.value:
                return False

            target_region = connection.execute(
                """
                SELECT *
                FROM regions
                WHERE region_id = ?
                """,
                (handoff["target_region_id"],),
            ).fetchone()

            if target_region is None:
                return False

            if target_region["state"] in {
                RegionState.FAILED.value,
                RegionState.ISOLATED.value,
                RegionState.REMOVED.value,
            }:
                return False

            connection.execute(
                """
                UPDATE partition_ownership
                SET
                    region_id = ?,
                    state = ?,
                    ownership_epoch = ?,
                    fencing_epoch = ?,
                    owner_id = ?,
                    updated_at = ?,
                    lease_until = ?,
                    source_region_id = NULL,
                    target_region_id = NULL
                WHERE
                    partition_id = ?
                    AND state = ?
                """,
                (
                    handoff["target_region_id"],
                    OwnershipState.ACTIVE.value,
                    int(handoff["target_epoch"]),
                    int(handoff["fencing_epoch"]),
                    self.owner_id,
                    now,
                    now + self.lease_seconds,
                    handoff["partition_id"],
                    OwnershipState.HANDING_OFF.value,
                ),
            )

            connection.execute(
                """
                UPDATE handoffs
                SET
                    state = ?,
                    updated_at = ?,
                    lease_until = ?
                WHERE handoff_id = ?
                """,
                (
                    HandoffState.COMMITTED.value,
                    now,
                    now,
                    handoff_id,
                ),
            )

        self._stats["handoffs_committed"] += 1

        self._record_event(
            "handoff_committed",
            handoff["target_region_id"],
            handoff["partition_id"],
            handoff_id,
            int(handoff["fencing_epoch"]),
            {},
        )

        return True

    def abort_handoff(
        self,
        handoff_id: str,
        reason: str,
    ) -> bool:

        now = self._now()

        with self.backend.transaction() as connection:
            handoff = connection.execute(
                """
                SELECT *
                FROM handoffs
                WHERE handoff_id = ?
                """,
                (handoff_id,),
            ).fetchone()

            if handoff is None:
                return False

            if handoff["state"] == HandoffState.COMMITTED.value:
                return False

            connection.execute(
                """
                UPDATE handoffs
                SET
                    state = ?,
                    updated_at = ?,
                    lease_until = ?,
                    last_error = ?
                WHERE handoff_id = ?
                """,
                (
                    HandoffState.ABORTED.value,
                    now,
                    now,
                    reason,
                    handoff_id,
                ),
            )

            connection.execute(
                """
                UPDATE partition_ownership
                SET
                    state = ?,
                    updated_at = ?,
                    source_region_id = NULL,
                    target_region_id = NULL
                WHERE
                    partition_id = ?
                    AND state = ?
                """,
                (
                    OwnershipState.ACTIVE.value,
                    now,
                    handoff["partition_id"],
                    OwnershipState.HANDING_OFF.value,
                ),
            )

        self._stats["handoffs_aborted"] += 1

        self._record_event(
            "handoff_aborted",
            handoff["source_region_id"],
            handoff["partition_id"],
            handoff_id,
            int(handoff["fencing_epoch"]),
            {
                "reason": reason,
            },
        )

        return True

    # ------------------------------------------------------------------
    # Recovery checkpoints
    # ------------------------------------------------------------------

    def write_checkpoint(
        self,
        partition_id: str,
        region_id: str,
        ownership_epoch: int,
        fencing_epoch: int,
        cursor: str | None,
        sequence: int,
        payload: dict[str, Any] | None = None,
    ) -> RecoveryCheckpoint:

        checkpoint = RecoveryCheckpoint(
            checkpoint_id=f"checkpoint-{uuid.uuid4().hex}",
            partition_id=partition_id,
            region_id=region_id,
            ownership_epoch=ownership_epoch,
            fencing_epoch=fencing_epoch,
            cursor=cursor,
            sequence=sequence,
            created_at=self._now(),
            payload=payload or {},
        )

        self.backend.execute(
            """
            INSERT INTO recovery_checkpoints(
                checkpoint_id,
                partition_id,
                region_id,
                ownership_epoch,
                fencing_epoch,
                cursor,
                sequence,
                created_at,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                checkpoint.checkpoint_id,
                checkpoint.partition_id,
                checkpoint.region_id,
                checkpoint.ownership_epoch,
                checkpoint.fencing_epoch,
                checkpoint.cursor,
                checkpoint.sequence,
                checkpoint.created_at,
                json.dumps(
                    checkpoint.payload,
                    sort_keys=True,
                ),
            ),
        )

        self._stats["checkpoints_written"] += 1

        return checkpoint

    def latest_checkpoint(
        self,
        partition_id: str,
    ) -> RecoveryCheckpoint | None:

        row = self.backend.execute(
            """
            SELECT *
            FROM recovery_checkpoints
            WHERE partition_id = ?
            ORDER BY sequence DESC, created_at DESC
            LIMIT 1
            """,
            (partition_id,),
        ).fetchone()

        if row is None:
            return None

        return RecoveryCheckpoint(
            checkpoint_id=row["checkpoint_id"],
            partition_id=row["partition_id"],
            region_id=row["region_id"],
            ownership_epoch=int(
                row["ownership_epoch"]
            ),
            fencing_epoch=int(
                row["fencing_epoch"]
            ),
            cursor=row["cursor"],
            sequence=int(row["sequence"]),
            created_at=float(row["created_at"]),
            payload=json.loads(
                row["payload_json"]
            ),
        )

    # ------------------------------------------------------------------
    # Failure detection
    # ------------------------------------------------------------------

    def detect_region_health(
        self,
        now: float | None = None,
    ) -> list[RegionRecord]:

        current = now if now is not None else self._now()

        regions = self.list_regions()

        for region in regions:

            if region.state in {
                RegionState.REMOVED.value,
                RegionState.FAILED.value,
            }:
                continue

            age = current - region.last_heartbeat

            if age <= self.heartbeat_timeout:
                health = RegionHealth.HEALTHY

            elif age <= self.failure_confirmation_timeout:
                health = RegionHealth.SUSPECTED

            else:
                health = RegionHealth.FAILED

            if health.value != region.health:
                self._update_region_health(
                    region.region_id,
                    health,
                )

            if health == RegionHealth.FAILED:
                self.mark_region_failed(
                    region.region_id,
                    "heartbeat_timeout",
                )

        return self.list_regions()

    def _update_region_health(
        self,
        region_id: str,
        health: RegionHealth,
    ) -> None:

        self.backend.execute(
            """
            UPDATE regions
            SET
                health = ?,
                state = CASE
                    WHEN ? = ?
                         AND state = ?
                    THEN ?
                    ELSE state
                END,
                updated_at = ?
            WHERE region_id = ?
            """,
            (
                health.value,
                health.value,
                RegionHealth.DEGRADED.value,
                RegionState.ACTIVE.value,
                RegionState.DEGRADED.value,
                self._now(),
                region_id,
            ),
        )

    def mark_region_failed(
        self,
        region_id: str,
        reason: str,
    ) -> bool:

        epoch = self.advance_fencing_epoch(
            f"region_failed:{region_id}"
        )

        with self.backend.transaction() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM regions
                WHERE region_id = ?
                """,
                (region_id,),
            ).fetchone()

            if row is None:
                return False

            if row["state"] == RegionState.FAILED.value:
                return True

            connection.execute(
                """
                UPDATE regions
                SET
                    state = ?,
                    health = ?,
                    fencing_epoch = ?,
                    updated_at = ?
                WHERE region_id = ?
                """,
                (
                    RegionState.FAILED.value,
                    RegionHealth.FAILED.value,
                    epoch,
                    self._now(),
                    region_id,
                ),
            )

            self._fence_region_partitions_locked(
                connection,
                region_id,
                epoch,
                reason,
            )

        self._stats["region_failures"] += 1

        self._record_event(
            "region_failed",
            region_id,
            None,
            None,
            epoch,
            {
                "reason": reason,
            },
        )

        return True

    def recover_region(
        self,
        region_id: str,
    ) -> bool:

        epoch = self.advance_fencing_epoch(
            f"region_recover:{region_id}"
        )

        now = self._now()

        with self.backend.transaction() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM regions
                WHERE region_id = ?
                """,
                (region_id,),
            ).fetchone()

            if row is None:
                return False

            connection.execute(
                """
                UPDATE regions
                SET
                    state = ?,
                    health = ?,
                    fencing_epoch = ?,
                    last_heartbeat = ?,
                    updated_at = ?
                WHERE region_id = ?
                """,
                (
                    RegionState.RECOVERING.value,
                    RegionHealth.DEGRADED.value,
                    epoch,
                    now,
                    now,
                    region_id,
                ),
            )

        self._stats["region_recoveries"] += 1

        self._record_event(
            "region_recovering",
            region_id,
            None,
            None,
            epoch,
            {},
        )

        return True

    # ------------------------------------------------------------------
    # Failover selection
    # ------------------------------------------------------------------

    def select_failover_region(
        self,
        failed_region_id: str,
        partition_id: str,
    ) -> str | None:

        candidates = []

        for region in self.list_regions():

            if region.region_id == failed_region_id:
                continue

            if region.state not in {
                RegionState.ACTIVE.value,
                RegionState.DEGRADED.value,
                RegionState.RECOVERING.value,
            }:
                continue

            if region.health == RegionHealth.FAILED.value:
                continue

            if region.available_capacity_units <= 0:
                continue

            # Avoid immediately preferring the same failure domain.
            same_zone = region.zone_id == (
                self.get_region(
                    failed_region_id
                ).zone_id
                if self.get_region(
                    failed_region_id
                )
                else None
            )

            score = self.stable_partition_score(
                partition_id,
                region.region_id,
            )

            candidates.append(
                (
                    0 if same_zone else 1,
                    region.available_capacity_units,
                    score,
                    region.region_id,
                )
            )

        if not candidates:
            return None

        candidates.sort(
            key=lambda value: (
                -value[0],
                -value[1],
                -value[2],
                value[3],
            )
        )

        return candidates[0][3]

    def create_failover_decision(
        self,
        partition_id: str,
        failed_region_id: str,
    ) -> FailoverDecision:

        target = self.select_failover_region(
            failed_region_id,
            partition_id,
        )

        epoch = self.advance_fencing_epoch(
            f"failover:{partition_id}"
        )

        decision = FailoverDecision(
            partition_id=partition_id,
            failed_region_id=failed_region_id,
            target_region_id=target,
            reason=(
                "region_failure"
                if target
                else "no_eligible_region_capacity"
            ),
            fencing_epoch=epoch,
            created_at=self._now(),
        )

        self._stats["failover_decisions"] += 1

        self._record_event(
            "failover_decision",
            failed_region_id,
            partition_id,
            None,
            epoch,
            asdict(decision),
        )

        return decision

    # ------------------------------------------------------------------
    # Automatic failover
    # ------------------------------------------------------------------

    def failover_partition(
        self,
        partition_id: str,
        failed_region_id: str,
    ) -> CrossRegionHandoff | None:

        decision = self.create_failover_decision(
            partition_id,
            failed_region_id,
        )

        if decision.target_region_id is None:
            return None

        ownership = self.get_partition_ownership(
            partition_id
        )

        if ownership is None:
            return None

        self.fence_partition(
            partition_id,
            "source_region_failed",
        )

        # Re-acquire under the target region's current
        # fencing epoch after the source has been fenced.
        target = self.acquire_partition(
            partition_id,
            decision.target_region_id,
            owner_id=self.owner_id,
        )

        if target is None:
            return None

        checkpoint = self.latest_checkpoint(
            partition_id
        )

        handoff = self.prepare_handoff(
            partition_id,
            decision.target_region_id,
            checkpoint=(
                checkpoint.checkpoint_id
                if checkpoint
                else None
            ),
        )

        if handoff is None:
            return None

        self.begin_handoff_transfer(
            handoff.handoff_id
        )

        return self.get_handoff(
            handoff.handoff_id
        )

    # ------------------------------------------------------------------
    # Region capacity
    # ------------------------------------------------------------------

    def capacity(
        self,
        region_id: str,
    ) -> RegionCapacity | None:

        region = self.get_region(region_id)

        if region is None:
            return None

        partitions = self.list_region_partitions(
            region_id
        )

        active = sum(
            1
            for partition in partitions
            if partition.state
            == OwnershipState.ACTIVE.value
        )

        handing_off = sum(
            1
            for partition in partitions
            if partition.state
            == OwnershipState.HANDING_OFF.value
        )

        return RegionCapacity(
            region_id=region_id,
            capacity_units=region.capacity_units,
            used_capacity_units=region.used_capacity_units,
            available_capacity_units=(
                region.available_capacity_units
            ),
            active_partitions=active,
            handing_off_partitions=handing_off,
            health=region.health,
            state=region.state,
        )

    # ------------------------------------------------------------------
    # Recovery / expired work
    # ------------------------------------------------------------------

    def recover_expired_handoffs(
        self,
        now: float | None = None,
    ) -> int:

        current = now if now is not None else self._now()

        rows = self.backend.execute(
            """
            SELECT handoff_id
            FROM handoffs
            WHERE
                state IN (?, ?)
                AND lease_until <= ?
            ORDER BY updated_at
            """,
            (
                HandoffState.PREPARING.value,
                HandoffState.TRANSFERRING.value,
                current,
            ),
        ).fetchall()

        recovered = 0

        for row in rows:
            handoff_id = row["handoff_id"]

            handoff = self.get_handoff(
                handoff_id
            )

            if handoff is None:
                continue

            if handoff.attempts >= self.max_attempts:
                self.abort_handoff(
                    handoff_id,
                    "handoff_attempt_limit_exceeded",
                )
                continue

            result = self.backend.execute(
                """
                UPDATE handoffs
                SET
                    state = ?,
                    updated_at = ?,
                    lease_until = ?,
                    attempts = attempts + 1
                WHERE
                    handoff_id = ?
                    AND state IN (?, ?)
                """,
                (
                    HandoffState.RECOVERING.value,
                    current,
                    current + self.handoff_timeout,
                    handoff_id,
                    HandoffState.PREPARING.value,
                    HandoffState.TRANSFERRING.value,
                ),
            )

            if result.rowcount:
                recovered += 1

                self._record_event(
                    "handoff_recovered",
                    handoff.source_region_id,
                    handoff.partition_id,
                    handoff_id,
                    handoff.fencing_epoch,
                    {},
                )

        self._stats["handoffs_recovered"] += recovered

        return recovered

    def recover_expired_partitions(
        self,
        now: float | None = None,
    ) -> int:

        current = now if now is not None else self._now()

        rows = self.backend.execute(
            """
            SELECT *
            FROM partition_ownership
            WHERE
                state = ?
                AND lease_until <= ?
            ORDER BY updated_at
            """,
            (
                OwnershipState.ACTIVE.value,
                current,
            ),
        ).fetchall()

        recovered = 0

        for row in rows:
            region = self.get_region(
                row["region_id"]
            )

            if region is None:
                continue

            if region.state in {
                RegionState.FAILED.value,
                RegionState.ISOLATED.value,
                RegionState.REMOVED.value,
            }:
                continue

            acquired = self.acquire_partition(
                row["partition_id"],
                row["region_id"],
                owner_id=self.owner_id,
            )

            if acquired is not None:
                recovered += 1

        return recovered

    # ------------------------------------------------------------------
    # Internal fencing
    # ------------------------------------------------------------------

    def _fence_region_locked(
        self,
        connection: sqlite3.Connection,
        region_id: str,
        reason: str,
    ) -> None:

        epoch = self.current_fencing_epoch()

        connection.execute(
            """
            UPDATE regions
            SET
                state = ?,
                health = ?,
                fencing_epoch = ?,
                updated_at = ?
            WHERE region_id = ?
            """,
            (
                RegionState.ISOLATED.value,
                RegionHealth.SUSPECTED.value,
                epoch,
                self._now(),
                region_id,
            ),
        )

        self._fence_region_partitions_locked(
            connection,
            region_id,
            epoch,
            reason,
        )

    def _fence_region_partitions_locked(
        self,
        connection: sqlite3.Connection,
        region_id: str,
        fencing_epoch: int,
        reason: str,
    ) -> None:

        connection.execute(
            """
            UPDATE partition_ownership
            SET
                state = ?,
                fencing_epoch = ?,
                updated_at = ?
            WHERE
                region_id = ?
                AND state = ?
            """,
            (
                OwnershipState.FENCED.value,
                fencing_epoch,
                self._now(),
                region_id,
                OwnershipState.ACTIVE.value,
            ),
        )

    # ------------------------------------------------------------------
    # Event journal
    # ------------------------------------------------------------------

    def _record_event(
        self,
        event_type: str,
        region_id: str | None,
        partition_id: str | None,
        handoff_id: str | None,
        fencing_epoch: int,
        payload: dict[str, Any],
    ) -> CoordinationEvent:

        event = CoordinationEvent(
            event_id=f"event-{uuid.uuid4().hex}",
            event_type=event_type,
            region_id=region_id,
            partition_id=partition_id,
            handoff_id=handoff_id,
            fencing_epoch=fencing_epoch,
            created_at=self._now(),
            payload=payload,
        )

        self.backend.execute(
            """
            INSERT OR IGNORE INTO coordination_events(
                event_id,
                event_type,
                region_id,
                partition_id,
                handoff_id,
                fencing_epoch,
                created_at,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.event_type,
                event.region_id,
                event.partition_id,
                event.handoff_id,
                event.fencing_epoch,
                event.created_at,
                json.dumps(
                    event.payload,
                    sort_keys=True,
                ),
            ),
        )

        return event

    def events(
        self,
        limit: int = 1000,
    ) -> list[CoordinationEvent]:

        limit = max(1, min(int(limit), 1_000_000))

        rows = self.backend.execute(
            """
            SELECT *
            FROM coordination_events
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        return [
            CoordinationEvent(
                event_id=row["event_id"],
                event_type=row["event_type"],
                region_id=row["region_id"],
                partition_id=row["partition_id"],
                handoff_id=row["handoff_id"],
                fencing_epoch=int(
                    row["fencing_epoch"]
                ),
                created_at=float(
                    row["created_at"]
                ),
                payload=json.loads(
                    row["payload_json"]
                ),
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def cycle(self) -> dict[str, Any]:

        self.detect_region_health()
        self.recover_expired_handoffs()
        self.recover_expired_partitions()

        return self.stats()

    def run(
        self,
        interval: float = 5.0,
    ) -> None:

        if interval <= 0:
            raise ValueError("interval must be positive")

        if self.running:
            return

        self.running = True

        while self.running:
            try:
                self.cycle()
            except Exception:
                # Coordination infrastructure must not silently terminate
                # its supervisory loop because of one recoverable cycle error.
                pass

            time.sleep(interval)

    def stop(self) -> None:
        self.running = False

    # ------------------------------------------------------------------
    # Stats / architecture declaration
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:

        regions = self.list_regions()

        ownership_count = self.backend.execute(
            """
            SELECT COUNT(*)
            AS count
            FROM partition_ownership
            """
        ).fetchone()["count"]

        active_ownership = self.backend.execute(
            """
            SELECT COUNT(*)
            AS count
            FROM partition_ownership
            WHERE state = ?
            """,
            (OwnershipState.ACTIVE.value,),
        ).fetchone()["count"]

        handoffs = self.backend.execute(
            """
            SELECT COUNT(*)
            AS count
            FROM handoffs
            """
        ).fetchone()["count"]

        active_handoffs = self.backend.execute(
            """
            SELECT COUNT(*)
            AS count
            FROM handoffs
            WHERE state IN (?, ?, ?)
            """,
            (
                HandoffState.PREPARING.value,
                HandoffState.TRANSFERRING.value,
                HandoffState.RECOVERING.value,
            ),
        ).fetchone()["count"]

        return {
            "architecture_version": ARCHITECTURE_VERSION,

            "regions": len(regions),
            "active_regions": sum(
                1
                for region in regions
                if region.state
                == RegionState.ACTIVE.value
            ),
            "degraded_regions": sum(
                1
                for region in regions
                if region.state
                == RegionState.DEGRADED.value
            ),
            "failed_regions": sum(
                1
                for region in regions
                if region.state
                == RegionState.FAILED.value
            ),

            "tracked_partitions": int(
                ownership_count
            ),
            "active_partition_ownership": int(
                active_ownership
            ),

            "handoffs": int(handoffs),
            "active_handoffs": int(
                active_handoffs
            ),

            "fencing_epoch": self.current_fencing_epoch(),

            "fixed_global_execution_limit": False,
            "fixed_region_count_limit": False,
            "fixed_partition_count_limit": False,

            "cross_region_coordination": True,
            "cross_region_failover": True,
            "region_health_monitoring": True,
            "region_membership": True,
            "generation_fencing": True,
            "epoch_fencing": True,
            "split_brain_protection": True,
            "partition_ownership": True,
            "cross_region_handoff": True,
            "durable_checkpoints": True,
            "failure_recovery": True,
            "network_isolation_protection": True,
            "capacity_aware_failover": True,
            "failure_domain_awareness": True,
            "idempotent_handoff_records": True,
            "durable_event_journal": True,

            "distributed_backend_abstraction": True,
            "local_sqlite_is_reference_backend": True,

            "scale_target": (
                "billions_to_trillions_of_public_web_resources"
            ),
            "google_scale_capability_target": True,

            "stats": dict(self._stats),
        }


# ---------------------------------------------------------------------------
# Public alias
# ---------------------------------------------------------------------------

CrossRegionCoordinator = CrossRegionCoordinationFailover


__all__ = [
    "ARCHITECTURE_VERSION",
    "RegionState",
    "RegionHealth",
    "OwnershipState",
    "HandoffState",
    "RegionIdentity",
    "RegionRecord",
    "RegionLease",
    "PartitionOwnership",
    "CrossRegionHandoff",
    "RecoveryCheckpoint",
    "RegionCapacity",
    "FailoverDecision",
    "CoordinationEvent",
    "CrossRegionMetadataBackend",
    "SQLiteCrossRegionMetadataBackend",
    "CrossRegionCoordinationFailover",
    "CrossRegionCoordinator",
]
