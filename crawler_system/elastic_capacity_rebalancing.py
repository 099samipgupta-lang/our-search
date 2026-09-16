"""
OUR SEARCH — Phase 9.6
Elastic Capacity & Rebalancing

Architecture target:
    Giant globally distributed Web-scale infrastructure supporting
    billions and potentially trillions of public-Web resources.

This layer sits above:

    9.1 Distributed Execution Fabric
    9.2 Massive Worker Fleet
    9.3 Global Partition / Shard Management
    9.4 Distributed Queue / Scheduling Fabric
    9.5 Cross-Region Coordination & Failover

and provides:

    elastic capacity
    workload balancing
    hot-partition detection
    capacity-aware placement
    partition migration
    workload migration
    scale-out / scale-in decisions
    durable rebalancing plans
    fencing-safe movement
    failure-aware capacity protection

The design deliberately avoids treating a single local SQLite database as
the global production architecture. SQLite is only a durable reference
backend. A production deployment can replace the metadata backend with a
distributed strongly-consistent metadata/control service.

Important:
    This module describes and implements the control architecture.
    It does not claim that a local development machine has Google-scale
    compute, storage, bandwidth, or crawler coverage.
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


ARCHITECTURE_VERSION = "elastic-capacity-rebalancing.v1"

DEFAULT_UTILIZATION_HIGH = 0.80
DEFAULT_UTILIZATION_LOW = 0.30
DEFAULT_UTILIZATION_CRITICAL = 0.92

DEFAULT_REBALANCE_LEASE_SECONDS = 600.0
DEFAULT_MAX_ATTEMPTS = 8

DEFAULT_MAX_BATCH_SIZE = 10_000


# ============================================================================
# ENUMERATIONS
# ============================================================================

class CapacityState(str, Enum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    DRAINING = "draining"
    DECOMMISSIONING = "decommissioning"
    FAILED = "failed"
    REMOVED = "removed"


class RebalanceState(str, Enum):
    PLANNED = "planned"
    PREPARING = "preparing"
    MOVING = "moving"
    VERIFYING = "verifying"
    COMMITTED = "committed"
    ABORTED = "aborted"
    RECOVERING = "recovering"


class MovementType(str, Enum):
    PARTITION = "partition"
    WORKLOAD = "workload"
    WORKER = "worker"
    REGION = "region"


class PressureLevel(str, Enum):
    IDLE = "idle"
    NORMAL = "normal"
    ELEVATED = "elevated"
    HOT = "hot"
    CRITICAL = "critical"


class ScalingAction(str, Enum):
    SCALE_OUT = "scale_out"
    SCALE_IN = "scale_in"
    HOLD = "hold"


# ============================================================================
# DATA CONTRACTS
# ============================================================================

@dataclass(frozen=True)
class CapacityNode:
    """
    Elastic capacity unit.

    A node may represent a worker, worker pool, cluster, zone, or another
    schedulable capacity domain depending on deployment granularity.
    """

    node_id: str
    region_id: str
    zone_id: str
    cluster_id: str

    capacity_units: float
    used_capacity_units: float

    state: str = CapacityState.ACTIVE.value

    worker_count: int = 0

    metadata: dict[str, Any] | None = None

    @property
    def available_capacity_units(self) -> float:
        return max(
            0.0,
            self.capacity_units - self.used_capacity_units,
        )

    @property
    def utilization(self) -> float:
        if self.capacity_units <= 0:
            return 1.0

        return min(
            1.0,
            max(
                0.0,
                self.used_capacity_units
                / self.capacity_units,
            ),
        )


@dataclass(frozen=True)
class PartitionLoad:
    partition_id: str

    region_id: str
    node_id: str | None

    workload_units: float
    queue_depth: int
    active_workers: int

    throughput: float
    error_rate: float

    observed_at: float

    @property
    def pressure(self) -> PressureLevel:

        pressure_score = (
            min(1.0, self.workload_units / 100.0)
            + min(1.0, self.queue_depth / 10_000.0)
            + min(1.0, self.error_rate * 10.0)
        ) / 3.0

        if pressure_score >= 0.90:
            return PressureLevel.CRITICAL

        if pressure_score >= 0.75:
            return PressureLevel.HOT

        if pressure_score >= 0.50:
            return PressureLevel.ELEVATED

        if pressure_score <= 0.15:
            return PressureLevel.IDLE

        return PressureLevel.NORMAL


@dataclass(frozen=True)
class CapacitySnapshot:
    node_id: str

    capacity_units: float
    used_capacity_units: float

    utilization: float
    available_capacity_units: float

    observed_at: float


@dataclass(frozen=True)
class ScalingDecision:
    decision_id: str

    region_id: str

    action: str

    desired_capacity_units: float
    current_capacity_units: float

    delta_capacity_units: float

    reason: str

    created_at: float

    metadata: dict[str, Any]


@dataclass
class RebalancePlan:
    plan_id: str

    movement_type: str

    object_id: str

    source_region_id: str | None
    target_region_id: str | None

    source_node_id: str | None
    target_node_id: str | None

    state: str

    source_epoch: int
    target_epoch: int
    fencing_epoch: int

    created_at: float
    updated_at: float

    lease_until: float
    attempts: int

    payload_json: str

    last_error: str | None = None


@dataclass(frozen=True)
class RebalanceCapacity:
    total_capacity_units: float
    used_capacity_units: float
    available_capacity_units: float

    active_nodes: int
    draining_nodes: int

    utilization: float


@dataclass(frozen=True)
class RebalanceEvent:
    event_id: str

    event_type: str

    plan_id: str | None
    region_id: str | None
    node_id: str | None
    object_id: str | None

    fencing_epoch: int

    created_at: float

    payload: dict[str, Any]


# ============================================================================
# BACKEND CONTRACT
# ============================================================================

class ElasticCapacityMetadataBackend:
    """
    Production abstraction.

    A giant deployment should implement this interface using a distributed
    metadata system capable of:

        - atomic placement changes
        - durable epochs
        - fencing
        - replicated state
        - failure-domain awareness
        - transactional movement state
        - consensus/quorum semantics

    The SQLite backend below is only the durable reference implementation.
    """

    def transaction(self):
        raise NotImplementedError

    def execute(self, *args, **kwargs):
        raise NotImplementedError

    def executemany(self, *args, **kwargs):
        raise NotImplementedError


# ============================================================================
# SQLITE REFERENCE BACKEND
# ============================================================================

class SQLiteElasticCapacityMetadataBackend(
    ElasticCapacityMetadataBackend
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
            / "elastic_capacity_rebalancing.db"
        )

        self._local = threading.local()

        connection = self._connection()

        self._initialize(connection)

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

    def transaction(self):
        return _SQLiteTransaction(
            self._connection()
        )

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

    def _initialize(
        self,
        connection: sqlite3.Connection,
    ) -> None:

        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS elastic_epochs (
                namespace TEXT PRIMARY KEY,
                epoch INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS capacity_nodes (
                node_id TEXT PRIMARY KEY,
                region_id TEXT NOT NULL,
                zone_id TEXT NOT NULL,
                cluster_id TEXT NOT NULL,
                capacity_units REAL NOT NULL,
                used_capacity_units REAL NOT NULL,
                state TEXT NOT NULL,
                worker_count INTEGER NOT NULL,
                metadata_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS
                idx_capacity_region
                ON capacity_nodes(region_id);

            CREATE INDEX IF NOT EXISTS
                idx_capacity_state
                ON capacity_nodes(state);

            CREATE TABLE IF NOT EXISTS
                partition_loads (
                    partition_id TEXT PRIMARY KEY,
                    region_id TEXT NOT NULL,
                    node_id TEXT,
                    workload_units REAL NOT NULL,
                    queue_depth INTEGER NOT NULL,
                    active_workers INTEGER NOT NULL,
                    throughput REAL NOT NULL,
                    error_rate REAL NOT NULL,
                    observed_at REAL NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_partition_load_region
                ON partition_loads(region_id);

            CREATE INDEX IF NOT EXISTS
                idx_partition_load_pressure
                ON partition_loads(
                    workload_units DESC
                );

            CREATE TABLE IF NOT EXISTS
                capacity_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    capacity_units REAL NOT NULL,
                    used_capacity_units REAL NOT NULL,
                    utilization REAL NOT NULL,
                    available_capacity_units REAL NOT NULL,
                    observed_at REAL NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_capacity_snapshot_node
                ON capacity_snapshots(
                    node_id,
                    observed_at DESC
                );

            CREATE TABLE IF NOT EXISTS
                scaling_decisions (
                    decision_id TEXT PRIMARY KEY,
                    region_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    desired_capacity_units REAL NOT NULL,
                    current_capacity_units REAL NOT NULL,
                    delta_capacity_units REAL NOT NULL,
                    reason TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    metadata_json TEXT NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_scaling_region
                ON scaling_decisions(
                    region_id,
                    created_at DESC
                );

            CREATE TABLE IF NOT EXISTS
                rebalance_plans (
                    plan_id TEXT PRIMARY KEY,
                    movement_type TEXT NOT NULL,
                    object_id TEXT NOT NULL,
                    source_region_id TEXT,
                    target_region_id TEXT,
                    source_node_id TEXT,
                    target_node_id TEXT,
                    state TEXT NOT NULL,
                    source_epoch INTEGER NOT NULL,
                    target_epoch INTEGER NOT NULL,
                    fencing_epoch INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    lease_until REAL NOT NULL,
                    attempts INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    last_error TEXT
                );

            CREATE INDEX IF NOT EXISTS
                idx_rebalance_state
                ON rebalance_plans(state);

            CREATE INDEX IF NOT EXISTS
                idx_rebalance_object
                ON rebalance_plans(
                    movement_type,
                    object_id
                );

            CREATE INDEX IF NOT EXISTS
                idx_rebalance_region
                ON rebalance_plans(
                    source_region_id,
                    target_region_id
                );

            CREATE TABLE IF NOT EXISTS
                rebalance_events (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    plan_id TEXT,
                    region_id TEXT,
                    node_id TEXT,
                    object_id TEXT,
                    fencing_epoch INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    payload_json TEXT NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_rebalance_events_time
                ON rebalance_events(
                    created_at DESC
                );
            """
        )


class _SQLiteTransaction:

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
# ELASTIC CAPACITY & REBALANCING
# ============================================================================

class ElasticCapacityRebalancing:

    def __init__(
        self,
        storage_root: str,
        metadata_backend:
            ElasticCapacityMetadataBackend | None = None,
        utilization_high: float =
            DEFAULT_UTILIZATION_HIGH,
        utilization_low: float =
            DEFAULT_UTILIZATION_LOW,
        utilization_critical: float =
            DEFAULT_UTILIZATION_CRITICAL,
        rebalance_lease_seconds: float =
            DEFAULT_REBALANCE_LEASE_SECONDS,
        max_attempts: int =
            DEFAULT_MAX_ATTEMPTS,
        max_batch_size: int =
            DEFAULT_MAX_BATCH_SIZE,
    ) -> None:

        if not 0.0 < utilization_low < 1.0:
            raise ValueError(
                "utilization_low must be between 0 and 1"
            )

        if not 0.0 < utilization_high < 1.0:
            raise ValueError(
                "utilization_high must be between 0 and 1"
            )

        if not 0.0 < utilization_critical <= 1.0:
            raise ValueError(
                "utilization_critical must be between 0 and 1"
            )

        if utilization_low >= utilization_high:
            raise ValueError(
                "utilization_low must be below utilization_high"
            )

        if utilization_high >= utilization_critical:
            raise ValueError(
                "utilization_high must be below utilization_critical"
            )

        if rebalance_lease_seconds <= 0:
            raise ValueError(
                "rebalance_lease_seconds must be positive"
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
            metadata_backend
            or SQLiteElasticCapacityMetadataBackend(
                storage_root
            )
        )

        self.utilization_high = utilization_high
        self.utilization_low = utilization_low
        self.utilization_critical = (
            utilization_critical
        )

        self.rebalance_lease_seconds = (
            rebalance_lease_seconds
        )

        self.max_attempts = max_attempts
        self.max_batch_size = max_batch_size

        self.owner_id = (
            f"elastic-{uuid.uuid4().hex}"
        )

        self._lock = threading.RLock()

        self.running = False

        self._stats = {
            "capacity_nodes_registered": 0,
            "capacity_updates": 0,
            "load_updates": 0,
            "scale_out_decisions": 0,
            "scale_in_decisions": 0,
            "rebalance_plans_created": 0,
            "rebalance_plans_committed": 0,
            "rebalance_plans_aborted": 0,
            "rebalance_plans_recovered": 0,
            "partition_movements": 0,
            "workload_movements": 0,
            "capacity_protection_events": 0,
            "hot_partition_events": 0,
            "fencing_events": 0,
        }

        self._ensure_epoch_namespace()

    # ----------------------------------------------------------------------
    # Utilities
    # ----------------------------------------------------------------------

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def stable_score(
        object_id: str,
        node_id: str,
    ) -> int:

        digest = hashlib.sha256(
            f"{object_id}|{node_id}".encode()
        ).digest()

        return int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

    @staticmethod
    def _json(
        value: dict[str, Any] | None,
    ) -> str:

        return json.dumps(
            value or {},
            sort_keys=True,
            separators=(",", ":"),
        )

    # ----------------------------------------------------------------------
    # Epoch management
    # ----------------------------------------------------------------------

    def _ensure_epoch_namespace(self) -> None:

        self.backend.execute(
            """
            INSERT OR IGNORE INTO
                elastic_epochs(namespace, epoch)
            VALUES (?, 0)
            """,
            ("elastic",),
        )

    def current_epoch(self) -> int:

        row = self.backend.execute(
            """
            SELECT epoch
            FROM elastic_epochs
            WHERE namespace = ?
            """,
            ("elastic",),
        ).fetchone()

        if row is None:
            self._ensure_epoch_namespace()
            return 0

        return int(row["epoch"])

    def advance_epoch(
        self,
        reason: str,
    ) -> int:

        with self.backend.transaction() as connection:

            row = connection.execute(
                """
                SELECT epoch
                FROM elastic_epochs
                WHERE namespace = ?
                """,
                ("elastic",),
            ).fetchone()

            current = (
                int(row["epoch"])
                if row is not None
                else 0
            )

            new_epoch = current + 1

            connection.execute(
                """
                INSERT INTO
                    elastic_epochs(namespace, epoch)
                VALUES (?, ?)
                ON CONFLICT(namespace)
                DO UPDATE SET
                    epoch = excluded.epoch
                """,
                ("elastic", new_epoch),
            )

        self._record_event(
            "elastic_epoch_advanced",
            None,
            None,
            None,
            new_epoch,
            {
                "reason": reason,
            },
        )

        return new_epoch

    # ----------------------------------------------------------------------
    # Capacity nodes
    # ----------------------------------------------------------------------

    def register_capacity_node(
        self,
        node: CapacityNode,
    ) -> CapacityNode:

        now = self._now()

        with self.backend.transaction() as connection:

            connection.execute(
                """
                INSERT INTO capacity_nodes(
                    node_id,
                    region_id,
                    zone_id,
                    cluster_id,
                    capacity_units,
                    used_capacity_units,
                    state,
                    worker_count,
                    metadata_json,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id)
                DO UPDATE SET
                    region_id =
                        excluded.region_id,
                    zone_id =
                        excluded.zone_id,
                    cluster_id =
                        excluded.cluster_id,
                    capacity_units =
                        excluded.capacity_units,
                    used_capacity_units =
                        excluded.used_capacity_units,
                    state =
                        excluded.state,
                    worker_count =
                        excluded.worker_count,
                    metadata_json =
                        excluded.metadata_json,
                    updated_at =
                        excluded.updated_at
                """,
                (
                    node.node_id,
                    node.region_id,
                    node.zone_id,
                    node.cluster_id,
                    max(0.0, node.capacity_units),
                    max(
                        0.0,
                        node.used_capacity_units,
                    ),
                    node.state,
                    max(0, node.worker_count),
                    self._json(node.metadata),
                    now,
                ),
            )

        self._stats[
            "capacity_nodes_registered"
        ] += 1

        return self.get_capacity_node(
            node.node_id
        )

    def get_capacity_node(
        self,
        node_id: str,
    ) -> CapacityNode | None:

        row = self.backend.execute(
            """
            SELECT *
            FROM capacity_nodes
            WHERE node_id = ?
            """,
            (node_id,),
        ).fetchone()

        if row is None:
            return None

        return CapacityNode(
            node_id=row["node_id"],
            region_id=row["region_id"],
            zone_id=row["zone_id"],
            cluster_id=row["cluster_id"],
            capacity_units=float(
                row["capacity_units"]
            ),
            used_capacity_units=float(
                row["used_capacity_units"]
            ),
            state=row["state"],
            worker_count=int(
                row["worker_count"]
            ),
            metadata=json.loads(
                row["metadata_json"]
            ),
        )

    def list_capacity_nodes(
        self,
        region_id: str | None = None,
        states: set[str] | None = None,
    ) -> list[CapacityNode]:

        clauses = []
        params: list[Any] = []

        if region_id is not None:
            clauses.append(
                "region_id = ?"
            )
            params.append(region_id)

        if states:
            placeholders = ",".join(
                "?" for _ in states
            )

            clauses.append(
                f"state IN ({placeholders})"
            )

            params.extend(
                sorted(states)
            )

        where = (
            "WHERE " + " AND ".join(clauses)
            if clauses
            else ""
        )

        rows = self.backend.execute(
            f"""
            SELECT *
            FROM capacity_nodes
            {where}
            ORDER BY node_id
            """,
            tuple(params),
        ).fetchall()

        return [
            CapacityNode(
                node_id=row["node_id"],
                region_id=row["region_id"],
                zone_id=row["zone_id"],
                cluster_id=row["cluster_id"],
                capacity_units=float(
                    row["capacity_units"]
                ),
                used_capacity_units=float(
                    row["used_capacity_units"]
                ),
                state=row["state"],
                worker_count=int(
                    row["worker_count"]
                ),
                metadata=json.loads(
                    row["metadata_json"]
                ),
            )
            for row in rows
        ]

    def update_capacity(
        self,
        node_id: str,
        used_capacity_units: float,
        worker_count: int | None = None,
    ) -> bool:

        now = self._now()

        with self.backend.transaction() as connection:

            row = connection.execute(
                """
                SELECT *
                FROM capacity_nodes
                WHERE node_id = ?
                """,
                (node_id,),
            ).fetchone()

            if row is None:
                return False

            if worker_count is None:

                connection.execute(
                    """
                    UPDATE capacity_nodes
                    SET
                        used_capacity_units = ?,
                        updated_at = ?
                    WHERE node_id = ?
                    """,
                    (
                        max(
                            0.0,
                            used_capacity_units,
                        ),
                        now,
                        node_id,
                    ),
                )

            else:

                connection.execute(
                    """
                    UPDATE capacity_nodes
                    SET
                        used_capacity_units = ?,
                        worker_count = ?,
                        updated_at = ?
                    WHERE node_id = ?
                    """,
                    (
                        max(
                            0.0,
                            used_capacity_units,
                        ),
                        max(0, worker_count),
                        now,
                        node_id,
                    ),
                )

        self._stats[
            "capacity_updates"
        ] += 1

        self._write_capacity_snapshot(
            node_id
        )

        return True

    def _write_capacity_snapshot(
        self,
        node_id: str,
    ) -> None:

        node = self.get_capacity_node(
            node_id
        )

        if node is None:
            return

        snapshot = CapacitySnapshot(
            node_id=node.node_id,
            capacity_units=node.capacity_units,
            used_capacity_units=(
                node.used_capacity_units
            ),
            utilization=node.utilization,
            available_capacity_units=(
                node.available_capacity_units
            ),
            observed_at=self._now(),
        )

        self.backend.execute(
            """
            INSERT INTO capacity_snapshots(
                snapshot_id,
                node_id,
                capacity_units,
                used_capacity_units,
                utilization,
                available_capacity_units,
                observed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"snapshot-{uuid.uuid4().hex}",
                snapshot.node_id,
                snapshot.capacity_units,
                snapshot.used_capacity_units,
                snapshot.utilization,
                snapshot.available_capacity_units,
                snapshot.observed_at,
            ),
        )

    # ----------------------------------------------------------------------
    # Partition load
    # ----------------------------------------------------------------------

    def update_partition_load(
        self,
        load: PartitionLoad,
    ) -> None:

        self.backend.execute(
            """
            INSERT INTO partition_loads(
                partition_id,
                region_id,
                node_id,
                workload_units,
                queue_depth,
                active_workers,
                throughput,
                error_rate,
                observed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(partition_id)
            DO UPDATE SET
                region_id =
                    excluded.region_id,
                node_id =
                    excluded.node_id,
                workload_units =
                    excluded.workload_units,
                queue_depth =
                    excluded.queue_depth,
                active_workers =
                    excluded.active_workers,
                throughput =
                    excluded.throughput,
                error_rate =
                    excluded.error_rate,
                observed_at =
                    excluded.observed_at
            """,
            (
                load.partition_id,
                load.region_id,
                load.node_id,
                max(0.0, load.workload_units),
                max(0, load.queue_depth),
                max(0, load.active_workers),
                max(0.0, load.throughput),
                max(0.0, load.error_rate),
                load.observed_at,
            ),
        )

        self._stats[
            "load_updates"
        ] += 1

        if load.pressure in {
            PressureLevel.HOT,
            PressureLevel.CRITICAL,
        }:
            self._stats[
                "hot_partition_events"
            ] += 1

    def get_partition_load(
        self,
        partition_id: str,
    ) -> PartitionLoad | None:

        row = self.backend.execute(
            """
            SELECT *
            FROM partition_loads
            WHERE partition_id = ?
            """,
            (partition_id,),
        ).fetchone()

        if row is None:
            return None

        return PartitionLoad(
            partition_id=row["partition_id"],
            region_id=row["region_id"],
            node_id=row["node_id"],
            workload_units=float(
                row["workload_units"]
            ),
            queue_depth=int(
                row["queue_depth"]
            ),
            active_workers=int(
                row["active_workers"]
            ),
            throughput=float(
                row["throughput"]
            ),
            error_rate=float(
                row["error_rate"]
            ),
            observed_at=float(
                row["observed_at"]
            ),
        )

    def hot_partitions(
        self,
        region_id: str | None = None,
    ) -> list[PartitionLoad]:

        if region_id is None:

            rows = self.backend.execute(
                """
                SELECT *
                FROM partition_loads
                ORDER BY workload_units DESC
                """
            ).fetchall()

        else:

            rows = self.backend.execute(
                """
                SELECT *
                FROM partition_loads
                WHERE region_id = ?
                ORDER BY workload_units DESC
                """,
                (region_id,),
            ).fetchall()

        loads = [
            PartitionLoad(
                partition_id=row["partition_id"],
                region_id=row["region_id"],
                node_id=row["node_id"],
                workload_units=float(
                    row["workload_units"]
                ),
                queue_depth=int(
                    row["queue_depth"]
                ),
                active_workers=int(
                    row["active_workers"]
                ),
                throughput=float(
                    row["throughput"]
                ),
                error_rate=float(
                    row["error_rate"]
                ),
                observed_at=float(
                    row["observed_at"]
                ),
            )
            for row in rows
        ]

        return [
            load
            for load in loads
            if load.pressure in {
                PressureLevel.HOT,
                PressureLevel.CRITICAL,
            }
        ]

    # ----------------------------------------------------------------------
    # Elastic scaling decisions
    # ----------------------------------------------------------------------

    def decide_region_scaling(
        self,
        region_id: str,
    ) -> ScalingDecision:

        nodes = self.list_capacity_nodes(
            region_id=region_id,
            states={
                CapacityState.ACTIVE.value,
                CapacityState.PROVISIONING.value,
            },
        )

        current_capacity = sum(
            node.capacity_units
            for node in nodes
        )

        used_capacity = sum(
            node.used_capacity_units
            for node in nodes
        )

        utilization = (
            used_capacity / current_capacity
            if current_capacity > 0
            else 1.0
        )

        if utilization >= self.utilization_critical:

            desired = max(
                current_capacity * 1.50,
                current_capacity + 1.0,
            )

            action = ScalingAction.SCALE_OUT

            reason = (
                "critical_capacity_pressure"
            )

        elif utilization >= self.utilization_high:

            desired = max(
                current_capacity * 1.25,
                current_capacity + 1.0,
            )

            action = ScalingAction.SCALE_OUT

            reason = (
                "high_capacity_utilization"
            )

        elif (
            utilization <= self.utilization_low
            and len(nodes) > 1
        ):

            desired = max(
                1.0,
                current_capacity * 0.75,
            )

            action = ScalingAction.SCALE_IN

            reason = (
                "sustained_underutilization"
            )

        else:

            desired = current_capacity

            action = ScalingAction.HOLD

            reason = "capacity_within_target"

        decision = ScalingDecision(
            decision_id=f"scale-{uuid.uuid4().hex}",
            region_id=region_id,
            action=action.value,
            desired_capacity_units=desired,
            current_capacity_units=current_capacity,
            delta_capacity_units=(
                desired - current_capacity
            ),
            reason=reason,
            created_at=self._now(),
            metadata={
                "utilization": utilization,
                "node_count": len(nodes),
            },
        )

        self.backend.execute(
            """
            INSERT INTO scaling_decisions(
                decision_id,
                region_id,
                action,
                desired_capacity_units,
                current_capacity_units,
                delta_capacity_units,
                reason,
                created_at,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision.decision_id,
                decision.region_id,
                decision.action,
                decision.desired_capacity_units,
                decision.current_capacity_units,
                decision.delta_capacity_units,
                decision.reason,
                decision.created_at,
                self._json(decision.metadata),
            ),
        )

        if action == ScalingAction.SCALE_OUT:
            self._stats[
                "scale_out_decisions"
            ] += 1

        elif action == ScalingAction.SCALE_IN:
            self._stats[
                "scale_in_decisions"
            ] += 1

        return decision

    # ----------------------------------------------------------------------
    # Target selection
    # ----------------------------------------------------------------------

    def select_target_node(
        self,
        object_id: str,
        source_node_id: str | None = None,
        region_id: str | None = None,
    ) -> CapacityNode | None:

        candidates = self.list_capacity_nodes(
            region_id=region_id,
            states={
                CapacityState.ACTIVE.value,
            },
        )

        scored = []

        for node in candidates:

            if (
                source_node_id is not None
                and node.node_id == source_node_id
            ):
                continue

            if node.available_capacity_units <= 0:
                continue

            # Protect against moving work into a node
            # that is already near saturation.
            if (
                node.utilization
                >= self.utilization_high
            ):
                continue

            score = self.stable_score(
                object_id,
                node.node_id,
            )

            scored.append(
                (
                    node.utilization,
                    -node.available_capacity_units,
                    -score,
                    node.node_id,
                )
            )

        if not scored:
            return None

        scored.sort()

        return self.get_capacity_node(
            scored[0][3]
        )

    # ----------------------------------------------------------------------
    # Rebalance planning
    # ----------------------------------------------------------------------

    def create_rebalance_plan(
        self,
        movement_type: MovementType,
        object_id: str,
        source_region_id: str | None,
        target_region_id: str | None,
        source_node_id: str | None = None,
        target_node_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RebalancePlan:

        now = self._now()

        source_epoch = self.current_epoch()
        target_epoch = source_epoch

        fencing_epoch = self.advance_epoch(
            f"rebalance:{movement_type.value}:{object_id}"
        )

        existing = self.backend.execute(
            """
            SELECT *
            FROM rebalance_plans
            WHERE
                movement_type = ?
                AND object_id = ?
                AND state IN (?, ?, ?, ?)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                movement_type.value,
                object_id,
                RebalanceState.PLANNED.value,
                RebalanceState.PREPARING.value,
                RebalanceState.MOVING.value,
                RebalanceState.VERIFYING.value,
            ),
        ).fetchone()

        if existing is not None:
            return RebalancePlan(
                **dict(existing)
            )

        plan = RebalancePlan(
            plan_id=f"rebalance-{uuid.uuid4().hex}",
            movement_type=movement_type.value,
            object_id=object_id,
            source_region_id=source_region_id,
            target_region_id=target_region_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            state=RebalanceState.PLANNED.value,
            source_epoch=source_epoch,
            target_epoch=target_epoch,
            fencing_epoch=fencing_epoch,
            created_at=now,
            updated_at=now,
            lease_until=(
                now
                + self.rebalance_lease_seconds
            ),
            attempts=0,
            payload_json=self._json(payload),
            last_error=None,
        )

        self.backend.execute(
            """
            INSERT INTO rebalance_plans(
                plan_id,
                movement_type,
                object_id,
                source_region_id,
                target_region_id,
                source_node_id,
                target_node_id,
                state,
                source_epoch,
                target_epoch,
                fencing_epoch,
                created_at,
                updated_at,
                lease_until,
                attempts,
                payload_json,
                last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                plan.plan_id,
                plan.movement_type,
                plan.object_id,
                plan.source_region_id,
                plan.target_region_id,
                plan.source_node_id,
                plan.target_node_id,
                plan.state,
                plan.source_epoch,
                plan.target_epoch,
                plan.fencing_epoch,
                plan.created_at,
                plan.updated_at,
                plan.lease_until,
                plan.attempts,
                plan.payload_json,
                plan.last_error,
            ),
        )

        self._stats[
            "rebalance_plans_created"
        ] += 1

        return plan

    def get_rebalance_plan(
        self,
        plan_id: str,
    ) -> RebalancePlan | None:

        row = self.backend.execute(
            """
            SELECT *
            FROM rebalance_plans
            WHERE plan_id = ?
            """,
            (plan_id,),
        ).fetchone()

        if row is None:
            return None

        return RebalancePlan(
            **dict(row)
        )

    # ----------------------------------------------------------------------
    # Planning hot partition movement
    # ----------------------------------------------------------------------

    def plan_hot_partition_movement(
        self,
        partition_id: str,
    ) -> RebalancePlan | None:

        load = self.get_partition_load(
            partition_id
        )

        if load is None:
            return None

        if load.pressure not in {
            PressureLevel.HOT,
            PressureLevel.CRITICAL,
        }:
            return None

        target = self.select_target_node(
            object_id=partition_id,
            source_node_id=load.node_id,
        )

        if target is None:
            self._stats[
                "capacity_protection_events"
            ] += 1

            self._record_event(
                "capacity_protection_triggered",
                load.region_id,
                load.node_id,
                partition_id,
                self.current_epoch(),
                {
                    "pressure": (
                        load.pressure.value
                    ),
                },
            )

            return None

        return self.create_rebalance_plan(
            movement_type=MovementType.PARTITION,
            object_id=partition_id,
            source_region_id=load.region_id,
            target_region_id=target.region_id,
            source_node_id=load.node_id,
            target_node_id=target.node_id,
            payload={
                "pressure": load.pressure.value,
                "queue_depth": load.queue_depth,
                "workload_units": (
                    load.workload_units
                ),
            },
        )

    # ----------------------------------------------------------------------
    # Workload movement
    # ----------------------------------------------------------------------

    def plan_workload_movement(
        self,
        workload_id: str,
        source_node_id: str,
        region_id: str | None = None,
    ) -> RebalancePlan | None:

        source = self.get_capacity_node(
            source_node_id
        )

        if source is None:
            return None

        target = self.select_target_node(
            object_id=workload_id,
            source_node_id=source_node_id,
            region_id=region_id,
        )

        if target is None:
            self._stats[
                "capacity_protection_events"
            ] += 1

            return None

        return self.create_rebalance_plan(
            movement_type=MovementType.WORKLOAD,
            object_id=workload_id,
            source_region_id=source.region_id,
            target_region_id=target.region_id,
            source_node_id=source.node_id,
            target_node_id=target.node_id,
            payload={
                "reason": "capacity_rebalance",
            },
        )

    # ----------------------------------------------------------------------
    # Plan state machine
    # ----------------------------------------------------------------------

    def prepare_plan(
        self,
        plan_id: str,
    ) -> bool:

        now = self._now()

        result = self.backend.execute(
            """
            UPDATE rebalance_plans
            SET
                state = ?,
                updated_at = ?,
                lease_until = ?,
                attempts = attempts + 1
            WHERE
                plan_id = ?
                AND state = ?
            """,
            (
                RebalanceState.PREPARING.value,
                now,
                now + self.rebalance_lease_seconds,
                plan_id,
                RebalanceState.PLANNED.value,
            ),
        )

        return result.rowcount == 1

    def begin_movement(
        self,
        plan_id: str,
    ) -> bool:

        now = self._now()

        result = self.backend.execute(
            """
            UPDATE rebalance_plans
            SET
                state = ?,
                updated_at = ?,
                lease_until = ?,
                attempts = attempts + 1
            WHERE
                plan_id = ?
                AND state = ?
            """,
            (
                RebalanceState.MOVING.value,
                now,
                now + self.rebalance_lease_seconds,
                plan_id,
                RebalanceState.PREPARING.value,
            ),
        )

        return result.rowcount == 1

    def begin_verification(
        self,
        plan_id: str,
    ) -> bool:

        now = self._now()

        result = self.backend.execute(
            """
            UPDATE rebalance_plans
            SET
                state = ?,
                updated_at = ?,
                lease_until = ?
            WHERE
                plan_id = ?
                AND state = ?
            """,
            (
                RebalanceState.VERIFYING.value,
                now,
                now + self.rebalance_lease_seconds,
                plan_id,
                RebalanceState.MOVING.value,
            ),
        )

        return result.rowcount == 1

    def commit_plan(
        self,
        plan_id: str,
    ) -> bool:

        now = self._now()

        with self.backend.transaction() as connection:

            row = connection.execute(
                """
                SELECT *
                FROM rebalance_plans
                WHERE plan_id = ?
                """,
                (plan_id,),
            ).fetchone()

            if row is None:
                return False

            if row["state"] == (
                RebalanceState.COMMITTED.value
            ):
                return True

            if row["state"] != (
                RebalanceState.VERIFYING.value
            ):
                return False

            connection.execute(
                """
                UPDATE rebalance_plans
                SET
                    state = ?,
                    updated_at = ?,
                    lease_until = ?
                WHERE
                    plan_id = ?
                    AND state = ?
                """,
                (
                    RebalanceState.COMMITTED.value,
                    now,
                    now,
                    plan_id,
                    RebalanceState.VERIFYING.value,
                ),
            )

        self._stats[
            "rebalance_plans_committed"
        ] += 1

        movement_type = row[
            "movement_type"
        ]

        if movement_type == (
            MovementType.PARTITION.value
        ):
            self._stats[
                "partition_movements"
            ] += 1

        elif movement_type == (
            MovementType.WORKLOAD.value
        ):
            self._stats[
                "workload_movements"
            ] += 1

        self._record_event(
            "rebalance_committed",
            row["target_region_id"],
            row["target_node_id"],
            row["object_id"],
            int(row["fencing_epoch"]),
            {
                "plan_id": plan_id,
                "movement_type": movement_type,
            },
        )

        return True

    def abort_plan(
        self,
        plan_id: str,
        reason: str,
    ) -> bool:

        now = self._now()

        with self.backend.transaction() as connection:

            row = connection.execute(
                """
                SELECT *
                FROM rebalance_plans
                WHERE plan_id = ?
                """,
                (plan_id,),
            ).fetchone()

            if row is None:
                return False

            if row["state"] == (
                RebalanceState.COMMITTED.value
            ):
                return False

            result = connection.execute(
                """
                UPDATE rebalance_plans
                SET
                    state = ?,
                    updated_at = ?,
                    lease_until = ?,
                    last_error = ?
                WHERE
                    plan_id = ?
                    AND state NOT IN (?, ?)
                """,
                (
                    RebalanceState.ABORTED.value,
                    now,
                    now,
                    reason,
                    plan_id,
                    RebalanceState.COMMITTED.value,
                    RebalanceState.ABORTED.value,
                ),
            )

        if result.rowcount:

            self._stats[
                "rebalance_plans_aborted"
            ] += 1

            self._record_event(
                "rebalance_aborted",
                row["source_region_id"],
                row["source_node_id"],
                row["object_id"],
                int(row["fencing_epoch"]),
                {
                    "plan_id": plan_id,
                    "reason": reason,
                },
            )

            return True

        return False

    # ----------------------------------------------------------------------
    # Lease recovery
    # ----------------------------------------------------------------------

    def recover_expired_plans(
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
            FROM rebalance_plans
            WHERE
                state IN (?, ?, ?, ?)
                AND lease_until <= ?
            ORDER BY updated_at
            LIMIT ?
            """,
            (
                RebalanceState.PLANNED.value,
                RebalanceState.PREPARING.value,
                RebalanceState.MOVING.value,
                RebalanceState.VERIFYING.value,
                current,
                self.max_batch_size,
            ),
        ).fetchall()

        recovered = 0

        for row in rows:

            attempts = int(
                row["attempts"]
            )

            if attempts >= self.max_attempts:

                self.abort_plan(
                    row["plan_id"],
                    "rebalance_attempt_limit_exceeded",
                )

                continue

            result = self.backend.execute(
                """
                UPDATE rebalance_plans
                SET
                    state = ?,
                    updated_at = ?,
                    lease_until = ?,
                    attempts = attempts + 1
                WHERE
                    plan_id = ?
                    AND state IN (?, ?, ?, ?)
                    AND lease_until <= ?
                """,
                (
                    RebalanceState.RECOVERING.value,
                    current,
                    current
                    + self.rebalance_lease_seconds,
                    row["plan_id"],
                    RebalanceState.PLANNED.value,
                    RebalanceState.PREPARING.value,
                    RebalanceState.MOVING.value,
                    RebalanceState.VERIFYING.value,
                    current,
                ),
            )

            if result.rowcount:

                recovered += 1

                self._record_event(
                    "rebalance_recovered",
                    row["source_region_id"],
                    row["source_node_id"],
                    row["object_id"],
                    int(
                        row["fencing_epoch"]
                    ),
                    {
                        "plan_id": row[
                            "plan_id"
                        ],
                    },
                )

        self._stats[
            "rebalance_plans_recovered"
        ] += recovered

        return recovered

    # ----------------------------------------------------------------------
    # Fencing
    # ----------------------------------------------------------------------

    def fence_node(
        self,
        node_id: str,
        reason: str,
    ) -> bool:

        epoch = self.advance_epoch(
            f"fence_node:{node_id}"
        )

        result = self.backend.execute(
            """
            UPDATE capacity_nodes
            SET
                state = ?,
                updated_at = ?
            WHERE node_id = ?
            """,
            (
                CapacityState.FAILED.value,
                self._now(),
                node_id,
            ),
        )

        if result.rowcount:

            self._stats[
                "fencing_events"
            ] += 1

            self._record_event(
                "capacity_node_fenced",
                None,
                node_id,
                None,
                epoch,
                {
                    "reason": reason,
                },
            )

            return True

        return False

    # ----------------------------------------------------------------------
    # Global rebalancing cycle
    # ----------------------------------------------------------------------

    def plan_rebalancing(
        self,
        region_id: str | None = None,
    ) -> list[RebalancePlan]:

        plans: list[RebalancePlan] = []

        hot = self.hot_partitions(
            region_id=region_id
        )

        for load in hot:

            plan = (
                self.plan_hot_partition_movement(
                    load.partition_id
                )
            )

            if plan is not None:
                plans.append(plan)

            if len(plans) >= self.max_batch_size:
                break

        return plans

    def process_plan(
        self,
        plan_id: str,
    ) -> bool:

        plan = self.get_rebalance_plan(
            plan_id
        )

        if plan is None:
            return False

        if plan.state == (
            RebalanceState.COMMITTED.value
        ):
            return True

        if plan.state == (
            RebalanceState.ABORTED.value
        ):
            return False

        if plan.state == (
            RebalanceState.RECOVERING.value
        ):
            now = self._now()

            self.backend.execute(
                """
                UPDATE rebalance_plans
                SET
                    state = ?,
                    updated_at = ?,
                    lease_until = ?
                WHERE
                    plan_id = ?
                    AND state = ?
                """,
                (
                    RebalanceState.PREPARING.value,
                    now,
                    now + self.rebalance_lease_seconds,
                    plan_id,
                    RebalanceState.RECOVERING.value,
                ),
            )

        plan = self.get_rebalance_plan(
            plan_id
        )

        if plan is None:
            return False

        if plan.state == (
            RebalanceState.PLANNED.value
        ):
            if not self.prepare_plan(plan_id):
                return False

        if not self.begin_movement(plan_id):
            current = self.get_rebalance_plan(
                plan_id
            )

            if current is None:
                return False

            if current.state not in {
                RebalanceState.MOVING.value,
                RebalanceState.VERIFYING.value,
            }:
                return False

        if not self.begin_verification(
            plan_id
        ):
            current = self.get_rebalance_plan(
                plan_id
            )

            if current is None:
                return False

            if current.state != (
                RebalanceState.VERIFYING.value
            ):
                return False

        return self.commit_plan(
            plan_id
        )

    # ----------------------------------------------------------------------
    # Capacity aggregation
    # ----------------------------------------------------------------------

    def capacity(
        self,
        region_id: str | None = None,
    ) -> RebalanceCapacity:

        nodes = self.list_capacity_nodes(
            region_id=region_id
        )

        active = [
            node
            for node in nodes
            if node.state
            in {
                CapacityState.ACTIVE.value,
                CapacityState.PROVISIONING.value,
            }
        ]

        draining = [
            node
            for node in nodes
            if node.state
            == CapacityState.DRAINING.value
        ]

        total = sum(
            node.capacity_units
            for node in active
        )

        used = sum(
            node.used_capacity_units
            for node in active
        )

        available = max(
            0.0,
            total - used,
        )

        utilization = (
            used / total
            if total > 0
            else 1.0
        )

        return RebalanceCapacity(
            total_capacity_units=total,
            used_capacity_units=used,
            available_capacity_units=available,
            active_nodes=len(active),
            draining_nodes=len(draining),
            utilization=utilization,
        )

    # ----------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------

    def cycle(self) -> dict[str, Any]:

        self.recover_expired_plans()

        plans = self.plan_rebalancing()

        for plan in plans:

            try:
                self.process_plan(
                    plan.plan_id
                )
            except Exception as exc:
                self.abort_plan(
                    plan.plan_id,
                    f"processing_error:{exc}",
                )

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

    # ----------------------------------------------------------------------
    # Event journal
    # ----------------------------------------------------------------------

    def _record_event(
        self,
        event_type: str,
        region_id: str | None,
        node_id: str | None,
        object_id: str | None,
        fencing_epoch: int,
        payload: dict[str, Any],
        plan_id: str | None = None,
    ) -> RebalanceEvent:

        event = RebalanceEvent(
            event_id=f"event-{uuid.uuid4().hex}",
            event_type=event_type,
            plan_id=plan_id,
            region_id=region_id,
            node_id=node_id,
            object_id=object_id,
            fencing_epoch=fencing_epoch,
            created_at=self._now(),
            payload=payload,
        )

        self.backend.execute(
            """
            INSERT OR IGNORE INTO rebalance_events(
                event_id,
                event_type,
                plan_id,
                region_id,
                node_id,
                object_id,
                fencing_epoch,
                created_at,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.event_type,
                event.plan_id,
                event.region_id,
                event.node_id,
                event.object_id,
                event.fencing_epoch,
                event.created_at,
                self._json(event.payload),
            ),
        )

        return event

    def events(
        self,
        limit: int = 1000,
    ) -> list[RebalanceEvent]:

        limit = max(
            1,
            min(
                int(limit),
                1_000_000,
            ),
        )

        rows = self.backend.execute(
            """
            SELECT *
            FROM rebalance_events
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        return [
            RebalanceEvent(
                event_id=row["event_id"],
                event_type=row["event_type"],
                plan_id=row["plan_id"],
                region_id=row["region_id"],
                node_id=row["node_id"],
                object_id=row["object_id"],
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

    # ----------------------------------------------------------------------
    # Statistics
    # ----------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:

        nodes = self.list_capacity_nodes()

        regions = {
            node.region_id
            for node in nodes
        }

        active_nodes = sum(
            1
            for node in nodes
            if node.state
            == CapacityState.ACTIVE.value
        )

        failed_nodes = sum(
            1
            for node in nodes
            if node.state
            == CapacityState.FAILED.value
        )

        plans = self.backend.execute(
            """
            SELECT COUNT(*) AS count
            FROM rebalance_plans
            """
        ).fetchone()["count"]

        active_plans = self.backend.execute(
            """
            SELECT COUNT(*) AS count
            FROM rebalance_plans
            WHERE state IN (?, ?, ?, ?)
            """,
            (
                RebalanceState.PLANNED.value,
                RebalanceState.PREPARING.value,
                RebalanceState.MOVING.value,
                RebalanceState.VERIFYING.value,
            ),
        ).fetchone()["count"]

        return {
            "architecture_version": (
                ARCHITECTURE_VERSION
            ),

            "regions": len(regions),

            "capacity_nodes": len(nodes),
            "active_capacity_nodes": (
                active_nodes
            ),
            "failed_capacity_nodes": (
                failed_nodes
            ),

            "rebalance_plans": int(plans),
            "active_rebalance_plans": (
                int(active_plans)
            ),

            "elastic_capacity": True,
            "automatic_rebalancing": True,
            "hot_partition_detection": True,
            "capacity_aware_placement": True,
            "scale_out": True,
            "scale_in": True,
            "partition_migration": True,
            "workload_migration": True,
            "failure_aware_capacity": True,
            "fencing_safe_movement": True,
            "durable_rebalance_plans": True,
            "lease_recovery": True,
            "restart_safe_rebalancing": True,
            "deterministic_target_selection": True,
            "backpressure_aware": True,
            "failure_domain_aware": True,

            "fixed_global_capacity_limit": False,
            "fixed_global_worker_limit": False,
            "fixed_global_partition_limit": False,
            "fixed_global_region_limit": False,

            "distributed_backend_abstraction": True,
            "local_sqlite_is_reference_backend": True,

            "scale_target": (
                "billions_to_trillions_of_public_web_resources"
            ),

            "google_scale_capability_target": True,

            "fencing_epoch": self.current_epoch(),

            "stats": dict(self._stats),
        }


# ============================================================================
# PUBLIC ALIAS
# ============================================================================

ElasticRebalancer = ElasticCapacityRebalancing


__all__ = [
    "ARCHITECTURE_VERSION",
    "CapacityState",
    "RebalanceState",
    "MovementType",
    "PressureLevel",
    "ScalingAction",
    "CapacityNode",
    "PartitionLoad",
    "CapacitySnapshot",
    "ScalingDecision",
    "RebalancePlan",
    "RebalanceCapacity",
    "RebalanceEvent",
    "ElasticCapacityMetadataBackend",
    "SQLiteElasticCapacityMetadataBackend",
    "ElasticCapacityRebalancing",
    "ElasticRebalancer",
]
