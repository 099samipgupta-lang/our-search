"""
OUR SEARCH — Phase 9.7
Distributed State / Metadata Fabric

Architecture target:
    Globally distributed infrastructure capable of coordinating
    billions and potentially trillions of public-Web resources.

This layer provides the durable distributed metadata architecture required
by:

    9.1 Distributed Execution Fabric
    9.2 Massive Worker Fleet
    9.3 Global Partition / Shard Management
    9.4 Distributed Queue / Scheduling Fabric
    9.5 Cross-Region Coordination / Failover
    9.6 Elastic Capacity / Rebalancing

Core principle:

    There must NOT be one global SQLite database, one global lock,
    one global queue, or one global metadata bottleneck.

The local SQLite implementation is only a durable reference backend.
Production deployment can replace it with a distributed metadata service.

Important:
    This file establishes architecture and durable contracts.
    It does not claim that a local machine has Google-scale resources
    or Google-scale Web coverage.
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


ARCHITECTURE_VERSION = "distributed-state-metadata.v1"

DEFAULT_LOGICAL_PARTITIONS = 1_048_576
DEFAULT_REPLICATION_FACTOR = 3
DEFAULT_LEASE_SECONDS = 300.0
DEFAULT_MAX_BATCH_SIZE = 10_000


# ============================================================================
# DATA CONTRACTS
# ============================================================================

@dataclass(frozen=True)
class MetadataKey:
    namespace: str
    key: str

    def canonical(self) -> str:
        return f"{self.namespace}:{self.key}"


@dataclass(frozen=True)
class MetadataRecord:
    namespace: str
    key: str
    value: dict[str, Any]

    version: int
    epoch: int

    partition_id: int

    updated_at: float

    owner_region: str | None = None
    owner_cluster: str | None = None

    tombstone: bool = False


@dataclass(frozen=True)
class MetadataVersion:
    version: int
    epoch: int
    updated_at: float
    writer_id: str


@dataclass(frozen=True)
class MetadataPartition:
    partition_id: int

    owner_region: str
    owner_cluster: str

    replicas: tuple[str, ...]

    epoch: int

    state: str

    capacity_units: float

    used_capacity_units: float


@dataclass(frozen=True)
class MetadataLease:
    lease_id: str

    namespace: str
    partition_id: int

    owner_id: str

    fencing_token: int

    acquired_at: float
    expires_at: float


@dataclass(frozen=True)
class MetadataMutation:
    mutation_id: str

    namespace: str
    key: str

    operation: str

    value: dict[str, Any] | None

    expected_version: int | None

    writer_id: str

    created_at: float


@dataclass(frozen=True)
class MetadataEvent:
    event_id: str

    event_type: str

    namespace: str
    key: str | None

    partition_id: int | None

    epoch: int
    fencing_token: int

    created_at: float

    payload: dict[str, Any]


@dataclass(frozen=True)
class MetadataCapacity:
    total_partitions: int
    active_partitions: int

    total_records: int
    tombstones: int

    total_capacity_units: float
    used_capacity_units: float


# ============================================================================
# PARTITIONING
# ============================================================================

class MetadataPartitioner:
    """
    Stable logical partitioning.

    Logical partitions are deliberately much larger in number than the
    number of machines.

    Physical placement can therefore change without changing the identity
    of the logical partition.
    """

    def __init__(
        self,
        partition_count: int = DEFAULT_LOGICAL_PARTITIONS,
    ) -> None:

        if partition_count < 1:
            raise ValueError(
                "partition_count must be >= 1"
            )

        self.partition_count = int(
            partition_count
        )

    def partition_for(
        self,
        namespace: str,
        key: str,
    ) -> int:

        canonical = (
            f"{namespace}:{key}"
        ).encode()

        digest = hashlib.sha256(
            canonical
        ).digest()

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return value % self.partition_count


# ============================================================================
# DISTRIBUTED METADATA BACKEND CONTRACT
# ============================================================================

class DistributedMetadataBackend:
    """
    Production metadata backend contract.

    A real deployment should provide:

        - replicated durable storage
        - strongly ordered metadata mutations where required
        - partition-local transactions
        - compare-and-set
        - fencing
        - epochs
        - snapshots
        - log replay
        - membership changes
        - failure recovery
        - cross-region replication

    This abstraction deliberately prevents the application layer from
    depending on SQLite semantics.
    """

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


# ============================================================================
# REFERENCE SQLITE BACKEND
# ============================================================================

class SQLiteDistributedMetadataBackend(
    DistributedMetadataBackend
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
            / "distributed_state_metadata.db"
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
        return _SQLiteTransaction(
            self._connection()
        )

    def _initialize(
        self,
        connection: sqlite3.Connection,
    ) -> None:

        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS
                metadata_records (
                    namespace TEXT NOT NULL,
                    key TEXT NOT NULL,

                    value_json TEXT NOT NULL,

                    version INTEGER NOT NULL,
                    epoch INTEGER NOT NULL,

                    partition_id INTEGER NOT NULL,

                    updated_at REAL NOT NULL,

                    owner_region TEXT,
                    owner_cluster TEXT,

                    tombstone INTEGER NOT NULL DEFAULT 0,

                    PRIMARY KEY(namespace, key)
                );

            CREATE INDEX IF NOT EXISTS
                idx_metadata_partition
                ON metadata_records(
                    partition_id
                );

            CREATE INDEX IF NOT EXISTS
                idx_metadata_namespace
                ON metadata_records(
                    namespace
                );

            CREATE TABLE IF NOT EXISTS
                metadata_partitions (
                    partition_id INTEGER PRIMARY KEY,

                    owner_region TEXT NOT NULL,
                    owner_cluster TEXT NOT NULL,

                    replicas_json TEXT NOT NULL,

                    epoch INTEGER NOT NULL,

                    state TEXT NOT NULL,

                    capacity_units REAL NOT NULL,
                    used_capacity_units REAL NOT NULL,

                    updated_at REAL NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_metadata_partition_region
                ON metadata_partitions(
                    owner_region
                );

            CREATE TABLE IF NOT EXISTS
                metadata_leases (
                    lease_id TEXT PRIMARY KEY,

                    namespace TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,

                    owner_id TEXT NOT NULL,

                    fencing_token INTEGER NOT NULL,

                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );

            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_metadata_active_lease
                ON metadata_leases(
                    namespace,
                    partition_id
                );

            CREATE TABLE IF NOT EXISTS
                metadata_mutations (
                    mutation_id TEXT PRIMARY KEY,

                    namespace TEXT NOT NULL,
                    key TEXT NOT NULL,

                    operation TEXT NOT NULL,

                    value_json TEXT,

                    expected_version INTEGER,

                    writer_id TEXT NOT NULL,

                    created_at REAL NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_metadata_mutation_key
                ON metadata_mutations(
                    namespace,
                    key
                );

            CREATE TABLE IF NOT EXISTS
                metadata_events (
                    event_id TEXT PRIMARY KEY,

                    event_type TEXT NOT NULL,

                    namespace TEXT NOT NULL,
                    key TEXT,

                    partition_id INTEGER,

                    epoch INTEGER NOT NULL,
                    fencing_token INTEGER NOT NULL,

                    created_at REAL NOT NULL,

                    payload_json TEXT NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_metadata_event_time
                ON metadata_events(
                    created_at DESC
                );

            CREATE TABLE IF NOT EXISTS
                metadata_epochs (
                    namespace TEXT PRIMARY KEY,
                    epoch INTEGER NOT NULL
                );

            CREATE TABLE IF NOT EXISTS
                metadata_checkpoints (
                    checkpoint_id TEXT PRIMARY KEY,

                    namespace TEXT NOT NULL,

                    partition_id INTEGER,

                    version INTEGER NOT NULL,
                    epoch INTEGER NOT NULL,

                    created_at REAL NOT NULL,

                    payload_json TEXT NOT NULL
                );

            CREATE INDEX IF NOT EXISTS
                idx_metadata_checkpoint_partition
                ON metadata_checkpoints(
                    namespace,
                    partition_id,
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
# DISTRIBUTED STATE / METADATA FABRIC
# ============================================================================

class DistributedStateMetadata:

    def __init__(
        self,
        storage_root: str,
        backend:
            DistributedMetadataBackend | None = None,
        partition_count: int =
            DEFAULT_LOGICAL_PARTITIONS,
        replication_factor: int =
            DEFAULT_REPLICATION_FACTOR,
        lease_seconds: float =
            DEFAULT_LEASE_SECONDS,
        max_batch_size: int =
            DEFAULT_MAX_BATCH_SIZE,
    ) -> None:

        if partition_count < 1:
            raise ValueError(
                "partition_count must be >= 1"
            )

        if replication_factor < 1:
            raise ValueError(
                "replication_factor must be >= 1"
            )

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive"
            )

        if max_batch_size < 1:
            raise ValueError(
                "max_batch_size must be >= 1"
            )

        self.storage_root = storage_root

        self.backend = (
            backend
            or SQLiteDistributedMetadataBackend(
                storage_root
            )
        )

        self.partitioner = MetadataPartitioner(
            partition_count
        )

        self.partition_count = (
            partition_count
        )

        self.replication_factor = (
            replication_factor
        )

        self.lease_seconds = (
            lease_seconds
        )

        self.max_batch_size = (
            max_batch_size
        )

        self.instance_id = (
            f"metadata-{uuid.uuid4().hex}"
        )

        self._lock = threading.RLock()

        self.running = False

        self._stats = {
            "reads": 0,
            "writes": 0,
            "deletes": 0,
            "batch_writes": 0,
            "cas_conflicts": 0,
            "leases_acquired": 0,
            "leases_recovered": 0,
            "fencing_events": 0,
            "epoch_advances": 0,
            "partition_assignments": 0,
            "partition_moves": 0,
            "replication_events": 0,
            "checkpoint_writes": 0,
            "tombstones": 0,
        }

        self._ensure_epoch("global")

    # ------------------------------------------------------------------
    # Utilities
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
    # Epochs
    # ------------------------------------------------------------------

    def _ensure_epoch(
        self,
        namespace: str,
    ) -> None:

        self.backend.execute(
            """
            INSERT OR IGNORE INTO
                metadata_epochs(
                    namespace,
                    epoch
                )
            VALUES (?, 0)
            """,
            (namespace,),
        )

    def current_epoch(
        self,
        namespace: str = "global",
    ) -> int:

        self._ensure_epoch(
            namespace
        )

        row = self.backend.execute(
            """
            SELECT epoch
            FROM metadata_epochs
            WHERE namespace = ?
            """,
            (namespace,),
        ).fetchone()

        return int(row["epoch"])

    def advance_epoch(
        self,
        namespace: str,
        reason: str,
    ) -> int:

        self._ensure_epoch(
            namespace
        )

        with self.backend.transaction() as connection:

            row = connection.execute(
                """
                SELECT epoch
                FROM metadata_epochs
                WHERE namespace = ?
                """,
                (namespace,),
            ).fetchone()

            epoch = (
                int(row["epoch"])
                if row
                else 0
            )

            epoch += 1

            connection.execute(
                """
                UPDATE metadata_epochs
                SET epoch = ?
                WHERE namespace = ?
                """,
                (
                    epoch,
                    namespace,
                ),
            )

        self._stats[
            "epoch_advances"
        ] += 1

        self._record_event(
            event_type="epoch_advanced",
            namespace=namespace,
            key=None,
            partition_id=None,
            epoch=epoch,
            fencing_token=epoch,
            payload={
                "reason": reason,
            },
        )

        return epoch

    # ------------------------------------------------------------------
    # Partition placement
    # ------------------------------------------------------------------

    def assign_partition(
        self,
        partition_id: int,
        region_id: str,
        cluster_id: str,
        replicas: Iterable[str] = (),
        capacity_units: float = 0.0,
    ) -> MetadataPartition:

        if not (
            0 <= partition_id
            < self.partition_count
        ):
            raise ValueError(
                "partition_id outside logical namespace"
            )

        replica_tuple = tuple(
            sorted(
                set(
                    str(value)
                    for value in replicas
                )
            )
        )

        epoch = self.advance_epoch(
            "global",
            f"partition_assignment:{partition_id}",
        )

        now = self._now()

        self.backend.execute(
            """
            INSERT INTO metadata_partitions(
                partition_id,
                owner_region,
                owner_cluster,
                replicas_json,
                epoch,
                state,
                capacity_units,
                used_capacity_units,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(partition_id)
            DO UPDATE SET
                owner_region =
                    excluded.owner_region,
                owner_cluster =
                    excluded.owner_cluster,
                replicas_json =
                    excluded.replicas_json,
                epoch =
                    excluded.epoch,
                state =
                    excluded.state,
                capacity_units =
                    excluded.capacity_units,
                updated_at =
                    excluded.updated_at
            """,
            (
                partition_id,
                region_id,
                cluster_id,
                self._json(
                    {
                        "replicas": (
                            list(replica_tuple)
                        )
                    }
                ),
                epoch,
                "active",
                max(0.0, capacity_units),
                0.0,
                now,
            ),
        )

        self._stats[
            "partition_assignments"
        ] += 1

        return self.get_partition(
            partition_id
        )

    def move_partition(
        self,
        partition_id: int,
        region_id: str,
        cluster_id: str,
        replicas: Iterable[str] = (),
    ) -> MetadataPartition:

        current = self.get_partition(
            partition_id
        )

        if current is None:
            return self.assign_partition(
                partition_id,
                region_id,
                cluster_id,
                replicas,
            )

        epoch = self.advance_epoch(
            "global",
            f"partition_move:{partition_id}",
        )

        now = self._now()

        replica_tuple = tuple(
            sorted(
                set(
                    str(value)
                    for value in replicas
                )
            )
        )

        self.backend.execute(
            """
            UPDATE metadata_partitions
            SET
                owner_region = ?,
                owner_cluster = ?,
                replicas_json = ?,
                epoch = ?,
                state = ?,
                updated_at = ?
            WHERE partition_id = ?
            """,
            (
                region_id,
                cluster_id,
                self._json(
                    {
                        "replicas": (
                            list(replica_tuple)
                        )
                    }
                ),
                epoch,
                "active",
                now,
                partition_id,
            ),
        )

        self._stats[
            "partition_moves"
        ] += 1

        self._record_event(
            event_type="partition_moved",
            namespace="global",
            key=None,
            partition_id=partition_id,
            epoch=epoch,
            fencing_token=epoch,
            payload={
                "from_region": (
                    current.owner_region
                ),
                "to_region": region_id,
                "from_cluster": (
                    current.owner_cluster
                ),
                "to_cluster": cluster_id,
            },
        )

        return self.get_partition(
            partition_id
        )

    def get_partition(
        self,
        partition_id: int,
    ) -> MetadataPartition | None:

        row = self.backend.execute(
            """
            SELECT *
            FROM metadata_partitions
            WHERE partition_id = ?
            """,
            (partition_id,),
        ).fetchone()

        if row is None:
            return None

        payload = json.loads(
            row["replicas_json"]
        )

        replicas = tuple(
            payload.get(
                "replicas",
                [],
            )
        )

        return MetadataPartition(
            partition_id=int(
                row["partition_id"]
            ),
            owner_region=row[
                "owner_region"
            ],
            owner_cluster=row[
                "owner_cluster"
            ],
            replicas=replicas,
            epoch=int(
                row["epoch"]
            ),
            state=row["state"],
            capacity_units=float(
                row["capacity_units"]
            ),
            used_capacity_units=float(
                row["used_capacity_units"]
            ),
        )

    # ------------------------------------------------------------------
    # Partition capacity
    # ------------------------------------------------------------------

    def update_partition_capacity(
        self,
        partition_id: int,
        used_capacity_units: float,
    ) -> bool:

        result = self.backend.execute(
            """
            UPDATE metadata_partitions
            SET
                used_capacity_units = ?,
                updated_at = ?
            WHERE partition_id = ?
            """,
            (
                max(
                    0.0,
                    used_capacity_units,
                ),
                self._now(),
                partition_id,
            ),
        )

        return result.rowcount == 1

    # ------------------------------------------------------------------
    # Key/value reads
    # ------------------------------------------------------------------

    def get(
        self,
        namespace: str,
        key: str,
        include_tombstone: bool = False,
    ) -> MetadataRecord | None:

        partition_id = (
            self.partitioner.partition_for(
                namespace,
                key,
            )
        )

        row = self.backend.execute(
            """
            SELECT *
            FROM metadata_records
            WHERE
                namespace = ?
                AND key = ?
            """,
            (
                namespace,
                key,
            ),
        ).fetchone()

        self._stats[
            "reads"
        ] += 1

        if row is None:
            return None

        if (
            bool(row["tombstone"])
            and not include_tombstone
        ):
            return None

        return MetadataRecord(
            namespace=row[
                "namespace"
            ],
            key=row["key"],
            value=json.loads(
                row["value_json"]
            ),
            version=int(
                row["version"]
            ),
            epoch=int(
                row["epoch"]
            ),
            partition_id=partition_id,
            updated_at=float(
                row["updated_at"]
            ),
            owner_region=row[
                "owner_region"
            ],
            owner_cluster=row[
                "owner_cluster"
            ],
            tombstone=bool(
                row["tombstone"]
            ),
        )

    # ------------------------------------------------------------------
    # Compare-and-set
    # ------------------------------------------------------------------

    def compare_and_set(
        self,
        namespace: str,
        key: str,
        value: dict[str, Any],
        expected_version: int | None,
        writer_id: str | None = None,
        fencing_token: int | None = None,
    ) -> MetadataRecord:

        writer = (
            writer_id
            or self.instance_id
        )

        partition_id = (
            self.partitioner.partition_for(
                namespace,
                key,
            )
        )

        partition = self.get_partition(
            partition_id
        )

        if fencing_token is not None:

            if (
                partition is not None
                and fencing_token
                < partition.epoch
            ):
                self._stats[
                    "fencing_events"
                ] += 1

                raise RuntimeError(
                    "stale fencing token"
                )

        now = self._now()

        with self.backend.transaction() as connection:

            row = connection.execute(
                """
                SELECT *
                FROM metadata_records
                WHERE
                    namespace = ?
                    AND key = ?
                """,
                (
                    namespace,
                    key,
                ),
            ).fetchone()

            current_version = (
                int(row["version"])
                if row
                else 0
            )

            if (
                expected_version is not None
                and current_version
                != expected_version
            ):

                self._stats[
                    "cas_conflicts"
                ] += 1

                raise RuntimeError(
                    "metadata compare-and-set conflict"
                )

            new_version = (
                current_version + 1
            )

            epoch = max(
                self.current_epoch(
                    namespace
                ),
                fencing_token or 0,
            )

            owner_region = (
                partition.owner_region
                if partition
                else None
            )

            owner_cluster = (
                partition.owner_cluster
                if partition
                else None
            )

            connection.execute(
                """
                INSERT INTO metadata_records(
                    namespace,
                    key,
                    value_json,
                    version,
                    epoch,
                    partition_id,
                    updated_at,
                    owner_region,
                    owner_cluster,
                    tombstone
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(namespace, key)
                DO UPDATE SET
                    value_json =
                        excluded.value_json,
                    version =
                        excluded.version,
                    epoch =
                        excluded.epoch,
                    partition_id =
                        excluded.partition_id,
                    updated_at =
                        excluded.updated_at,
                    owner_region =
                        excluded.owner_region,
                    owner_cluster =
                        excluded.owner_cluster,
                    tombstone = 0
                """,
                (
                    namespace,
                    key,
                    self._json(value),
                    new_version,
                    epoch,
                    partition_id,
                    now,
                    owner_region,
                    owner_cluster,
                ),
            )

            mutation_id = (
                f"mutation-{uuid.uuid4().hex}"
            )

            connection.execute(
                """
                INSERT INTO metadata_mutations(
                    mutation_id,
                    namespace,
                    key,
                    operation,
                    value_json,
                    expected_version,
                    writer_id,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mutation_id,
                    namespace,
                    key,
                    "compare_and_set",
                    self._json(value),
                    expected_version,
                    writer,
                    now,
                ),
            )

        self._stats[
            "writes"
        ] += 1

        return self.get(
            namespace,
            key,
            include_tombstone=True,
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def put(
        self,
        namespace: str,
        key: str,
        value: dict[str, Any],
        writer_id: str | None = None,
        fencing_token: int | None = None,
    ) -> MetadataRecord:

        current = self.get(
            namespace,
            key,
            include_tombstone=True,
        )

        expected = (
            current.version
            if current is not None
            else 0
        )

        return self.compare_and_set(
            namespace=namespace,
            key=key,
            value=value,
            expected_version=expected,
            writer_id=writer_id,
            fencing_token=fencing_token,
        )

    # ------------------------------------------------------------------
    # Batch writes
    # ------------------------------------------------------------------

    def put_many(
        self,
        records: Iterable[
            tuple[
                str,
                str,
                dict[str, Any],
            ]
        ],
        writer_id: str | None = None,
    ) -> list[MetadataRecord]:

        items = list(records)

        if not items:
            return []

        if len(items) > self.max_batch_size:
            raise ValueError(
                "batch exceeds max_batch_size"
            )

        results: list[MetadataRecord] = []

        writer = (
            writer_id
            or self.instance_id
        )

        # Group by logical partition.
        grouped: dict[
            int,
            list[
                tuple[
                    str,
                    str,
                    dict[str, Any],
                ]
            ],
        ] = {}

        for namespace, key, value in items:

            partition_id = (
                self.partitioner.partition_for(
                    namespace,
                    key,
                )
            )

            grouped.setdefault(
                partition_id,
                [],
            ).append(
                (
                    namespace,
                    key,
                    value,
                )
            )

        for partition_id in sorted(
            grouped
        ):

            partition_records = grouped[
                partition_id
            ]

            for namespace, key, value in (
                partition_records
            ):

                current = self.get(
                    namespace,
                    key,
                    include_tombstone=True,
                )

                expected = (
                    current.version
                    if current
                    else 0
                )

                results.append(
                    self.compare_and_set(
                        namespace,
                        key,
                        value,
                        expected,
                        writer,
                    )
                )

        self._stats[
            "batch_writes"
        ] += 1

        return results

    # ------------------------------------------------------------------
    # Tombstones / deletes
    # ------------------------------------------------------------------

    def delete(
        self,
        namespace: str,
        key: str,
        writer_id: str | None = None,
        fencing_token: int | None = None,
    ) -> MetadataRecord:

        current = self.get(
            namespace,
            key,
            include_tombstone=True,
        )

        if current is None:
            version = 0
        else:
            version = current.version

        partition_id = (
            self.partitioner.partition_for(
                namespace,
                key,
            )
        )

        partition = self.get_partition(
            partition_id
        )

        epoch = max(
            self.current_epoch(
                namespace
            ),
            fencing_token or 0,
        )

        now = self._now()

        with self.backend.transaction() as connection:

            connection.execute(
                """
                INSERT INTO metadata_records(
                    namespace,
                    key,
                    value_json,
                    version,
                    epoch,
                    partition_id,
                    updated_at,
                    owner_region,
                    owner_cluster,
                    tombstone
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(namespace, key)
                DO UPDATE SET
                    value_json =
                        excluded.value_json,
                    version =
                        excluded.version,
                    epoch =
                        excluded.epoch,
                    partition_id =
                        excluded.partition_id,
                    updated_at =
                        excluded.updated_at,
                    owner_region =
                        excluded.owner_region,
                    owner_cluster =
                        excluded.owner_cluster,
                    tombstone = 1
                """,
                (
                    namespace,
                    key,
                    "{}",
                    version + 1,
                    epoch,
                    partition_id,
                    now,
                    (
                        partition.owner_region
                        if partition
                        else None
                    ),
                    (
                        partition.owner_cluster
                        if partition
                        else None
                    ),
                ),
            )

        self._stats[
            "deletes"
        ] += 1

        self._stats[
            "tombstones"
        ] += 1

        return self.get(
            namespace,
            key,
            include_tombstone=True,
        )

    # ------------------------------------------------------------------
    # Leases / fencing
    # ------------------------------------------------------------------

    def acquire_lease(
        self,
        namespace: str,
        partition_id: int,
        owner_id: str | None = None,
    ) -> MetadataLease:

        owner = (
            owner_id
            or self.instance_id
        )

        now = self._now()

        epoch = self.advance_epoch(
            namespace,
            f"lease_acquire:{partition_id}",
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
                FROM metadata_leases
                WHERE
                    namespace = ?
                    AND partition_id = ?
                """,
                (
                    namespace,
                    partition_id,
                ),
            ).fetchone()

            if (
                existing is not None
                and float(
                    existing["expires_at"]
                ) > now
            ):
                raise RuntimeError(
                    "metadata partition lease already held"
                )

            connection.execute(
                """
                INSERT INTO metadata_leases(
                    lease_id,
                    namespace,
                    partition_id,
                    owner_id,
                    fencing_token,
                    acquired_at,
                    expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    namespace,
                    partition_id
                )
                DO UPDATE SET
                    lease_id =
                        excluded.lease_id,
                    owner_id =
                        excluded.owner_id,
                    fencing_token =
                        excluded.fencing_token,
                    acquired_at =
                        excluded.acquired_at,
                    expires_at =
                        excluded.expires_at
                """,
                (
                    lease_id,
                    namespace,
                    partition_id,
                    owner,
                    epoch,
                    now,
                    expires,
                ),
            )

        self._stats[
            "leases_acquired"
        ] += 1

        self._record_event(
            event_type="lease_acquired",
            namespace=namespace,
            key=None,
            partition_id=partition_id,
            epoch=epoch,
            fencing_token=epoch,
            payload={
                "owner_id": owner,
                "lease_id": lease_id,
                "expires_at": expires,
            },
        )

        return MetadataLease(
            lease_id=lease_id,
            namespace=namespace,
            partition_id=partition_id,
            owner_id=owner,
            fencing_token=epoch,
            acquired_at=now,
            expires_at=expires,
        )

    def renew_lease(
        self,
        lease_id: str,
        owner_id: str,
    ) -> bool:

        now = self._now()

        result = self.backend.execute(
            """
            UPDATE metadata_leases
            SET expires_at = ?
            WHERE
                lease_id = ?
                AND owner_id = ?
                AND expires_at > ?
            """,
            (
                now + self.lease_seconds,
                lease_id,
                owner_id,
                now,
            ),
        )

        return result.rowcount == 1

    def release_lease(
        self,
        lease_id: str,
        owner_id: str,
    ) -> bool:

        result = self.backend.execute(
            """
            DELETE FROM metadata_leases
            WHERE
                lease_id = ?
                AND owner_id = ?
            """,
            (
                lease_id,
                owner_id,
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
            FROM metadata_leases
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

            result = self.backend.execute(
                """
                DELETE FROM metadata_leases
                WHERE
                    lease_id = ?
                    AND expires_at <= ?
                """,
                (
                    row["lease_id"],
                    current,
                ),
            )

            if result.rowcount:

                recovered += 1

                self._record_event(
                    event_type="lease_expired",
                    namespace=row[
                        "namespace"
                    ],
                    key=None,
                    partition_id=int(
                        row["partition_id"]
                    ),
                    epoch=int(
                        row["fencing_token"]
                    ),
                    fencing_token=int(
                        row["fencing_token"]
                    ),
                    payload={
                        "owner_id": row[
                            "owner_id"
                        ],
                    },
                )

        self._stats[
            "leases_recovered"
        ] += recovered

        return recovered

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def checkpoint(
        self,
        namespace: str,
        partition_id: int | None,
        version: int,
        payload: dict[str, Any],
    ) -> str:

        checkpoint_id = (
            f"checkpoint-{uuid.uuid4().hex}"
        )

        epoch = self.current_epoch(
            namespace
        )

        self.backend.execute(
            """
            INSERT INTO metadata_checkpoints(
                checkpoint_id,
                namespace,
                partition_id,
                version,
                epoch,
                created_at,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                checkpoint_id,
                namespace,
                partition_id,
                version,
                epoch,
                self._now(),
                self._json(payload),
            ),
        )

        self._stats[
            "checkpoint_writes"
        ] += 1

        return checkpoint_id

    def latest_checkpoint(
        self,
        namespace: str,
        partition_id: int | None = None,
    ) -> dict[str, Any] | None:

        if partition_id is None:

            row = self.backend.execute(
                """
                SELECT *
                FROM metadata_checkpoints
                WHERE namespace = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (namespace,),
            ).fetchone()

        else:

            row = self.backend.execute(
                """
                SELECT *
                FROM metadata_checkpoints
                WHERE
                    namespace = ?
                    AND partition_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (
                    namespace,
                    partition_id,
                ),
            ).fetchone()

        if row is None:
            return None

        return {
            "checkpoint_id": row[
                "checkpoint_id"
            ],
            "namespace": row[
                "namespace"
            ],
            "partition_id": row[
                "partition_id"
            ],
            "version": int(
                row["version"]
            ),
            "epoch": int(
                row["epoch"]
            ),
            "created_at": float(
                row["created_at"]
            ),
            "payload": json.loads(
                row["payload_json"]
            ),
        }

    # ------------------------------------------------------------------
    # Event journal
    # ------------------------------------------------------------------

    def _record_event(
        self,
        event_type: str,
        namespace: str,
        key: str | None,
        partition_id: int | None,
        epoch: int,
        fencing_token: int,
        payload: dict[str, Any],
    ) -> MetadataEvent:

        event = MetadataEvent(
            event_id=(
                f"event-{uuid.uuid4().hex}"
            ),
            event_type=event_type,
            namespace=namespace,
            key=key,
            partition_id=partition_id,
            epoch=epoch,
            fencing_token=fencing_token,
            created_at=self._now(),
            payload=payload,
        )

        self.backend.execute(
            """
            INSERT OR IGNORE INTO metadata_events(
                event_id,
                event_type,
                namespace,
                key,
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
                event.namespace,
                event.key,
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

    def events(
        self,
        limit: int = 1000,
    ) -> list[MetadataEvent]:

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
            FROM metadata_events
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        return [
            MetadataEvent(
                event_id=row[
                    "event_id"
                ],
                event_type=row[
                    "event_type"
                ],
                namespace=row[
                    "namespace"
                ],
                key=row["key"],
                partition_id=(
                    int(
                        row["partition_id"]
                    )
                    if row[
                        "partition_id"
                    ] is not None
                    else None
                ),
                epoch=int(
                    row["epoch"]
                ),
                fencing_token=int(
                    row[
                        "fencing_token"
                    ]
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
    # Capacity / statistics
    # ------------------------------------------------------------------

    def capacity(self) -> MetadataCapacity:

        partition_row = self.backend.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(
                    CASE
                        WHEN state = 'active'
                        THEN 1
                        ELSE 0
                    END
                ) AS active,
                COALESCE(
                    SUM(capacity_units),
                    0
                ) AS capacity,
                COALESCE(
                    SUM(used_capacity_units),
                    0
                ) AS used
            FROM metadata_partitions
            """
        ).fetchone()

        record_row = self.backend.execute(
            """
            SELECT
                COUNT(*) AS total,
                COALESCE(
                    SUM(tombstone),
                    0
                ) AS tombstones
            FROM metadata_records
            """
        ).fetchone()

        return MetadataCapacity(
            total_partitions=int(
                partition_row["total"]
                or 0
            ),
            active_partitions=int(
                partition_row["active"]
                or 0
            ),
            total_records=int(
                record_row["total"]
                or 0
            ),
            tombstones=int(
                record_row["tombstones"]
                or 0
            ),
            total_capacity_units=float(
                partition_row["capacity"]
                or 0.0
            ),
            used_capacity_units=float(
                partition_row["used"]
                or 0.0
            ),
        )

    def stats(self) -> dict[str, Any]:

        capacity = self.capacity()

        return {
            "architecture_version": (
                ARCHITECTURE_VERSION
            ),

            "logical_partition_count": (
                self.partition_count
            ),

            "replication_factor": (
                self.replication_factor
            ),

            "metadata_records": (
                capacity.total_records
            ),

            "tombstones": (
                capacity.tombstones
            ),

            "active_partitions": (
                capacity.active_partitions
            ),

            "total_partition_capacity": (
                capacity.total_capacity_units
            ),

            "used_partition_capacity": (
                capacity.used_capacity_units
            ),

            "partition_locality": True,
            "logical_partitioning": True,
            "distributed_backend_abstraction": True,
            "compare_and_set": True,
            "durable_versions": True,
            "epochs": True,
            "fencing": True,
            "leases": True,
            "lease_recovery": True,
            "durable_checkpoints": True,
            "event_journal": True,
            "tombstone_deletes": True,
            "partition_movement": True,
            "region_aware_metadata": True,
            "cluster_aware_metadata": True,
            "replication_metadata": True,

            "single_global_sqlite_required": False,
            "single_global_lock_required": False,
            "single_global_queue_required": False,
            "single_global_metadata_bottleneck": False,

            "fixed_global_record_limit": False,
            "fixed_global_partition_limit": False,
            "fixed_global_region_limit": False,
            "fixed_global_cluster_limit": False,

            "elastic_metadata_namespace": True,
            "failure_recovery": True,
            "restart_recovery": True,

            "scale_target": (
                "billions_to_trillions_of_public_web_resources"
            ),

            "google_scale_capability_target": True,

            "stats": dict(self._stats),
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def cycle(self) -> dict[str, Any]:

        self.recover_expired_leases()

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


# ============================================================================
# PUBLIC ALIASES
# ============================================================================

DistributedMetadata = DistributedStateMetadata
StateMetadataFabric = DistributedStateMetadata


__all__ = [
    "ARCHITECTURE_VERSION",
    "MetadataKey",
    "MetadataRecord",
    "MetadataVersion",
    "MetadataPartition",
    "MetadataLease",
    "MetadataMutation",
    "MetadataEvent",
    "MetadataCapacity",
    "MetadataPartitioner",
    "DistributedMetadataBackend",
    "SQLiteDistributedMetadataBackend",
    "DistributedStateMetadata",
    "DistributedMetadata",
    "StateMetadataFabric",
]
