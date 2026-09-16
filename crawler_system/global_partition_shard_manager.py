from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Optional


class PartitionShardError(RuntimeError):
    """Base error for global partition/shard management."""


@dataclass(frozen=True)
class PartitionDescriptor:
    partition_id: int
    partition_key: str
    epoch: int
    state: str
    priority: float
    hotness: float
    owner_id: Optional[str]
    generation: int


@dataclass(frozen=True)
class ShardDescriptor:
    shard_id: str
    region: str
    zone: str
    rack: str
    endpoint: str
    capacity: float
    used_capacity: float
    health: str
    generation: int
    fencing_epoch: int


@dataclass(frozen=True)
class ReplicaPlacement:
    partition_id: int
    shard_id: str
    replica_role: str
    placement_epoch: int
    state: str


@dataclass(frozen=True)
class PartitionLease:
    partition_id: int
    owner_id: str
    generation: int
    fencing_epoch: int
    lease_until: float


@dataclass(frozen=True)
class ShardMovePlan:
    plan_id: str
    partition_id: int
    source_shard: Optional[str]
    target_shard: str
    placement_epoch: int
    state: str
    created_at: float


@dataclass(frozen=True)
class PartitionCheckpoint:
    partition_id: int
    epoch: int
    cursor: str
    updated_at: float


@dataclass(frozen=True)
class PartitionShardCapacity:
    logical_partitions: int
    physical_shards: int
    active_shards: int
    healthy_shards: int
    assigned_partitions: int
    moving_partitions: int
    fenced_shards: int
    hot_partitions: int


class GlobalPartitionShardManager:
    """
    Global logical-partition and physical-shard management plane.

    Design goals:
      - enormous logical partition space
      - independently scalable physical shards
      - deterministic partition identity
      - epoch-based placement changes
      - ownership leases and fencing
      - replica-aware placement
      - shard health/capacity tracking
      - durable movement plans
      - split/merge hooks
      - restart-safe checkpoints
      - no fixed small-Web execution ceiling

    SQLite is intentionally treated as a local durable control-plane
    implementation. The logical model does not depend on one database
    being globally authoritative and can be backed by a distributed
    metadata service later.
    """

    VERSION = "global-partition-shard-manager.v1"

    DEFAULT_LOGICAL_PARTITION_SPACE = 1_048_576
    DEFAULT_REPLICA_COUNT = 3

    def __init__(
        self,
        storage_root: str,
        logical_partition_space: int = DEFAULT_LOGICAL_PARTITION_SPACE,
        replica_count: int = DEFAULT_REPLICA_COUNT,
        lease_seconds: float = 300.0,
        hotness_threshold: float = 0.80,
    ) -> None:
        if logical_partition_space < 1:
            raise ValueError(
                "logical_partition_space must be >= 1"
            )

        if replica_count < 1:
            raise ValueError(
                "replica_count must be >= 1"
            )

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be > 0"
            )

        if not 0.0 <= hotness_threshold <= 1.0:
            raise ValueError(
                "hotness_threshold must be between 0 and 1"
            )

        self.storage_root = storage_root
        self.logical_partition_space = logical_partition_space
        self.replica_count = replica_count
        self.lease_seconds = lease_seconds
        self.hotness_threshold = hotness_threshold

        self._lock = threading.RLock()
        self._running = False
        self._stop_event = threading.Event()

        self._db_path = (
            f"{storage_root.rstrip('/')}/"
            "global_partition_shard_manager.db"
        )

        self._initialize_database()

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------

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
        connection.execute(
            "PRAGMA foreign_keys=ON"
        )

        return connection

    def _initialize_database(self) -> None:
        connection = self._connect()

        try:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS partitions (
                    partition_id INTEGER PRIMARY KEY,
                    partition_key TEXT NOT NULL UNIQUE,
                    epoch INTEGER NOT NULL DEFAULT 1,
                    state TEXT NOT NULL DEFAULT 'active',
                    priority REAL NOT NULL DEFAULT 50.0,
                    hotness REAL NOT NULL DEFAULT 0.0,
                    owner_id TEXT,
                    generation INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_partitions_state
                ON partitions(state);

                CREATE INDEX IF NOT EXISTS
                    idx_partitions_priority
                ON partitions(priority DESC);

                CREATE INDEX IF NOT EXISTS
                    idx_partitions_hotness
                ON partitions(hotness DESC);

                CREATE TABLE IF NOT EXISTS shards (
                    shard_id TEXT PRIMARY KEY,
                    region TEXT NOT NULL,
                    zone TEXT NOT NULL,
                    rack TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    capacity REAL NOT NULL,
                    used_capacity REAL NOT NULL DEFAULT 0.0,
                    health TEXT NOT NULL DEFAULT 'healthy',
                    generation INTEGER NOT NULL DEFAULT 1,
                    fencing_epoch INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_shards_health
                ON shards(health);

                CREATE INDEX IF NOT EXISTS
                    idx_shards_region_zone
                ON shards(region, zone);

                CREATE TABLE IF NOT EXISTS placements (
                    partition_id INTEGER NOT NULL,
                    shard_id TEXT NOT NULL,
                    replica_role TEXT NOT NULL,
                    placement_epoch INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'active',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (
                        partition_id,
                        shard_id,
                        placement_epoch
                    )
                );

                CREATE INDEX IF NOT EXISTS
                    idx_placements_partition
                ON placements(partition_id);

                CREATE INDEX IF NOT EXISTS
                    idx_placements_shard
                ON placements(shard_id);

                CREATE TABLE IF NOT EXISTS partition_leases (
                    partition_id INTEGER PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    fencing_epoch INTEGER NOT NULL,
                    lease_until REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_partition_leases_expiry
                ON partition_leases(lease_until);

                CREATE TABLE IF NOT EXISTS move_plans (
                    plan_id TEXT PRIMARY KEY,
                    partition_id INTEGER NOT NULL,
                    source_shard TEXT,
                    target_shard TEXT NOT NULL,
                    placement_epoch INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'planned',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_move_plans_partition
                ON move_plans(partition_id);

                CREATE INDEX IF NOT EXISTS
                    idx_move_plans_state
                ON move_plans(state);

                CREATE TABLE IF NOT EXISTS checkpoints (
                    partition_id INTEGER PRIMARY KEY,
                    epoch INTEGER NOT NULL,
                    cursor TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS placement_epochs (
                    epoch INTEGER PRIMARY KEY,
                    created_at REAL NOT NULL,
                    reason TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS partition_events (
                    event_id TEXT PRIMARY KEY,
                    partition_id INTEGER,
                    shard_id TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT,
                    created_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    idx_partition_events_partition
                ON partition_events(partition_id);

                CREATE TABLE IF NOT EXISTS split_merge_plans (
                    plan_id TEXT PRIMARY KEY,
                    partition_id INTEGER NOT NULL,
                    operation TEXT NOT NULL,
                    target_count INTEGER,
                    state TEXT NOT NULL DEFAULT 'planned',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )

            row = connection.execute(
                """
                SELECT MAX(epoch)
                FROM placement_epochs
                """
            ).fetchone()

            if row is None or row[0] is None:
                connection.execute(
                    """
                    INSERT INTO placement_epochs(
                        epoch,
                        created_at,
                        reason
                    )
                    VALUES(?, ?, ?)
                    """,
                    (
                        1,
                        time.time(),
                        "initial",
                    ),
                )

            connection.commit()

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Deterministic identity
    # ------------------------------------------------------------------

    @staticmethod
    def _stable_hash(value: str) -> int:
        digest = hashlib.sha256(
            value.encode("utf-8")
        ).digest()

        return int.from_bytes(
            digest[:16],
            "big",
        )

    def partition_for(self, value: str) -> int:
        """
        Deterministically map any namespace value into the logical
        partition space.
        """
        if not value:
            raise ValueError("value must not be empty")

        return (
            self._stable_hash(value)
            % self.logical_partition_space
        )

    @staticmethod
    def partition_key_for(value: str) -> str:
        return hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()

    @staticmethod
    def stable_shard_score(
        partition_id: int,
        shard_id: str,
        epoch: int,
    ) -> int:
        return GlobalPartitionShardManager._stable_hash(
            f"{partition_id}:{shard_id}:{epoch}"
        )

    # ------------------------------------------------------------------
    # Partition lifecycle
    # ------------------------------------------------------------------

    def ensure_partition(
        self,
        partition_id: int,
        partition_key: Optional[str] = None,
        priority: float = 50.0,
    ) -> PartitionDescriptor:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                if partition_id < 0:
                    raise ValueError(
                        "partition_id must be >= 0"
                    )

                if partition_id >= self.logical_partition_space:
                    raise ValueError(
                        "partition_id exceeds logical partition space"
                    )

                if partition_key is None:
                    partition_key = str(partition_id)

                connection.execute(
                    """
                    INSERT OR IGNORE INTO partitions(
                        partition_id,
                        partition_key,
                        epoch,
                        state,
                        priority,
                        hotness,
                        owner_id,
                        generation,
                        created_at,
                        updated_at
                    )
                    VALUES(
                        ?, ?, 1, 'active', ?, 0.0,
                        NULL, 0, ?, ?
                    )
                    """,
                    (
                        partition_id,
                        partition_key,
                        float(priority),
                        now,
                        now,
                    ),
                )

                connection.commit()

                return self.get_partition(
                    partition_id,
                    connection=connection,
                )

            finally:
                connection.close()

    def ensure_partition_for(
        self,
        value: str,
        priority: float = 50.0,
    ) -> PartitionDescriptor:
        partition_id = self.partition_for(value)

        return self.ensure_partition(
            partition_id=partition_id,
            partition_key=self.partition_key_for(value),
            priority=priority,
        )

    def get_partition(
        self,
        partition_id: int,
        connection: Optional[sqlite3.Connection] = None,
    ) -> PartitionDescriptor:
        own_connection = connection is None

        if own_connection:
            connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM partitions
                WHERE partition_id = ?
                """,
                (partition_id,),
            ).fetchone()

            if row is None:
                raise PartitionShardError(
                    f"partition {partition_id} does not exist"
                )

            return PartitionDescriptor(
                partition_id=row["partition_id"],
                partition_key=row["partition_key"],
                epoch=row["epoch"],
                state=row["state"],
                priority=row["priority"],
                hotness=row["hotness"],
                owner_id=row["owner_id"],
                generation=row["generation"],
            )

        finally:
            if own_connection:
                connection.close()

    def update_partition_signal(
        self,
        partition_id: int,
        priority: Optional[float] = None,
        hotness: Optional[float] = None,
        state: Optional[str] = None,
    ) -> PartitionDescriptor:
        with self._lock:
            connection = self._connect()

            try:
                self.get_partition(
                    partition_id,
                    connection=connection,
                )

                fields = []
                values = []

                if priority is not None:
                    fields.append("priority = ?")
                    values.append(float(priority))

                if hotness is not None:
                    fields.append("hotness = ?")
                    values.append(
                        max(0.0, min(1.0, float(hotness)))
                    )

                if state is not None:
                    fields.append("state = ?")
                    values.append(state)

                fields.append("updated_at = ?")
                values.append(time.time())

                values.append(partition_id)

                connection.execute(
                    f"""
                    UPDATE partitions
                    SET {", ".join(fields)}
                    WHERE partition_id = ?
                    """,
                    tuple(values),
                )

                connection.commit()

                return self.get_partition(
                    partition_id,
                    connection=connection,
                )

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # Shard lifecycle
    # ------------------------------------------------------------------

    def register_shard(
        self,
        shard_id: str,
        region: str,
        zone: str,
        rack: str = "",
        endpoint: str = "",
        capacity: float = 1.0,
    ) -> ShardDescriptor:
        if not shard_id:
            raise ValueError(
                "shard_id must not be empty"
            )

        if capacity <= 0:
            raise ValueError(
                "capacity must be > 0"
            )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                existing = connection.execute(
                    """
                    SELECT *
                    FROM shards
                    WHERE shard_id = ?
                    """,
                    (shard_id,),
                ).fetchone()

                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO shards(
                            shard_id,
                            region,
                            zone,
                            rack,
                            endpoint,
                            capacity,
                            used_capacity,
                            health,
                            generation,
                            fencing_epoch,
                            created_at,
                            updated_at
                        )
                        VALUES(
                            ?, ?, ?, ?, ?, ?, 0.0,
                            'healthy', 1, 1, ?, ?
                        )
                        """,
                        (
                            shard_id,
                            region,
                            zone,
                            rack,
                            endpoint,
                            float(capacity),
                            now,
                            now,
                        ),
                    )

                else:
                    connection.execute(
                        """
                        UPDATE shards
                        SET
                            region = ?,
                            zone = ?,
                            rack = ?,
                            endpoint = ?,
                            capacity = ?,
                            health = 'healthy',
                            generation = generation + 1,
                            updated_at = ?
                        WHERE shard_id = ?
                        """,
                        (
                            region,
                            zone,
                            rack,
                            endpoint,
                            float(capacity),
                            now,
                            shard_id,
                        ),
                    )

                connection.commit()

                return self.get_shard(
                    shard_id,
                    connection=connection,
                )

            finally:
                connection.close()

    def get_shard(
        self,
        shard_id: str,
        connection: Optional[sqlite3.Connection] = None,
    ) -> ShardDescriptor:
        own_connection = connection is None

        if own_connection:
            connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM shards
                WHERE shard_id = ?
                """,
                (shard_id,),
            ).fetchone()

            if row is None:
                raise PartitionShardError(
                    f"shard {shard_id} does not exist"
                )

            return ShardDescriptor(
                shard_id=row["shard_id"],
                region=row["region"],
                zone=row["zone"],
                rack=row["rack"],
                endpoint=row["endpoint"],
                capacity=row["capacity"],
                used_capacity=row["used_capacity"],
                health=row["health"],
                generation=row["generation"],
                fencing_epoch=row["fencing_epoch"],
            )

        finally:
            if own_connection:
                connection.close()

    def update_shard_health(
        self,
        shard_id: str,
        health: str,
    ) -> ShardDescriptor:
        allowed = {
            "healthy",
            "degraded",
            "draining",
            "fenced",
            "offline",
        }

        if health not in allowed:
            raise ValueError(
                f"unsupported shard health: {health}"
            )

        with self._lock:
            connection = self._connect()

            try:
                shard = self.get_shard(
                    shard_id,
                    connection=connection,
                )

                fencing_epoch = shard.fencing_epoch

                if health == "fenced":
                    fencing_epoch += 1

                connection.execute(
                    """
                    UPDATE shards
                    SET
                        health = ?,
                        fencing_epoch = ?,
                        updated_at = ?
                    WHERE shard_id = ?
                    """,
                    (
                        health,
                        fencing_epoch,
                        time.time(),
                        shard_id,
                    ),
                )

                connection.commit()

                return self.get_shard(
                    shard_id,
                    connection=connection,
                )

            finally:
                connection.close()

    def update_shard_capacity(
        self,
        shard_id: str,
        used_capacity: float,
    ) -> ShardDescriptor:
        if used_capacity < 0:
            raise ValueError(
                "used_capacity must be >= 0"
            )

        with self._lock:
            connection = self._connect()

            try:
                self.get_shard(
                    shard_id,
                    connection=connection,
                )

                connection.execute(
                    """
                    UPDATE shards
                    SET
                        used_capacity = ?,
                        updated_at = ?
                    WHERE shard_id = ?
                    """,
                    (
                        float(used_capacity),
                        time.time(),
                        shard_id,
                    ),
                )

                connection.commit()

                return self.get_shard(
                    shard_id,
                    connection=connection,
                )

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # Placement epochs
    # ------------------------------------------------------------------

    def current_placement_epoch(self) -> int:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT MAX(epoch)
                FROM placement_epochs
                """
            ).fetchone()

            return int(row[0] or 1)

        finally:
            connection.close()

    def begin_placement_epoch(
        self,
        reason: str,
    ) -> int:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT MAX(epoch)
                    FROM placement_epochs
                    """
                ).fetchone()

                next_epoch = int(row[0] or 0) + 1

                connection.execute(
                    """
                    INSERT INTO placement_epochs(
                        epoch,
                        created_at,
                        reason
                    )
                    VALUES(?, ?, ?)
                    """,
                    (
                        next_epoch,
                        time.time(),
                        reason,
                    ),
                )

                connection.commit()

                return next_epoch

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # Placement
    # ------------------------------------------------------------------

    def _eligible_shards(
        self,
        connection: sqlite3.Connection,
    ) -> list[sqlite3.Row]:
        return connection.execute(
            """
            SELECT *
            FROM shards
            WHERE health IN(
                'healthy',
                'degraded'
            )
            AND capacity > used_capacity
            ORDER BY
                used_capacity / CASE
                    WHEN capacity <= 0 THEN 1
                    ELSE capacity
                END ASC,
                region,
                zone,
                rack,
                shard_id
            """
        ).fetchall()

    def _select_replica_shards(
        self,
        partition_id: int,
        epoch: int,
        connection: sqlite3.Connection,
        replica_count: Optional[int] = None,
    ) -> list[sqlite3.Row]:
        desired = (
            self.replica_count
            if replica_count is None
            else replica_count
        )

        shards = self._eligible_shards(
            connection
        )

        if not shards:
            return []

        ranked = sorted(
            shards,
            key=lambda row: (
                -self.stable_shard_score(
                    partition_id,
                    row["shard_id"],
                    epoch,
                ),
                row["region"],
                row["zone"],
                row["rack"],
                row["shard_id"],
            ),
        )

        selected: list[sqlite3.Row] = []
        used_domains: set[tuple[str, str]] = set()

        for row in ranked:
            domain = (
                row["region"],
                row["zone"],
            )

            if domain not in used_domains:
                selected.append(row)
                used_domains.add(domain)

            if len(selected) >= desired:
                break

        if len(selected) < desired:
            selected_ids = {
                row["shard_id"]
                for row in selected
            }

            for row in ranked:
                if row["shard_id"] in selected_ids:
                    continue

                selected.append(row)

                if len(selected) >= desired:
                    break

        return selected

    def place_partition(
        self,
        partition_id: int,
        replica_count: Optional[int] = None,
        reason: str = "placement",
    ) -> list[ReplicaPlacement]:
        with self._lock:
            connection = self._connect()

            try:
                self.get_partition(
                    partition_id,
                    connection=connection,
                )

                epoch = self.current_placement_epoch()

                selected = self._select_replica_shards(
                    partition_id,
                    epoch,
                    connection,
                    replica_count=replica_count,
                )

                if not selected:
                    raise PartitionShardError(
                        "no eligible shard available"
                    )

                connection.execute(
                    """
                    UPDATE placements
                    SET state = 'superseded',
                        updated_at = ?
                    WHERE partition_id = ?
                      AND placement_epoch < ?
                      AND state = 'active'
                    """,
                    (
                        time.time(),
                        partition_id,
                        epoch,
                    ),
                )

                placements: list[ReplicaPlacement] = []

                for index, shard in enumerate(selected):
                    role = (
                        "primary"
                        if index == 0
                        else "replica"
                    )

                    connection.execute(
                        """
                        INSERT OR REPLACE INTO placements(
                            partition_id,
                            shard_id,
                            replica_role,
                            placement_epoch,
                            state,
                            created_at,
                            updated_at
                        )
                        VALUES(
                            ?, ?, ?, ?, 'active', ?, ?
                        )
                        """,
                        (
                            partition_id,
                            shard["shard_id"],
                            role,
                            epoch,
                            time.time(),
                            time.time(),
                        ),
                    )

                    placements.append(
                        ReplicaPlacement(
                            partition_id=partition_id,
                            shard_id=shard["shard_id"],
                            replica_role=role,
                            placement_epoch=epoch,
                            state="active",
                        )
                    )

                    self._event(
                        connection,
                        partition_id,
                        shard["shard_id"],
                        "partition_placed",
                        {
                            "epoch": epoch,
                            "role": role,
                            "reason": reason,
                        },
                    )

                connection.execute(
                    """
                    UPDATE partitions
                    SET epoch = ?,
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        epoch,
                        time.time(),
                        partition_id,
                    ),
                )

                connection.commit()

                return placements

            finally:
                connection.close()

    def placements_for(
        self,
        partition_id: int,
        active_only: bool = True,
    ) -> list[ReplicaPlacement]:
        connection = self._connect()

        try:
            if active_only:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM placements
                    WHERE partition_id = ?
                      AND state = 'active'
                    ORDER BY
                        placement_epoch DESC,
                        replica_role
                    """,
                    (partition_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM placements
                    WHERE partition_id = ?
                    ORDER BY
                        placement_epoch DESC,
                        replica_role
                    """,
                    (partition_id,),
                ).fetchall()

            return [
                ReplicaPlacement(
                    partition_id=row["partition_id"],
                    shard_id=row["shard_id"],
                    replica_role=row["replica_role"],
                    placement_epoch=row["placement_epoch"],
                    state=row["state"],
                )
                for row in rows
            ]

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Ownership and fencing
    # ------------------------------------------------------------------

    def acquire_partition_lease(
        self,
        partition_id: int,
        owner_id: str,
        now: Optional[float] = None,
    ) -> PartitionLease:
        if not owner_id:
            raise ValueError(
                "owner_id must not be empty"
            )

        now = time.time() if now is None else now

        with self._lock:
            connection = self._connect()

            try:
                partition = self.get_partition(
                    partition_id,
                    connection=connection,
                )

                existing = connection.execute(
                    """
                    SELECT *
                    FROM partition_leases
                    WHERE partition_id = ?
                    """,
                    (partition_id,),
                ).fetchone()

                if (
                    existing is not None
                    and existing["lease_until"] > now
                    and existing["owner_id"] != owner_id
                ):
                    raise PartitionShardError(
                        f"partition {partition_id} is owned by "
                        f"{existing['owner_id']}"
                    )

                generation = (
                    int(existing["generation"]) + 1
                    if existing is not None
                    else partition.generation + 1
                )

                fencing_epoch = (
                    int(existing["fencing_epoch"]) + 1
                    if existing is not None
                    else generation
                )

                lease_until = (
                    now + self.lease_seconds
                )

                connection.execute(
                    """
                    INSERT OR REPLACE INTO partition_leases(
                        partition_id,
                        owner_id,
                        generation,
                        fencing_epoch,
                        lease_until,
                        updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        partition_id,
                        owner_id,
                        generation,
                        fencing_epoch,
                        lease_until,
                        now,
                    ),
                )

                connection.execute(
                    """
                    UPDATE partitions
                    SET
                        owner_id = ?,
                        generation = ?,
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        owner_id,
                        generation,
                        now,
                        partition_id,
                    ),
                )

                connection.commit()

                return PartitionLease(
                    partition_id=partition_id,
                    owner_id=owner_id,
                    generation=generation,
                    fencing_epoch=fencing_epoch,
                    lease_until=lease_until,
                )

            finally:
                connection.close()

    def renew_partition_lease(
        self,
        lease: PartitionLease,
        now: Optional[float] = None,
    ) -> PartitionLease:
        now = time.time() if now is None else now

        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM partition_leases
                    WHERE partition_id = ?
                    """,
                    (lease.partition_id,),
                ).fetchone()

                if row is None:
                    raise PartitionShardError(
                        "partition lease does not exist"
                    )

                if (
                    row["owner_id"] != lease.owner_id
                    or int(row["generation"])
                    != lease.generation
                    or int(row["fencing_epoch"])
                    != lease.fencing_epoch
                ):
                    raise PartitionShardError(
                        "stale partition lease"
                    )

                if row["lease_until"] < now:
                    raise PartitionShardError(
                        "partition lease expired"
                    )

                lease_until = (
                    now + self.lease_seconds
                )

                connection.execute(
                    """
                    UPDATE partition_leases
                    SET lease_until = ?,
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        lease_until,
                        now,
                        lease.partition_id,
                    ),
                )

                connection.commit()

                return PartitionLease(
                    partition_id=lease.partition_id,
                    owner_id=lease.owner_id,
                    generation=lease.generation,
                    fencing_epoch=lease.fencing_epoch,
                    lease_until=lease_until,
                )

            finally:
                connection.close()

    def release_partition_lease(
        self,
        lease: PartitionLease,
    ) -> bool:
        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    DELETE FROM partition_leases
                    WHERE partition_id = ?
                      AND owner_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                    """,
                    (
                        lease.partition_id,
                        lease.owner_id,
                        lease.generation,
                        lease.fencing_epoch,
                    ),
                )

                if cursor.rowcount:
                    connection.execute(
                        """
                        UPDATE partitions
                        SET owner_id = NULL,
                            updated_at = ?
                        WHERE partition_id = ?
                          AND owner_id = ?
                        """,
                        (
                            time.time(),
                            lease.partition_id,
                            lease.owner_id,
                        ),
                    )

                connection.commit()

                return cursor.rowcount > 0

            finally:
                connection.close()

    def recover_expired_partition_leases(
        self,
        now: Optional[float] = None,
    ) -> int:
        now = time.time() if now is None else now

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT partition_id
                    FROM partition_leases
                    WHERE lease_until <= ?
                    """,
                    (now,),
                ).fetchall()

                count = 0

                for row in rows:
                    partition_id = int(
                        row["partition_id"]
                    )

                    connection.execute(
                        """
                        DELETE FROM partition_leases
                        WHERE partition_id = ?
                          AND lease_until <= ?
                        """,
                        (
                            partition_id,
                            now,
                        ),
                    )

                    connection.execute(
                        """
                        UPDATE partitions
                        SET owner_id = NULL,
                            updated_at = ?
                        WHERE partition_id = ?
                        """,
                        (
                            now,
                            partition_id,
                        ),
                    )

                    self._event(
                        connection,
                        partition_id,
                        None,
                        "partition_lease_recovered",
                        {
                            "reason": "expired"
                        },
                    )

                    count += 1

                connection.commit()

                return count

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # Shard fencing
    # ------------------------------------------------------------------

    def fence_shard(
        self,
        shard_id: str,
        reason: str = "fault",
    ) -> ShardDescriptor:
        with self._lock:
            connection = self._connect()

            try:
                shard = self.get_shard(
                    shard_id,
                    connection=connection,
                )

                fencing_epoch = (
                    shard.fencing_epoch + 1
                )

                connection.execute(
                    """
                    UPDATE shards
                    SET
                        health = 'fenced',
                        fencing_epoch = ?,
                        updated_at = ?
                    WHERE shard_id = ?
                    """,
                    (
                        fencing_epoch,
                        time.time(),
                        shard_id,
                    ),
                )

                self._event(
                    connection,
                    None,
                    shard_id,
                    "shard_fenced",
                    {
                        "reason": reason,
                        "fencing_epoch": fencing_epoch,
                    },
                )

                connection.commit()

                return self.get_shard(
                    shard_id,
                    connection=connection,
                )

            finally:
                connection.close()

    def recover_fenced_shard(
        self,
        shard_id: str,
    ) -> ShardDescriptor:
        with self._lock:
            connection = self._connect()

            try:
                shard = self.get_shard(
                    shard_id,
                    connection=connection,
                )

                if shard.health != "fenced":
                    return shard

                connection.execute(
                    """
                    UPDATE shards
                    SET
                        health = 'healthy',
                        generation = generation + 1,
                        fencing_epoch = fencing_epoch + 1,
                        updated_at = ?
                    WHERE shard_id = ?
                    """,
                    (
                        time.time(),
                        shard_id,
                    ),
                )

                self._event(
                    connection,
                    None,
                    shard_id,
                    "shard_recovered",
                    {},
                )

                connection.commit()

                return self.get_shard(
                    shard_id,
                    connection=connection,
                )

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # Rebalancing / movement
    # ------------------------------------------------------------------

    def plan_move(
        self,
        partition_id: int,
        target_shard: str,
        source_shard: Optional[str] = None,
    ) -> ShardMovePlan:
        plan_id = uuid.uuid4().hex
        epoch = self.current_placement_epoch()
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                self.get_partition(
                    partition_id,
                    connection=connection,
                )

                self.get_shard(
                    target_shard,
                    connection=connection,
                )

                if source_shard is not None:
                    self.get_shard(
                        source_shard,
                        connection=connection,
                    )

                connection.execute(
                    """
                    INSERT INTO move_plans(
                        plan_id,
                        partition_id,
                        source_shard,
                        target_shard,
                        placement_epoch,
                        state,
                        created_at,
                        updated_at
                    )
                    VALUES(
                        ?, ?, ?, ?, ?, 'planned', ?, ?
                    )
                    """,
                    (
                        plan_id,
                        partition_id,
                        source_shard,
                        target_shard,
                        epoch,
                        now,
                        now,
                    ),
                )

                connection.commit()

                return ShardMovePlan(
                    plan_id=plan_id,
                    partition_id=partition_id,
                    source_shard=source_shard,
                    target_shard=target_shard,
                    placement_epoch=epoch,
                    state="planned",
                    created_at=now,
                )

            finally:
                connection.close()

    def execute_move(
        self,
        plan_id: str,
    ) -> ShardMovePlan:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM move_plans
                    WHERE plan_id = ?
                    """,
                    (plan_id,),
                ).fetchone()

                if row is None:
                    raise PartitionShardError(
                        f"move plan {plan_id} does not exist"
                    )

                if row["state"] not in {
                    "planned",
                    "moving",
                }:
                    raise PartitionShardError(
                        f"move plan is {row['state']}"
                    )

                now = time.time()

                connection.execute(
                    """
                    UPDATE move_plans
                    SET state = 'moving',
                        updated_at = ?
                    WHERE plan_id = ?
                    """,
                    (
                        now,
                        plan_id,
                    ),
                )

                connection.execute(
                    """
                    INSERT OR REPLACE INTO placements(
                        partition_id,
                        shard_id,
                        replica_role,
                        placement_epoch,
                        state,
                        created_at,
                        updated_at
                    )
                    VALUES(
                        ?, ?, 'replica', ?, 'active', ?, ?
                    )
                    """,
                    (
                        row["partition_id"],
                        row["target_shard"],
                        row["placement_epoch"],
                        now,
                        now,
                    ),
                )

                if row["source_shard"] is not None:
                    connection.execute(
                        """
                        UPDATE placements
                        SET state = 'draining',
                            updated_at = ?
                        WHERE partition_id = ?
                          AND shard_id = ?
                          AND state = 'active'
                        """,
                        (
                            now,
                            row["partition_id"],
                            row["source_shard"],
                        ),
                    )

                connection.execute(
                    """
                    UPDATE move_plans
                    SET state = 'complete',
                        updated_at = ?
                    WHERE plan_id = ?
                    """,
                    (
                        now,
                        plan_id,
                    ),
                )

                self._event(
                    connection,
                    row["partition_id"],
                    row["target_shard"],
                    "partition_move_complete",
                    {
                        "plan_id": plan_id,
                        "source_shard": row["source_shard"],
                    },
                )

                connection.commit()

                return ShardMovePlan(
                    plan_id=row["plan_id"],
                    partition_id=row["partition_id"],
                    source_shard=row["source_shard"],
                    target_shard=row["target_shard"],
                    placement_epoch=row["placement_epoch"],
                    state="complete",
                    created_at=row["created_at"],
                )

            finally:
                connection.close()

    def rebalance_partition(
        self,
        partition_id: int,
        reason: str = "rebalance",
    ) -> list[ReplicaPlacement]:
        epoch = self.begin_placement_epoch(
            reason
        )

        with self._lock:
            connection = self._connect()

            try:
                self.get_partition(
                    partition_id,
                    connection=connection,
                )

                selected = self._select_replica_shards(
                    partition_id,
                    epoch,
                    connection,
                )

                if not selected:
                    raise PartitionShardError(
                        "no shard available for rebalance"
                    )

                now = time.time()

                connection.execute(
                    """
                    UPDATE placements
                    SET state = 'superseded',
                        updated_at = ?
                    WHERE partition_id = ?
                      AND state = 'active'
                    """,
                    (
                        now,
                        partition_id,
                    ),
                )

                result = []

                for index, shard in enumerate(selected):
                    role = (
                        "primary"
                        if index == 0
                        else "replica"
                    )

                    connection.execute(
                        """
                        INSERT INTO placements(
                            partition_id,
                            shard_id,
                            replica_role,
                            placement_epoch,
                            state,
                            created_at,
                            updated_at
                        )
                        VALUES(
                            ?, ?, ?, ?, 'active', ?, ?
                        )
                        """,
                        (
                            partition_id,
                            shard["shard_id"],
                            role,
                            epoch,
                            now,
                            now,
                        ),
                    )

                    result.append(
                        ReplicaPlacement(
                            partition_id=partition_id,
                            shard_id=shard["shard_id"],
                            replica_role=role,
                            placement_epoch=epoch,
                            state="active",
                        )
                    )

                connection.execute(
                    """
                    UPDATE partitions
                    SET epoch = ?,
                        updated_at = ?
                    WHERE partition_id = ?
                    """,
                    (
                        epoch,
                        now,
                        partition_id,
                    ),
                )

                connection.commit()

                return result

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # Split / merge architecture
    # ------------------------------------------------------------------

    def plan_split(
        self,
        partition_id: int,
        target_count: int = 2,
    ) -> str:
        if target_count < 2:
            raise ValueError(
                "target_count must be >= 2"
            )

        plan_id = uuid.uuid4().hex
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                self.get_partition(
                    partition_id,
                    connection=connection,
                )

                connection.execute(
                    """
                    INSERT INTO split_merge_plans(
                        plan_id,
                        partition_id,
                        operation,
                        target_count,
                        state,
                        created_at,
                        updated_at
                    )
                    VALUES(
                        ?, ?, 'split', ?, 'planned', ?, ?
                    )
                    """,
                    (
                        plan_id,
                        partition_id,
                        target_count,
                        now,
                        now,
                    ),
                )

                connection.commit()

                return plan_id

            finally:
                connection.close()

    def plan_merge(
        self,
        partition_id: int,
        target_partition_id: int,
    ) -> str:
        plan_id = uuid.uuid4().hex
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                self.get_partition(
                    partition_id,
                    connection=connection,
                )

                self.get_partition(
                    target_partition_id,
                    connection=connection,
                )

                connection.execute(
                    """
                    INSERT INTO split_merge_plans(
                        plan_id,
                        partition_id,
                        operation,
                        target_count,
                        state,
                        created_at,
                        updated_at
                    )
                    VALUES(
                        ?, ?, 'merge', ?, 'planned', ?, ?
                    )
                    """,
                    (
                        plan_id,
                        partition_id,
                        target_partition_id,
                        now,
                        now,
                    ),
                )

                connection.commit()

                return plan_id

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def checkpoint(
        self,
        partition_id: int,
        cursor: str,
    ) -> PartitionCheckpoint:
        if cursor is None:
            raise ValueError(
                "cursor must not be None"
            )

        with self._lock:
            connection = self._connect()

            try:
                partition = self.get_partition(
                    partition_id,
                    connection=connection,
                )

                now = time.time()

                connection.execute(
                    """
                    INSERT OR REPLACE INTO checkpoints(
                        partition_id,
                        epoch,
                        cursor,
                        updated_at
                    )
                    VALUES(?, ?, ?, ?)
                    """,
                    (
                        partition_id,
                        partition.epoch,
                        str(cursor),
                        now,
                    ),
                )

                connection.commit()

                return PartitionCheckpoint(
                    partition_id=partition_id,
                    epoch=partition.epoch,
                    cursor=str(cursor),
                    updated_at=now,
                )

            finally:
                connection.close()

    def read_checkpoint(
        self,
        partition_id: int,
    ) -> Optional[PartitionCheckpoint]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM checkpoints
                WHERE partition_id = ?
                """,
                (partition_id,),
            ).fetchone()

            if row is None:
                return None

            return PartitionCheckpoint(
                partition_id=row["partition_id"],
                epoch=row["epoch"],
                cursor=row["cursor"],
                updated_at=row["updated_at"],
            )

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        partition_id: Optional[int],
        shard_id: Optional[str],
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        connection.execute(
            """
            INSERT INTO partition_events(
                event_id,
                partition_id,
                shard_id,
                event_type,
                payload_json,
                created_at
            )
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                partition_id,
                shard_id,
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
                FROM partition_events
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            return [
                {
                    "event_id": row["event_id"],
                    "partition_id": row["partition_id"],
                    "shard_id": row["shard_id"],
                    "event_type": row["event_type"],
                    "payload": json.loads(
                        row["payload_json"] or "{}"
                    ),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Discovery / management views
    # ------------------------------------------------------------------

    def hot_partitions(
        self,
        limit: int = 1000,
    ) -> list[PartitionDescriptor]:
        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT *
                FROM partitions
                WHERE hotness >= ?
                ORDER BY
                    hotness DESC,
                    priority DESC,
                    partition_id
                LIMIT ?
                """,
                (
                    self.hotness_threshold,
                    limit,
                ),
            ).fetchall()

            return [
                PartitionDescriptor(
                    partition_id=row["partition_id"],
                    partition_key=row["partition_key"],
                    epoch=row["epoch"],
                    state=row["state"],
                    priority=row["priority"],
                    hotness=row["hotness"],
                    owner_id=row["owner_id"],
                    generation=row["generation"],
                )
                for row in rows
            ]

        finally:
            connection.close()

    def partitions_for_shard(
        self,
        shard_id: str,
        active_only: bool = True,
        limit: int = 10000,
    ) -> list[int]:
        connection = self._connect()

        try:
            if active_only:
                rows = connection.execute(
                    """
                    SELECT DISTINCT partition_id
                    FROM placements
                    WHERE shard_id = ?
                      AND state = 'active'
                    ORDER BY partition_id
                    LIMIT ?
                    """,
                    (
                        shard_id,
                        limit,
                    ),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT DISTINCT partition_id
                    FROM placements
                    WHERE shard_id = ?
                    ORDER BY partition_id
                    LIMIT ?
                    """,
                    (
                        shard_id,
                        limit,
                    ),
                ).fetchall()

            return [
                int(row["partition_id"])
                for row in rows
            ]

        finally:
            connection.close()

    def list_shards(
        self,
        health: Optional[str] = None,
        limit: int = 10000,
    ) -> list[ShardDescriptor]:
        connection = self._connect()

        try:
            if health is None:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM shards
                    ORDER BY
                        region,
                        zone,
                        rack,
                        shard_id
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM shards
                    WHERE health = ?
                    ORDER BY
                        region,
                        zone,
                        rack,
                        shard_id
                    LIMIT ?
                    """,
                    (
                        health,
                        limit,
                    ),
                ).fetchall()

            return [
                ShardDescriptor(
                    shard_id=row["shard_id"],
                    region=row["region"],
                    zone=row["zone"],
                    rack=row["rack"],
                    endpoint=row["endpoint"],
                    capacity=row["capacity"],
                    used_capacity=row["used_capacity"],
                    health=row["health"],
                    generation=row["generation"],
                    fencing_epoch=row["fencing_epoch"],
                )
                for row in rows
            ]

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Capacity / statistics
    # ------------------------------------------------------------------

    def capacity(self) -> PartitionShardCapacity:
        connection = self._connect()

        try:
            logical_partitions = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM partitions
                    """
                ).fetchone()[0]
            )

            physical_shards = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM shards
                    """
                ).fetchone()[0]
            )

            active_shards = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM shards
                    WHERE health IN(
                        'healthy',
                        'degraded',
                        'draining'
                    )
                    """
                ).fetchone()[0]
            )

            healthy_shards = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM shards
                    WHERE health = 'healthy'
                    """
                ).fetchone()[0]
            )

            assigned_partitions = int(
                connection.execute(
                    """
                    SELECT COUNT(DISTINCT partition_id)
                    FROM placements
                    WHERE state = 'active'
                    """
                ).fetchone()[0]
            )

            moving_partitions = int(
                connection.execute(
                    """
                    SELECT COUNT(DISTINCT partition_id)
                    FROM move_plans
                    WHERE state = 'moving'
                    """
                ).fetchone()[0]
            )

            fenced_shards = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM shards
                    WHERE health = 'fenced'
                    """
                ).fetchone()[0]
            )

            hot_partitions = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM partitions
                    WHERE hotness >= ?
                    """,
                    (self.hotness_threshold,),
                ).fetchone()[0]
            )

            return PartitionShardCapacity(
                logical_partitions=logical_partitions,
                physical_shards=physical_shards,
                active_shards=active_shards,
                healthy_shards=healthy_shards,
                assigned_partitions=assigned_partitions,
                moving_partitions=moving_partitions,
                fenced_shards=fenced_shards,
                hot_partitions=hot_partitions,
            )

        finally:
            connection.close()

    def stats(self) -> dict[str, Any]:
        capacity = self.capacity()

        return {
            "version": self.VERSION,
            "logical_partition_space":
                self.logical_partition_space,
            "configured_replica_count":
                self.replica_count,
            "lease_seconds":
                self.lease_seconds,
            "hotness_threshold":
                self.hotness_threshold,
            "logical_partitions":
                capacity.logical_partitions,
            "physical_shards":
                capacity.physical_shards,
            "active_shards":
                capacity.active_shards,
            "healthy_shards":
                capacity.healthy_shards,
            "assigned_partitions":
                capacity.assigned_partitions,
            "moving_partitions":
                capacity.moving_partitions,
            "fenced_shards":
                capacity.fenced_shards,
            "hot_partitions":
                capacity.hot_partitions,
            "current_placement_epoch":
                self.current_placement_epoch(),
            "fixed_global_execution_limit": False,
            "fixed_physical_shard_limit": False,
            "elastic_shard_capacity": True,
            "epoch_based_placement": True,
            "replica_aware": True,
            "lease_based_ownership": True,
            "fencing_enabled": True,
            "failure_aware": True,
            "hot_partition_detection": True,
            "split_merge_hooks": True,
            "restart_safe_checkpoints": True,
            "enormous_scale_target": True,
            "billions_to_trillions_target": True,
        }

    # ------------------------------------------------------------------
    # Continuous supervision
    # ------------------------------------------------------------------

    def tick(self) -> dict[str, Any]:
        recovered = (
            self.recover_expired_partition_leases()
        )

        return {
            "recovered_expired_leases": recovered,
            "capacity": self.capacity(),
            "placement_epoch":
                self.current_placement_epoch(),
        }

    def run(
        self,
        interval: float = 30.0,
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
