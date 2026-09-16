"""
OUR SEARCH
Phase 9.2 — Massive Worker Fleet Architecture

Direct architecture target:
    enormous distributed worker fleets capable of supporting
    billions/trillions of public-Web resources.

This layer manages worker identity, generations, health,
capacity, capabilities, placement metadata, draining,
fencing, and fleet accounting.

It is intentionally independent of any fixed small worker count.
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


VERSION = "massive-worker-fleet.v1"

DEFAULT_HEARTBEAT_TIMEOUT = 90.0
DEFAULT_DRAIN_TIMEOUT = 300.0


@dataclass(frozen=True)
class WorkerCapability:
    name: str
    capacity: int = 1
    weight: float = 1.0


@dataclass(frozen=True)
class FleetWorker:
    worker_id: str
    generation: int
    state: str
    capacity: int
    active_work: int
    registered_at: float
    last_heartbeat: float
    heartbeat_deadline: float
    fencing_epoch: int
    region: Optional[str]
    zone: Optional[str]
    rack: Optional[str]
    endpoint: Optional[str]
    capabilities: tuple[str, ...]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class WorkerAssignment:
    worker_id: str
    generation: int
    partition_id: Optional[int]
    workload_type: Optional[str]
    fencing_epoch: int
    assigned_at: float


@dataclass(frozen=True)
class FleetCapacity:
    total_capacity: int
    active_capacity: int
    active_workers: int
    draining_workers: int
    unhealthy_workers: int
    active_work: int
    available_slots: int


@dataclass(frozen=True)
class FleetEvent:
    event_id: str
    worker_id: str
    generation: int
    event: str
    timestamp: float
    metadata: dict[str, Any]


class MassiveWorkerFleet:
    """
    Durable worker-fleet control architecture.

    The fleet has no architectural maximum worker count.

    Physical deployment can scale independently by adding worker
    processes, machines, zones, regions, or future execution
    clusters.

    This module owns worker lifecycle and fleet-level accounting;
    execution work itself remains owned by the distributed
    execution fabric.
    """

    def __init__(
        self,
        storage_root: str,
        heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT,
        drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
    ) -> None:
        if heartbeat_timeout <= 0:
            raise ValueError(
                "heartbeat_timeout must be positive"
            )

        if drain_timeout <= 0:
            raise ValueError(
                "drain_timeout must be positive"
            )

        self.storage_root = storage_root
        self.heartbeat_timeout = heartbeat_timeout
        self.drain_timeout = drain_timeout

        self.db_path = (
            f"{storage_root}/massive_worker_fleet.db"
        )

        self._lock = threading.RLock()
        self._running = False

        self.stats_data = {
            "version": VERSION,
            "registrations": 0,
            "heartbeats": 0,
            "fences": 0,
            "expirations": 0,
            "drains_started": 0,
            "drains_completed": 0,
            "worker_removals": 0,
            "events": 0,
            "fixed_worker_limit": False,
            "enormous_scale_target": True,
            "billions_to_trillions_target": True,
        }

        self._initialize()

    # ---------------------------------------------------------------
    # Database
    # ---------------------------------------------------------------

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
                CREATE TABLE IF NOT EXISTS fleet_workers (
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
                    rack TEXT,
                    endpoint TEXT,
                    capabilities_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    drain_started_at REAL,
                    drain_deadline REAL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                idx_fleet_workers_state
                ON fleet_workers(
                    state,
                    last_heartbeat
                );

                CREATE INDEX IF NOT EXISTS
                idx_fleet_workers_region
                ON fleet_workers(
                    region,
                    zone,
                    state
                );

                CREATE INDEX IF NOT EXISTS
                idx_fleet_workers_capacity
                ON fleet_workers(
                    state,
                    active_work,
                    capacity
                );

                CREATE TABLE IF NOT EXISTS fleet_assignments (
                    worker_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    partition_id INTEGER,
                    workload_type TEXT,
                    fencing_epoch INTEGER NOT NULL,
                    assigned_at REAL NOT NULL,
                    PRIMARY KEY (
                        worker_id,
                        generation
                    )
                );

                CREATE INDEX IF NOT EXISTS
                idx_fleet_assignments_partition
                ON fleet_assignments(
                    partition_id
                );

                CREATE TABLE IF NOT EXISTS fleet_events (
                    event_id TEXT PRIMARY KEY,
                    worker_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    event TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    metadata_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                idx_fleet_events_worker
                ON fleet_events(
                    worker_id,
                    timestamp
                );

                CREATE TABLE IF NOT EXISTS
                fleet_capability_index (
                    capability TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    capacity INTEGER NOT NULL,
                    weight REAL NOT NULL,
                    state TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (
                        capability,
                        worker_id,
                        generation
                    )
                );

                CREATE INDEX IF NOT EXISTS
                idx_fleet_capability_ready
                ON fleet_capability_index(
                    capability,
                    state,
                    capacity DESC
                );
                """
            )

            connection.commit()

        finally:
            connection.close()

    # ---------------------------------------------------------------
    # Identity
    # ---------------------------------------------------------------

    @staticmethod
    def new_worker_id() -> str:
        return (
            "fleet-worker-"
            + uuid.uuid4().hex
        )

    @staticmethod
    def stable_worker_score(
        worker_id: str,
        namespace: str = "global",
    ) -> int:
        raw = (
            f"{namespace}|{worker_id}"
        )

        return int.from_bytes(
            hashlib.sha256(
                raw.encode("utf-8")
            ).digest()[:8],
            "big",
        )

    # ---------------------------------------------------------------
    # Registration
    # ---------------------------------------------------------------

    def register(
        self,
        worker_id: Optional[str] = None,
        capacity: int = 1,
        region: Optional[str] = None,
        zone: Optional[str] = None,
        rack: Optional[str] = None,
        endpoint: Optional[str] = None,
        capabilities: Optional[
            Iterable[WorkerCapability | str]
        ] = None,
        metadata: Optional[
            dict[str, Any]
        ] = None,
    ) -> FleetWorker:
        if capacity <= 0:
            raise ValueError(
                "capacity must be positive"
            )

        if worker_id is None:
            worker_id = self.new_worker_id()

        normalized_capabilities: list[
            WorkerCapability
        ] = []

        for capability in (
            capabilities or []
        ):
            if isinstance(
                capability,
                WorkerCapability,
            ):
                normalized_capabilities.append(
                    capability
                )
            else:
                normalized_capabilities.append(
                    WorkerCapability(
                        name=str(
                            capability
                        ),
                    )
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
                FROM fleet_workers
                WHERE worker_id = ?
                """,
                (worker_id,),
            ).fetchone()

            if row is None:
                generation = 1
                fencing_epoch = 1

                connection.execute(
                    """
                    INSERT INTO fleet_workers (
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
                        rack,
                        endpoint,
                        capabilities_json,
                        metadata_json,
                        updated_at
                    )
                    VALUES (
                        ?, ?, 'active', ?, 0,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
                        rack,
                        endpoint,
                        json.dumps(
                            [
                                asdict(item)
                                for item
                                in normalized_capabilities
                            ],
                            sort_keys=True,
                        ),
                        json.dumps(
                            metadata or {},
                            sort_keys=True,
                            default=str,
                        ),
                        now,
                    ),
                )

            else:
                generation = (
                    int(
                        row["generation"]
                    )
                    + 1
                )

                fencing_epoch = (
                    int(
                        row["fencing_epoch"]
                    )
                    + 1
                )

                connection.execute(
                    """
                    UPDATE fleet_workers
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
                        rack = ?,
                        endpoint = ?,
                        capabilities_json = ?,
                        metadata_json = ?,
                        drain_started_at = NULL,
                        drain_deadline = NULL,
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
                        rack,
                        endpoint,
                        json.dumps(
                            [
                                asdict(item)
                                for item
                                in normalized_capabilities
                            ],
                            sort_keys=True,
                        ),
                        json.dumps(
                            metadata or {},
                            sort_keys=True,
                            default=str,
                        ),
                        now,
                        worker_id,
                    ),
                )

            connection.execute(
                """
                DELETE FROM fleet_capability_index
                WHERE worker_id = ?
                """,
                (worker_id,),
            )

            for capability in normalized_capabilities:
                connection.execute(
                    """
                    INSERT INTO
                    fleet_capability_index (
                        capability,
                        worker_id,
                        generation,
                        capacity,
                        weight,
                        state,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, 'active', ?)
                    """,
                    (
                        capability.name,
                        worker_id,
                        generation,
                        capability.capacity,
                        capability.weight,
                        now,
                    ),
                )

            self._event(
                connection,
                worker_id,
                generation,
                "registered",
                {
                    "capacity": capacity,
                    "region": region,
                    "zone": zone,
                    "rack": rack,
                },
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "registrations"
        ] += 1

        return self.get(
            worker_id
        )

    # ---------------------------------------------------------------
    # Worker state
    # ---------------------------------------------------------------

    def get(
        self,
        worker_id: str,
    ) -> Optional[FleetWorker]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM fleet_workers
                WHERE worker_id = ?
                """,
                (worker_id,),
            ).fetchone()

        finally:
            connection.close()

        if row is None:
            return None

        return self._worker_from_row(
            row
        )

    @staticmethod
    def _worker_from_row(
        row: sqlite3.Row,
    ) -> FleetWorker:
        capabilities_raw = json.loads(
            row["capabilities_json"]
        )

        return FleetWorker(
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
            rack=row["rack"],
            endpoint=row["endpoint"],
            capabilities=tuple(
                str(
                    item["name"]
                )
                for item in capabilities_raw
            ),
            metadata=json.loads(
                row["metadata_json"]
            ),
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
                UPDATE fleet_workers
                SET
                    state = CASE
                        WHEN state = 'draining'
                        THEN state
                        ELSE 'active'
                    END,
                    last_heartbeat = ?,
                    heartbeat_deadline = ?,
                    updated_at = ?
                WHERE
                    worker_id = ?
                    AND generation = ?
                    AND state NOT IN (
                        'fenced',
                        'removed'
                    )
                """,
                (
                    now,
                    deadline,
                    now,
                    worker_id,
                    generation,
                ),
            ).rowcount

            if updated:
                self._event(
                    connection,
                    worker_id,
                    generation,
                    "heartbeat",
                )

            connection.commit()

        finally:
            connection.close()

        if updated:
            self.stats_data[
                "heartbeats"
            ] += 1

        return updated == 1

    # ---------------------------------------------------------------
    # Draining
    # ---------------------------------------------------------------

    def start_drain(
        self,
        worker_id: str,
        generation: int,
    ) -> bool:
        now = time.time()
        deadline = (
            now + self.drain_timeout
        )

        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE fleet_workers
                SET
                    state = 'draining',
                    drain_started_at = ?,
                    drain_deadline = ?,
                    updated_at = ?
                WHERE
                    worker_id = ?
                    AND generation = ?
                    AND state = 'active'
                """,
                (
                    now,
                    deadline,
                    now,
                    worker_id,
                    generation,
                ),
            ).rowcount

            if updated:
                connection.execute(
                    """
                    UPDATE fleet_capability_index
                    SET
                        state = 'draining',
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

                self._event(
                    connection,
                    worker_id,
                    generation,
                    "drain_started",
                )

            connection.commit()

        finally:
            connection.close()

        if updated:
            self.stats_data[
                "drains_started"
            ] += 1

        return updated == 1

    def complete_drain(
        self,
        worker_id: str,
        generation: int,
    ) -> bool:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT active_work
                FROM fleet_workers
                WHERE
                    worker_id = ?
                    AND generation = ?
                    AND state = 'draining'
                """,
                (
                    worker_id,
                    generation,
                ),
            ).fetchone()

            if row is None:
                connection.rollback()
                return False

            if int(
                row["active_work"]
            ) != 0:
                connection.rollback()
                return False

            now = time.time()

            updated = connection.execute(
                """
                UPDATE fleet_workers
                SET
                    state = 'removed',
                    updated_at = ?
                WHERE
                    worker_id = ?
                    AND generation = ?
                    AND state = 'draining'
                    AND active_work = 0
                """,
                (
                    now,
                    worker_id,
                    generation,
                ),
            ).rowcount

            if updated:
                connection.execute(
                    """
                    UPDATE fleet_capability_index
                    SET
                        state = 'removed',
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

                self._event(
                    connection,
                    worker_id,
                    generation,
                    "drain_completed",
                )

            connection.commit()

        finally:
            connection.close()

        if updated:
            self.stats_data[
                "drains_completed"
            ] += 1

        return updated == 1

    # ---------------------------------------------------------------
    # Fencing / expiration
    # ---------------------------------------------------------------

    def fence(
        self,
        worker_id: str,
        generation: Optional[int] = None,
        reason: str = "operator",
    ) -> bool:
        connection = self._connect()

        try:
            if generation is None:
                row = connection.execute(
                    """
                    SELECT generation
                    FROM fleet_workers
                    WHERE worker_id = ?
                    """,
                    (worker_id,),
                ).fetchone()

                if row is None:
                    connection.rollback()
                    return False

                generation = int(
                    row["generation"]
                )

            now = time.time()

            updated = connection.execute(
                """
                UPDATE fleet_workers
                SET
                    state = 'fenced',
                    fencing_epoch =
                        fencing_epoch + 1,
                    updated_at = ?
                WHERE
                    worker_id = ?
                    AND generation = ?
                    AND state != 'removed'
                """,
                (
                    now,
                    worker_id,
                    generation,
                ),
            ).rowcount

            if updated:
                connection.execute(
                    """
                    UPDATE fleet_capability_index
                    SET
                        state = 'fenced',
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

                self._event(
                    connection,
                    worker_id,
                    generation,
                    "fenced",
                    {
                        "reason": reason,
                    },
                )

            connection.commit()

        finally:
            connection.close()

        if updated:
            self.stats_data[
                "fences"
            ] += 1

        return updated == 1

    def expire_unhealthy(
        self,
        now: Optional[float] = None,
    ) -> list[str]:
        if now is None:
            now = time.time()

        connection = self._connect()
        expired_ids: list[str] = []

        try:
            rows = connection.execute(
                """
                SELECT
                    worker_id,
                    generation
                FROM fleet_workers
                WHERE
                    state IN (
                        'active',
                        'draining'
                    )
                    AND heartbeat_deadline <= ?
                """,
                (now,),
            ).fetchall()

            for row in rows:
                worker_id = str(
                    row["worker_id"]
                )
                generation = int(
                    row["generation"]
                )

                updated = connection.execute(
                    """
                    UPDATE fleet_workers
                    SET
                        state = 'expired',
                        fencing_epoch =
                            fencing_epoch + 1,
                        updated_at = ?
                    WHERE
                        worker_id = ?
                        AND generation = ?
                        AND state IN (
                            'active',
                            'draining'
                        )
                    """,
                    (
                        now,
                        worker_id,
                        generation,
                    ),
                ).rowcount

                if updated:
                    expired_ids.append(
                        worker_id
                    )

                    connection.execute(
                        """
                        UPDATE fleet_capability_index
                        SET
                            state = 'expired',
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

                    self._event(
                        connection,
                        worker_id,
                        generation,
                        "expired",
                    )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "expirations"
        ] += len(expired_ids)

        return expired_ids

    # ---------------------------------------------------------------
    # Capacity
    # ---------------------------------------------------------------

    def set_active_work(
        self,
        worker_id: str,
        generation: int,
        active_work: int,
    ) -> bool:
        if active_work < 0:
            raise ValueError(
                "active_work cannot be negative"
            )

        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE fleet_workers
                SET
                    active_work = ?,
                    updated_at = ?
                WHERE
                    worker_id = ?
                    AND generation = ?
                    AND state IN (
                        'active',
                        'draining'
                    )
                    AND ? <= capacity
                """,
                (
                    active_work,
                    time.time(),
                    worker_id,
                    generation,
                    active_work,
                ),
            ).rowcount

            connection.commit()

        finally:
            connection.close()

        return updated == 1

    def capacity(self) -> FleetCapacity:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT
                    COALESCE(
                        SUM(capacity),
                        0
                    ),
                    COALESCE(
                        SUM(
                            CASE
                                WHEN state IN (
                                    'active',
                                    'draining'
                                )
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
                    ),
                    COUNT(
                        CASE
                            WHEN state = 'draining'
                            THEN 1
                        END
                    ),
                    COUNT(
                        CASE
                            WHEN state IN (
                                'expired',
                                'fenced'
                            )
                            THEN 1
                        END
                    ),
                    COALESCE(
                        SUM(active_work),
                        0
                    )
                FROM fleet_workers
                """
            ).fetchone()

        finally:
            connection.close()

        total = int(row[0])
        active_capacity = int(row[1])
        active_work = int(row[5])

        return FleetCapacity(
            total_capacity=total,
            active_capacity=active_capacity,
            active_workers=int(row[2]),
            draining_workers=int(row[3]),
            unhealthy_workers=int(row[4]),
            active_work=active_work,
            available_slots=max(
                0,
                active_capacity - active_work,
            ),
        )

    # ---------------------------------------------------------------
    # Capability-aware selection
    # ---------------------------------------------------------------

    def workers_for_capability(
        self,
        capability: str,
        limit: int = 100,
    ) -> list[FleetWorker]:
        if limit <= 0:
            return []

        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT
                    worker_id,
                    generation
                FROM fleet_capability_index
                WHERE
                    capability = ?
                    AND state = 'active'
                ORDER BY
                    capacity DESC,
                    worker_id ASC
                LIMIT ?
                """,
                (
                    capability,
                    limit,
                ),
            ).fetchall()

        finally:
            connection.close()

        result: list[FleetWorker] = []

        for row in rows:
            worker = self.get(
                str(row["worker_id"])
            )

            if (
                worker is not None
                and worker.generation
                == int(row["generation"])
            ):
                result.append(worker)

        return result

    def select_worker(
        self,
        capability: Optional[str] = None,
        region: Optional[str] = None,
        zone: Optional[str] = None,
    ) -> Optional[FleetWorker]:
        if capability:
            workers = self.workers_for_capability(
                capability,
                limit=1000,
            )
        else:
            workers = self.list_workers(
                state="active",
                limit=1000,
            )

        eligible = []

        for worker in workers:
            if worker.state != "active":
                continue

            if (
                worker.active_work
                >= worker.capacity
            ):
                continue

            if (
                region is not None
                and worker.region != region
            ):
                continue

            if (
                zone is not None
                and worker.zone != zone
            ):
                continue

            eligible.append(worker)

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
                self.stable_worker_score(
                    worker.worker_id
                ),
            ),
        )

    # ---------------------------------------------------------------
    # Assignments
    # ---------------------------------------------------------------

    def assign(
        self,
        worker_id: str,
        generation: int,
        partition_id: Optional[int] = None,
        workload_type: Optional[str] = None,
    ) -> Optional[WorkerAssignment]:
        worker = self.get(
            worker_id
        )

        if (
            worker is None
            or worker.generation != generation
            or worker.state != 'active'
            or worker.active_work
            >= worker.capacity
        ):
            return None

        now = time.time()

        connection = self._connect()

        try:
            fencing_epoch = (
                worker.fencing_epoch
            )

            connection.execute(
                """
                INSERT OR REPLACE INTO
                fleet_assignments (
                    worker_id,
                    generation,
                    partition_id,
                    workload_type,
                    fencing_epoch,
                    assigned_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    worker_id,
                    generation,
                    partition_id,
                    workload_type,
                    fencing_epoch,
                    now,
                ),
            )

            self._event(
                connection,
                worker_id,
                generation,
                "assigned",
                {
                    "partition_id":
                        partition_id,
                    "workload_type":
                        workload_type,
                    "fencing_epoch":
                        fencing_epoch,
                },
            )

            connection.commit()

        finally:
            connection.close()

        return WorkerAssignment(
            worker_id=worker_id,
            generation=generation,
            partition_id=partition_id,
            workload_type=workload_type,
            fencing_epoch=fencing_epoch,
            assigned_at=now,
        )

    def release_assignment(
        self,
        worker_id: str,
        generation: int,
    ) -> bool:
        connection = self._connect()

        try:
            updated = connection.execute(
                """
                DELETE FROM fleet_assignments
                WHERE
                    worker_id = ?
                    AND generation = ?
                """,
                (
                    worker_id,
                    generation,
                ),
            ).rowcount

            connection.commit()

        finally:
            connection.close()

        return updated == 1

    # ---------------------------------------------------------------
    # Listing
    # ---------------------------------------------------------------

    def list_workers(
        self,
        state: Optional[str] = None,
        region: Optional[str] = None,
        zone: Optional[str] = None,
        limit: int = 1000,
    ) -> list[FleetWorker]:
        if limit <= 0:
            return []

        connection = self._connect()

        try:
            query = """
                SELECT *
                FROM fleet_workers
                WHERE 1 = 1
            """

            params: list[Any] = []

            if state is not None:
                query += """
                    AND state = ?
                """
                params.append(state)

            if region is not None:
                query += """
                    AND region = ?
                """
                params.append(region)

            if zone is not None:
                query += """
                    AND zone = ?
                """
                params.append(zone)

            query += """
                ORDER BY
                    active_work ASC,
                    capacity DESC,
                    worker_id ASC
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
            self._worker_from_row(row)
            for row in rows
        ]

    # ---------------------------------------------------------------
    # Events
    # ---------------------------------------------------------------

    def _event(
        self,
        connection: sqlite3.Connection,
        worker_id: str,
        generation: int,
        event: str,
        metadata: Optional[
            dict[str, Any]
        ] = None,
    ) -> None:
        event_id = uuid.uuid4().hex

        connection.execute(
            """
            INSERT INTO fleet_events (
                event_id,
                worker_id,
                generation,
                event,
                timestamp,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                worker_id,
                generation,
                event,
                time.time(),
                json.dumps(
                    metadata or {},
                    sort_keys=True,
                    default=str,
                ),
            ),
        )

        self.stats_data[
            "events"
        ] += 1

    def recent_events(
        self,
        worker_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[FleetEvent]:
        if limit <= 0:
            return []

        connection = self._connect()

        try:
            if worker_id is None:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM fleet_events
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM fleet_events
                    WHERE worker_id = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (
                        worker_id,
                        limit,
                    ),
                ).fetchall()

        finally:
            connection.close()

        return [
            FleetEvent(
                event_id=str(
                    row["event_id"]
                ),
                worker_id=str(
                    row["worker_id"]
                ),
                generation=int(
                    row["generation"]
                ),
                event=str(
                    row["event"]
                ),
                timestamp=float(
                    row["timestamp"]
                ),
                metadata=json.loads(
                    row["metadata_json"]
                ),
            )
            for row in rows
        ]

    # ---------------------------------------------------------------
    # Fleet control loop
    # ---------------------------------------------------------------

    def tick(self) -> dict[str, Any]:
        expired = self.expire_unhealthy()

        draining = self.list_workers(
            state="draining",
            limit=10000,
        )

        drained = 0

        for worker in draining:
            if worker.active_work == 0:
                if self.complete_drain(
                    worker.worker_id,
                    worker.generation,
                ):
                    drained += 1

        return {
            "expired_workers": len(expired),
            "drained_workers": drained,
            "capacity": asdict(
                self.capacity()
            ),
        }

    def run(
        self,
        interval: float = 30.0,
    ) -> None:
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
                    self.tick()
                except Exception:
                    pass

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

    # ---------------------------------------------------------------
    # Statistics
    # ---------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        capacity = self.capacity()

        return {
            **self.stats_data,
            "workers_total": len(
                self.list_workers(
                    limit=1_000_000
                )
            ),
            "workers_active": len(
                self.list_workers(
                    state="active",
                    limit=1_000_000,
                )
            ),
            "workers_draining": len(
                self.list_workers(
                    state="draining",
                    limit=1_000_000,
                )
            ),
            "workers_expired": len(
                self.list_workers(
                    state="expired",
                    limit=1_000_000,
                )
            ),
            "workers_fenced": len(
                self.list_workers(
                    state="fenced",
                    limit=1_000_000,
                )
            ),
            "workers_removed": len(
                self.list_workers(
                    state="removed",
                    limit=1_000_000,
                )
            ),
            "capacity": asdict(
                capacity
            ),
            "running": self.running,
            "architecture": {
                "fixed_worker_limit": False,
                "fixed_global_execution_limit": False,
                "elastic_worker_fleet": True,
                "generation_fencing": True,
                "heartbeat_supervision": True,
                "capability_aware_selection": True,
                "region_zone_awareness": True,
                "draining": True,
                "enormous_scale_target": True,
                "billions_to_trillions_target": True,
            },
        }


__all__ = [
    "VERSION",
    "WorkerCapability",
    "FleetWorker",
    "WorkerAssignment",
    "FleetCapacity",
    "FleetEvent",
    "MassiveWorkerFleet",
]
