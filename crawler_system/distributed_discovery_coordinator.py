from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass(frozen=True)
class PartitionOwnership:
    logical_partition: int
    coordinator_id: str
    coordinator_epoch: int
    lease_id: str
    acquired_at: float
    lease_until: float
    state: str


@dataclass(frozen=True)
class CoordinatorRecord:
    coordinator_id: str
    epoch: int
    state: str
    registered_at: float
    last_heartbeat: float


@dataclass(frozen=True)
class PartitionAssignment:
    logical_partition: int
    coordinator_id: str
    coordinator_epoch: int
    assignment_epoch: int
    assigned_at: float
    state: str


@dataclass(frozen=True)
class CoordinatorCapacity:
    active_coordinators: int
    owned_partitions: int
    available_coordinators: int
    max_coordinators: int


class DistributedDiscoveryCoordinator:
    """
    Durable distributed discovery coordinator and logical-partition manager.

    Responsibilities:

        coordinator registration
        coordinator epochs
        coordinator fencing
        logical-partition ownership
        partition leases
        durable assignment epochs
        ownership recovery
        heartbeat/liveness
        deterministic partition placement
        load-aware ownership acquisition

    Logical partitions are the unit of coordination.

    The coordinator never needs to enumerate domains or URLs. A domain
    is mapped to a stable logical partition, and only the relevant
    partition state is coordinated.

    The SQLite backend is a durable local control-plane implementation.
    The API is deliberately designed around independently addressable
    partition records so the control plane can later be backed by a
    distributed metadata service without changing discovery semantics.
    """

    VERSION = "distributed-discovery-coordinator.v1"

    DEFAULT_MAX_COORDINATORS = 100_000
    DEFAULT_PARTITION_LEASE_SECONDS = 300.0
    DEFAULT_HEARTBEAT_TIMEOUT = 90.0

    def __init__(
        self,
        storage_path: str,
        max_coordinators: int = DEFAULT_MAX_COORDINATORS,
        partition_lease_seconds: float = (
            DEFAULT_PARTITION_LEASE_SECONDS
        ),
        heartbeat_timeout: float = (
            DEFAULT_HEARTBEAT_TIMEOUT
        ),
    ):
        if not storage_path:
            raise ValueError(
                "storage_path is required"
            )

        self.storage_path = storage_path

        self.max_coordinators = max(
            1,
            int(max_coordinators),
        )

        self.partition_lease_seconds = float(
            partition_lease_seconds
        )

        self.heartbeat_timeout = float(
            heartbeat_timeout
        )

        if self.partition_lease_seconds <= 0:
            raise ValueError(
                "partition_lease_seconds must be positive"
            )

        if self.heartbeat_timeout <= 0:
            raise ValueError(
                "heartbeat_timeout must be positive"
            )

        self._lock = threading.RLock()

        self._connect().close()

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
            CREATE TABLE IF NOT EXISTS coordinators (
                coordinator_id TEXT PRIMARY KEY,
                epoch INTEGER NOT NULL,
                state TEXT NOT NULL,
                registered_at REAL NOT NULL,
                last_heartbeat REAL NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_coordinators_state
            ON coordinators(
                state,
                last_heartbeat
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_assignments (
                logical_partition INTEGER PRIMARY KEY,
                coordinator_id TEXT,
                coordinator_epoch INTEGER NOT NULL,
                assignment_epoch INTEGER NOT NULL,
                assigned_at REAL NOT NULL,
                state TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_partition_assignments_coordinator
            ON partition_assignments(
                coordinator_id,
                state
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS partition_leases (
                logical_partition INTEGER PRIMARY KEY,
                lease_id TEXT NOT NULL,
                coordinator_id TEXT NOT NULL,
                coordinator_epoch INTEGER NOT NULL,
                acquired_at REAL NOT NULL,
                lease_until REAL NOT NULL,
                state TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_partition_leases_expiry
            ON partition_leases(
                state,
                lease_until
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_partition_leases_coordinator
            ON partition_leases(
                coordinator_id,
                state
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS coordinator_checkpoints (
                checkpoint_name TEXT PRIMARY KEY,
                checkpoint_value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )

    # ============================================================
    # Stable partition hashing
    # ============================================================

    @staticmethod
    def partition_hash(
        logical_partition: int,
    ) -> int:
        payload = str(
            int(logical_partition)
        ).encode("utf-8")

        digest = hashlib.sha256(
            payload
        ).digest()

        return int.from_bytes(
            digest[:8],
            "big",
        )

    @classmethod
    def coordinator_score(
        cls,
        logical_partition: int,
        coordinator_id: str,
    ) -> int:
        payload = (
            f"{int(logical_partition)}"
            "\x00"
            f"{coordinator_id}"
        ).encode("utf-8")

        digest = hashlib.sha256(
            payload
        ).digest()

        return int.from_bytes(
            digest[:8],
            "big",
        )

    # ============================================================
    # Coordinator registration
    # ============================================================

    def register_coordinator(
        self,
        coordinator_id: str,
    ) -> CoordinatorRecord:
        if not coordinator_id:
            raise ValueError(
                "coordinator_id is required"
            )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                row = connection.execute(
                    """
                    SELECT *
                    FROM coordinators
                    WHERE coordinator_id = ?
                    """,
                    (coordinator_id,),
                ).fetchone()

                if row is None:
                    active = connection.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM coordinators
                        WHERE state = 'active'
                        """
                    ).fetchone()

                    if (
                        int(active["count"])
                        >= self.max_coordinators
                    ):
                        raise RuntimeError(
                            "maximum coordinator capacity reached"
                        )

                    epoch = 1

                    connection.execute(
                        """
                        INSERT INTO coordinators (
                            coordinator_id,
                            epoch,
                            state,
                            registered_at,
                            last_heartbeat
                        )
                        VALUES (?, ?, 'active', ?, ?)
                        """,
                        (
                            coordinator_id,
                            epoch,
                            now,
                            now,
                        ),
                    )

                else:
                    epoch = (
                        int(row["epoch"])
                        + 1
                    )

                    connection.execute(
                        """
                        UPDATE coordinators
                        SET
                            epoch = ?,
                            state = 'active',
                            last_heartbeat = ?
                        WHERE coordinator_id = ?
                        """,
                        (
                            epoch,
                            now,
                            coordinator_id,
                        ),
                    )

                connection.execute(
                    "COMMIT"
                )

                result = connection.execute(
                    """
                    SELECT *
                    FROM coordinators
                    WHERE coordinator_id = ?
                    """,
                    (coordinator_id,),
                ).fetchone()

                return CoordinatorRecord(
                    coordinator_id=(
                        result["coordinator_id"]
                    ),
                    epoch=int(
                        result["epoch"]
                    ),
                    state=result["state"],
                    registered_at=float(
                        result["registered_at"]
                    ),
                    last_heartbeat=float(
                        result["last_heartbeat"]
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
    # Coordinator heartbeat
    # ============================================================

    def heartbeat(
        self,
        coordinator_id: str,
        epoch: int,
    ) -> bool:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE coordinators
                    SET last_heartbeat = ?
                    WHERE coordinator_id = ?
                      AND epoch = ?
                      AND state = 'active'
                    """,
                    (
                        now,
                        coordinator_id,
                        int(epoch),
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Coordinator fencing
    # ============================================================

    def fence_coordinator(
        self,
        coordinator_id: str,
        epoch: Optional[int] = None,
    ) -> bool:
        with self._lock:
            connection = self._connect()

            try:
                if epoch is None:
                    cursor = connection.execute(
                        """
                        UPDATE coordinators
                        SET
                            state = 'fenced',
                            epoch = epoch + 1
                        WHERE coordinator_id = ?
                          AND state = 'active'
                        """,
                        (coordinator_id,),
                    )
                else:
                    cursor = connection.execute(
                        """
                        UPDATE coordinators
                        SET
                            state = 'fenced',
                            epoch = epoch + 1
                        WHERE coordinator_id = ?
                          AND epoch = ?
                          AND state = 'active'
                        """,
                        (
                            coordinator_id,
                            int(epoch),
                        ),
                    )

                return cursor.rowcount == 1

            finally:
                connection.close()

    def fence_expired_coordinators(
        self,
    ) -> list[str]:
        cutoff = (
            time.time()
            - self.heartbeat_timeout
        )

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT coordinator_id
                    FROM coordinators
                    WHERE state = 'active'
                      AND last_heartbeat < ?
                    """,
                    (cutoff,),
                ).fetchall()

                ids = [
                    row["coordinator_id"]
                    for row in rows
                ]

                for coordinator_id in ids:
                    connection.execute(
                        """
                        UPDATE coordinators
                        SET
                            state = 'fenced',
                            epoch = epoch + 1
                        WHERE coordinator_id = ?
                          AND state = 'active'
                        """,
                        (coordinator_id,),
                    )

                return ids

            finally:
                connection.close()

    # ============================================================
    # Coordinator lookup
    # ============================================================

    def get_coordinator(
        self,
        coordinator_id: str,
    ) -> Optional[CoordinatorRecord]:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM coordinators
                    WHERE coordinator_id = ?
                    """,
                    (coordinator_id,),
                ).fetchone()

                if row is None:
                    return None

                return CoordinatorRecord(
                    coordinator_id=(
                        row["coordinator_id"]
                    ),
                    epoch=int(
                        row["epoch"]
                    ),
                    state=row["state"],
                    registered_at=float(
                        row["registered_at"]
                    ),
                    last_heartbeat=float(
                        row["last_heartbeat"]
                    ),
                )

            finally:
                connection.close()

    # ============================================================
    # Partition ownership
    # ============================================================

    def acquire_partition(
        self,
        logical_partition: int,
        coordinator_id: str,
        coordinator_epoch: int,
        lease_seconds: Optional[float] = None,
    ) -> Optional[PartitionOwnership]:
        now = time.time()

        if lease_seconds is None:
            lease_seconds = (
                self.partition_lease_seconds
            )

        lease_seconds = float(
            lease_seconds
        )

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive"
            )

        partition = int(
            logical_partition
        )

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                coordinator = connection.execute(
                    """
                    SELECT *
                    FROM coordinators
                    WHERE coordinator_id = ?
                      AND epoch = ?
                      AND state = 'active'
                    """,
                    (
                        coordinator_id,
                        int(coordinator_epoch),
                    ),
                ).fetchone()

                if coordinator is None:
                    connection.execute(
                        "ROLLBACK"
                    )
                    return None

                existing = connection.execute(
                    """
                    SELECT *
                    FROM partition_leases
                    WHERE logical_partition = ?
                    """,
                    (partition,),
                ).fetchone()

                if existing is not None:
                    if (
                        existing["state"]
                        == "active"
                        and float(
                            existing["lease_until"]
                        )
                        > now
                    ):
                        if (
                            existing[
                                "coordinator_id"
                            ]
                            != coordinator_id
                            or int(
                                existing[
                                    "coordinator_epoch"
                                ]
                            )
                            != int(
                                coordinator_epoch
                            )
                        ):
                            connection.execute(
                                "ROLLBACK"
                            )
                            return None

                        lease_id = existing[
                            "lease_id"
                        ]

                        lease_until = (
                            now + lease_seconds
                        )

                        connection.execute(
                            """
                            UPDATE partition_leases
                            SET
                                acquired_at = ?,
                                lease_until = ?
                            WHERE logical_partition = ?
                              AND lease_id = ?
                            """,
                            (
                                now,
                                lease_until,
                                partition,
                                lease_id,
                            ),
                        )

                        connection.execute(
                            """
                            UPDATE partition_assignments
                            SET
                                assigned_at = ?,
                                state = 'owned'
                            WHERE logical_partition = ?
                              AND coordinator_id = ?
                              AND coordinator_epoch = ?
                            """,
                            (
                                now,
                                partition,
                                coordinator_id,
                                int(
                                    coordinator_epoch
                                ),
                            ),
                        )

                        connection.execute(
                            "COMMIT"
                        )

                        return PartitionOwnership(
                            logical_partition=partition,
                            coordinator_id=(
                                coordinator_id
                            ),
                            coordinator_epoch=int(
                                coordinator_epoch
                            ),
                            lease_id=lease_id,
                            acquired_at=now,
                            lease_until=lease_until,
                            state="active",
                        )

                    connection.execute(
                        """
                        UPDATE partition_leases
                        SET
                            lease_id = ?,
                            coordinator_id = ?,
                            coordinator_epoch = ?,
                            acquired_at = ?,
                            lease_until = ?,
                            state = 'active'
                        WHERE logical_partition = ?
                        """,
                        (
                            uuid.uuid4().hex,
                            coordinator_id,
                            int(coordinator_epoch),
                            now,
                            now + lease_seconds,
                            partition,
                        ),
                    )

                    lease_row = connection.execute(
                        """
                        SELECT *
                        FROM partition_leases
                        WHERE logical_partition = ?
                        """,
                        (partition,),
                    ).fetchone()

                    assignment = connection.execute(
                        """
                        SELECT assignment_epoch
                        FROM partition_assignments
                        WHERE logical_partition = ?
                        """,
                        (partition,),
                    ).fetchone()

                    assignment_epoch = (
                        1
                        if assignment is None
                        else int(
                            assignment[
                                "assignment_epoch"
                            ]
                        )
                        + 1
                    )

                    connection.execute(
                        """
                        INSERT INTO partition_assignments (
                            logical_partition,
                            coordinator_id,
                            coordinator_epoch,
                            assignment_epoch,
                            assigned_at,
                            state
                        )
                        VALUES (?, ?, ?, ?, ?, 'owned')
                        ON CONFLICT(logical_partition)
                        DO UPDATE SET
                            coordinator_id =
                                excluded.coordinator_id,
                            coordinator_epoch =
                                excluded.coordinator_epoch,
                            assignment_epoch =
                                excluded.assignment_epoch,
                            assigned_at =
                                excluded.assigned_at,
                            state = 'owned'
                        """,
                        (
                            partition,
                            coordinator_id,
                            int(coordinator_epoch),
                            assignment_epoch,
                            now,
                        ),
                    )

                    connection.execute(
                        "COMMIT"
                    )

                    return PartitionOwnership(
                        logical_partition=partition,
                        coordinator_id=(
                            coordinator_id
                        ),
                        coordinator_epoch=int(
                            coordinator_epoch
                        ),
                        lease_id=lease_row[
                            "lease_id"
                        ],
                        acquired_at=now,
                        lease_until=float(
                            lease_row[
                                "lease_until"
                            ]
                        ),
                        state="active",
                    )

                lease_id = uuid.uuid4().hex
                lease_until = (
                    now + lease_seconds
                )

                connection.execute(
                    """
                    INSERT INTO partition_leases (
                        logical_partition,
                        lease_id,
                        coordinator_id,
                        coordinator_epoch,
                        acquired_at,
                        lease_until,
                        state
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'active')
                    """,
                    (
                        partition,
                        lease_id,
                        coordinator_id,
                        int(coordinator_epoch),
                        now,
                        lease_until,
                    ),
                )

                connection.execute(
                    """
                    INSERT INTO partition_assignments (
                        logical_partition,
                        coordinator_id,
                        coordinator_epoch,
                        assignment_epoch,
                        assigned_at,
                        state
                    )
                    VALUES (?, ?, ?, 1, ?, 'owned')
                    """,
                    (
                        partition,
                        coordinator_id,
                        int(coordinator_epoch),
                        now,
                    ),
                )

                connection.execute(
                    "COMMIT"
                )

                return PartitionOwnership(
                    logical_partition=partition,
                    coordinator_id=(
                        coordinator_id
                    ),
                    coordinator_epoch=int(
                        coordinator_epoch
                    ),
                    lease_id=lease_id,
                    acquired_at=now,
                    lease_until=lease_until,
                    state="active",
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
    # Partition lease renewal
    # ============================================================

    def renew_partition(
        self,
        ownership: PartitionOwnership,
        lease_seconds: Optional[float] = None,
    ) -> bool:
        now = time.time()

        if lease_seconds is None:
            lease_seconds = (
                self.partition_lease_seconds
            )

        lease_until = (
            now + float(lease_seconds)
        )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE partition_leases
                    SET
                        acquired_at = ?,
                        lease_until = ?
                    WHERE logical_partition = ?
                      AND lease_id = ?
                      AND coordinator_id = ?
                      AND coordinator_epoch = ?
                      AND state = 'active'
                      AND lease_until > ?
                    """,
                    (
                        now,
                        lease_until,
                        int(
                            ownership.logical_partition
                        ),
                        ownership.lease_id,
                        ownership.coordinator_id,
                        int(
                            ownership.coordinator_epoch
                        ),
                        now,
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Partition release
    # ============================================================

    def release_partition(
        self,
        ownership: PartitionOwnership,
    ) -> bool:
        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                cursor = connection.execute(
                    """
                    UPDATE partition_leases
                    SET state = 'released'
                    WHERE logical_partition = ?
                      AND lease_id = ?
                      AND coordinator_id = ?
                      AND coordinator_epoch = ?
                      AND state = 'active'
                    """,
                    (
                        int(
                            ownership.logical_partition
                        ),
                        ownership.lease_id,
                        ownership.coordinator_id,
                        int(
                            ownership.coordinator_epoch
                        ),
                    ),
                )

                if cursor.rowcount != 1:
                    connection.execute(
                        "ROLLBACK"
                    )
                    return False

                connection.execute(
                    """
                    UPDATE partition_assignments
                    SET state = 'released'
                    WHERE logical_partition = ?
                      AND coordinator_id = ?
                      AND coordinator_epoch = ?
                    """,
                    (
                        int(
                            ownership.logical_partition
                        ),
                        ownership.coordinator_id,
                        int(
                            ownership.coordinator_epoch
                        ),
                    ),
                )

                connection.execute(
                    "COMMIT"
                )

                return True

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
    # Expired partition recovery
    # ============================================================

    def recover_expired_partitions(
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
                    FROM partition_leases
                    WHERE state = 'active'
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
                        UPDATE partition_leases
                        SET state = 'expired'
                        WHERE logical_partition = ?
                          AND lease_id = ?
                          AND state = 'active'
                          AND lease_until <= ?
                        """,
                        (
                            int(
                                row[
                                    "logical_partition"
                                ]
                            ),
                            row["lease_id"],
                            now,
                        ),
                    )

                    if cursor.rowcount != 1:
                        continue

                    connection.execute(
                        """
                        UPDATE partition_assignments
                        SET state = 'available'
                        WHERE logical_partition = ?
                          AND coordinator_id = ?
                          AND coordinator_epoch = ?
                        """,
                        (
                            int(
                                row[
                                    "logical_partition"
                                ]
                            ),
                            row["coordinator_id"],
                            int(
                                row[
                                    "coordinator_epoch"
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
    # Ownership lookup
    # ============================================================

    def get_partition_owner(
        self,
        logical_partition: int,
    ) -> Optional[PartitionOwnership]:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM partition_leases
                    WHERE logical_partition = ?
                      AND state = 'active'
                      AND lease_until > ?
                    """,
                    (
                        int(logical_partition),
                        time.time(),
                    ),
                ).fetchone()

                if row is None:
                    return None

                return PartitionOwnership(
                    logical_partition=int(
                        row[
                            "logical_partition"
                        ]
                    ),
                    coordinator_id=(
                        row["coordinator_id"]
                    ),
                    coordinator_epoch=int(
                        row[
                            "coordinator_epoch"
                        ]
                    ),
                    lease_id=row["lease_id"],
                    acquired_at=float(
                        row["acquired_at"]
                    ),
                    lease_until=float(
                        row["lease_until"]
                    ),
                    state=row["state"],
                )

            finally:
                connection.close()

    # ============================================================
    # Deterministic coordinator selection
    # ============================================================

    def select_coordinator(
        self,
        logical_partition: int,
    ) -> Optional[CoordinatorRecord]:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM coordinators
                    WHERE state = 'active'
                      AND last_heartbeat >= ?
                    """,
                    (
                        now
                        - self.heartbeat_timeout,
                    ),
                ).fetchall()

                if not rows:
                    return None

                ranked = sorted(
                    rows,
                    key=lambda row: (
                        self.coordinator_score(
                            logical_partition,
                            row[
                                "coordinator_id"
                            ],
                        ),
                        row[
                            "coordinator_id"
                        ],
                    ),
                    reverse=True,
                )

                row = ranked[0]

                return CoordinatorRecord(
                    coordinator_id=(
                        row["coordinator_id"]
                    ),
                    epoch=int(
                        row["epoch"]
                    ),
                    state=row["state"],
                    registered_at=float(
                        row["registered_at"]
                    ),
                    last_heartbeat=float(
                        row["last_heartbeat"]
                    ),
                )

            finally:
                connection.close()

    # ============================================================
    # Automatic ownership acquisition
    # ============================================================

    def acquire_for_partition(
        self,
        logical_partition: int,
        lease_seconds: Optional[float] = None,
    ) -> Optional[PartitionOwnership]:
        coordinator = self.select_coordinator(
            logical_partition
        )

        if coordinator is None:
            return None

        return self.acquire_partition(
            logical_partition=logical_partition,
            coordinator_id=(
                coordinator.coordinator_id
            ),
            coordinator_epoch=(
                coordinator.epoch
            ),
            lease_seconds=lease_seconds,
        )

    # ============================================================
    # Bulk ownership acquisition
    # ============================================================

    def acquire_many(
        self,
        logical_partitions: Iterable[int],
        coordinator_id: str,
        coordinator_epoch: int,
        limit: Optional[int] = None,
    ) -> list[PartitionOwnership]:
        partitions = [
            int(partition)
            for partition in logical_partitions
        ]

        if limit is not None:
            partitions = partitions[
                : max(0, int(limit))
            ]

        acquired: list[
            PartitionOwnership
        ] = []

        for partition in partitions:
            ownership = (
                self.acquire_partition(
                    partition,
                    coordinator_id,
                    coordinator_epoch,
                )
            )

            if ownership is not None:
                acquired.append(
                    ownership
                )

        return acquired

    # ============================================================
    # Coordinator-owned partitions
    # ============================================================

    def coordinator_partitions(
        self,
        coordinator_id: str,
        include_expired: bool = False,
    ) -> list[int]:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                if include_expired:
                    rows = connection.execute(
                        """
                        SELECT logical_partition
                        FROM partition_leases
                        WHERE coordinator_id = ?
                        ORDER BY logical_partition
                        """,
                        (coordinator_id,),
                    ).fetchall()
                else:
                    rows = connection.execute(
                        """
                        SELECT logical_partition
                        FROM partition_leases
                        WHERE coordinator_id = ?
                          AND state = 'active'
                          AND lease_until > ?
                        ORDER BY logical_partition
                        """,
                        (
                            coordinator_id,
                            now,
                        ),
                    ).fetchall()

                return [
                    int(
                        row[
                            "logical_partition"
                        ]
                    )
                    for row in rows
                ]

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

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT INTO coordinator_checkpoints (
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
                        time.time(),
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
                    FROM coordinator_checkpoints
                    WHERE checkpoint_name = ?
                    """,
                    (name,),
                ).fetchone()

                if row is None:
                    return None

                return str(
                    row[
                        "checkpoint_value"
                    ]
                )

            finally:
                connection.close()

    # ============================================================
    # Capacity
    # ============================================================

    def capacity(
        self,
    ) -> CoordinatorCapacity:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                coordinators = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM coordinators
                    WHERE state = 'active'
                      AND last_heartbeat >= ?
                    """,
                    (
                        now
                        - self.heartbeat_timeout,
                    ),
                ).fetchone()

                partitions = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM partition_leases
                    WHERE state = 'active'
                      AND lease_until > ?
                    """,
                    (now,),
                ).fetchone()

                active = int(
                    coordinators["count"]
                )

                owned = int(
                    partitions["count"]
                )

                return CoordinatorCapacity(
                    active_coordinators=active,
                    owned_partitions=owned,
                    available_coordinators=max(
                        0,
                        self.max_coordinators
                        - active,
                    ),
                    max_coordinators=(
                        self.max_coordinators
                    ),
                )

            finally:
                connection.close()

    # ============================================================
    # Statistics
    # ============================================================

    def stats(
        self,
    ) -> dict[str, int | float | str]:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                coordinator_states = (
                    connection.execute(
                        """
                        SELECT
                            state,
                            COUNT(*) AS count
                        FROM coordinators
                        GROUP BY state
                        """
                    ).fetchall()
                )

                partition_states = (
                    connection.execute(
                        """
                        SELECT
                            state,
                            COUNT(*) AS count
                        FROM partition_assignments
                        GROUP BY state
                        """
                    ).fetchall()
                )

                active_leases = (
                    connection.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM partition_leases
                        WHERE state = 'active'
                          AND lease_until > ?
                        """,
                        (now,),
                    ).fetchone()
                )

                return {
                    "version": self.VERSION,
                    "active_coordinators": sum(
                        int(row["count"])
                        for row in coordinator_states
                        if row["state"]
                        == "active"
                    ),
                    "fenced_coordinators": sum(
                        int(row["count"])
                        for row in coordinator_states
                        if row["state"]
                        == "fenced"
                    ),
                    "owned_partitions": int(
                        active_leases["count"]
                    ),
                    "assignment_records": sum(
                        int(row["count"])
                        for row in partition_states
                    ),
                    "coordinator_records": sum(
                        int(row["count"])
                        for row in coordinator_states
                    ),
                    "max_coordinators": (
                        self.max_coordinators
                    ),
                }

            finally:
                connection.close()
