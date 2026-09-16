from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RecoveryLease:
    lease_id: str
    resource_id: str
    owner_id: str
    fencing_token: int
    acquired_at: float
    expires_at: float


@dataclass(frozen=True)
class RebalancePlan:
    plan_id: str
    resource_id: str
    old_owner: Optional[str]
    new_owner: str
    fencing_token: int
    created_at: float


@dataclass(frozen=True)
class FaultRecord:
    resource_id: str
    owner_id: Optional[str]
    fault_type: str
    error: str
    detected_at: float
    isolated_until: float


class GlobalRecoveryRebalancing:
    """
    Global recovery, rebalancing and fault-isolation control plane.

    Designed for enormous distributed Web discovery:

        worker failure
              ↓
        lease expiration
              ↓
        fencing
              ↓
        workload recovery
              ↓
        ownership rebalance
              ↓
        isolated faulty resource
              ↓
        healthy capacity resumes work

    The control plane is deliberately independent of crawler workers.
    """

    VERSION = "global-recovery-rebalancing.v1"

    def __init__(
        self,
        storage_path: str,
        lease_seconds: float = 300.0,
        fault_backoff_seconds: float = 60.0,
        max_fault_backoff_seconds: float = 3600.0,
    ):
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")

        if fault_backoff_seconds <= 0:
            raise ValueError(
                "fault_backoff_seconds must be > 0"
            )

        if max_fault_backoff_seconds < fault_backoff_seconds:
            raise ValueError(
                "max_fault_backoff_seconds must be >= fault_backoff_seconds"
            )

        self.storage_path = storage_path
        self.lease_seconds = float(lease_seconds)
        self.fault_backoff_seconds = float(
            fault_backoff_seconds
        )
        self.max_fault_backoff_seconds = float(
            max_fault_backoff_seconds
        )

        self._lock = threading.RLock()

        self._stats = {
            "lease_acquisitions": 0,
            "lease_renewals": 0,
            "lease_releases": 0,
            "expired_leases_recovered": 0,
            "fences": 0,
            "rebalance_plans": 0,
            "faults_recorded": 0,
            "resources_isolated": 0,
            "resources_released": 0,
        }

        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.storage_path,
            timeout=30,
        )

        connection.row_factory = sqlite3.Row

        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")

        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS recovery_resources (
                    resource_id TEXT PRIMARY KEY,
                    owner_id TEXT,
                    generation INTEGER NOT NULL DEFAULT 0,
                    fencing_token INTEGER NOT NULL DEFAULT 0,
                    state TEXT NOT NULL DEFAULT 'healthy',
                    fault_count INTEGER NOT NULL DEFAULT 0,
                    isolation_until REAL NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS recovery_leases (
                    lease_id TEXT PRIMARY KEY,
                    resource_id TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rebalance_plans (
                    plan_id TEXT PRIMARY KEY,
                    resource_id TEXT NOT NULL,
                    old_owner TEXT,
                    new_owner TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    applied_at REAL
                );

                CREATE TABLE IF NOT EXISTS fault_records (
                    resource_id TEXT NOT NULL,
                    owner_id TEXT,
                    fault_type TEXT NOT NULL,
                    error TEXT NOT NULL,
                    detected_at REAL NOT NULL,
                    isolated_until REAL NOT NULL,
                    PRIMARY KEY (
                        resource_id,
                        detected_at
                    )
                );

                CREATE INDEX IF NOT EXISTS idx_recovery_state
                ON recovery_resources(state);

                CREATE INDEX IF NOT EXISTS idx_recovery_isolation
                ON recovery_resources(isolation_until);

                CREATE INDEX IF NOT EXISTS idx_recovery_leases_expiry
                ON recovery_leases(expires_at);

                CREATE INDEX IF NOT EXISTS idx_rebalance_resource
                ON rebalance_plans(resource_id);

                CREATE INDEX IF NOT EXISTS idx_fault_resource
                ON fault_records(resource_id);
                """
            )

    @staticmethod
    def _stable_id(*parts: str) -> str:
        value = "\x00".join(parts).encode("utf-8")
        return hashlib.sha256(value).hexdigest()

    def register_resource(
        self,
        resource_id: str,
        owner_id: Optional[str] = None,
    ) -> bool:
        resource_id = str(resource_id).strip()

        if not resource_id:
            raise ValueError("resource_id is required")

        now = time.time()

        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO recovery_resources (
                    resource_id,
                    owner_id,
                    generation,
                    fencing_token,
                    state,
                    fault_count,
                    isolation_until,
                    updated_at
                )
                VALUES (?, ?, 0, 0, 'healthy', 0, 0, ?)
                """,
                (
                    resource_id,
                    owner_id,
                    now,
                ),
            )

            return cursor.rowcount > 0

    def acquire_lease(
        self,
        resource_id: str,
        owner_id: str,
    ) -> Optional[RecoveryLease]:
        resource_id = str(resource_id).strip()
        owner_id = str(owner_id).strip()

        if not resource_id or not owner_id:
            return None

        now = time.time()
        expires = now + self.lease_seconds

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")

            row = connection.execute(
                """
                SELECT *
                FROM recovery_resources
                WHERE resource_id = ?
                """,
                (resource_id,),
            ).fetchone()

            if row is None:
                connection.execute(
                    """
                    INSERT INTO recovery_resources (
                        resource_id,
                        owner_id,
                        generation,
                        fencing_token,
                        state,
                        fault_count,
                        isolation_until,
                        updated_at
                    )
                    VALUES (?, ?, 0, 0, 'healthy', 0, 0, ?)
                    """,
                    (
                        resource_id,
                        owner_id,
                        now,
                    ),
                )

                generation = 0
                fencing_token = 1

            else:
                if (
                    row["state"] == "isolated"
                    and row["isolation_until"] > now
                ):
                    connection.rollback()
                    return None

                generation = int(row["generation"]) + 1
                fencing_token = int(
                    row["fencing_token"]
                ) + 1

                connection.execute(
                    """
                    UPDATE recovery_resources
                    SET owner_id = ?,
                        generation = ?,
                        fencing_token = ?,
                        state = 'healthy',
                        updated_at = ?
                    WHERE resource_id = ?
                    """,
                    (
                        owner_id,
                        generation,
                        fencing_token,
                        now,
                        resource_id,
                    ),
                )

            lease_id = self._stable_id(
                resource_id,
                owner_id,
                str(fencing_token),
            )

            connection.execute(
                """
                INSERT OR REPLACE INTO recovery_leases (
                    lease_id,
                    resource_id,
                    owner_id,
                    fencing_token,
                    acquired_at,
                    expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    lease_id,
                    resource_id,
                    owner_id,
                    fencing_token,
                    now,
                    expires,
                ),
            )

            connection.commit()

        with self._lock:
            self._stats["lease_acquisitions"] += 1

        return RecoveryLease(
            lease_id=lease_id,
            resource_id=resource_id,
            owner_id=owner_id,
            fencing_token=fencing_token,
            acquired_at=now,
            expires_at=expires,
        )

    def renew_lease(
        self,
        lease_id: str,
        owner_id: str,
        fencing_token: int,
    ) -> bool:
        now = time.time()
        expires = now + self.lease_seconds

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE recovery_leases
                SET expires_at = ?
                WHERE lease_id = ?
                  AND owner_id = ?
                  AND fencing_token = ?
                  AND expires_at > ?
                """,
                (
                    expires,
                    lease_id,
                    owner_id,
                    int(fencing_token),
                    now,
                ),
            )

            success = cursor.rowcount > 0

        if success:
            with self._lock:
                self._stats["lease_renewals"] += 1

        return success

    def release_lease(
        self,
        lease_id: str,
        owner_id: str,
        fencing_token: int,
    ) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                DELETE FROM recovery_leases
                WHERE lease_id = ?
                  AND owner_id = ?
                  AND fencing_token = ?
                """,
                (
                    lease_id,
                    owner_id,
                    int(fencing_token),
                ),
            )

            success = cursor.rowcount > 0

        if success:
            with self._lock:
                self._stats["lease_releases"] += 1

        return success

    def recover_expired_leases(self) -> int:
        now = time.time()

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    lease_id,
                    resource_id,
                    owner_id,
                    fencing_token
                FROM recovery_leases
                WHERE expires_at <= ?
                """,
                (now,),
            ).fetchall()

            if not rows:
                return 0

            connection.execute(
                """
                DELETE FROM recovery_leases
                WHERE expires_at <= ?
                """,
                (now,),
            )

            for row in rows:
                connection.execute(
                    """
                    UPDATE recovery_resources
                    SET fencing_token =
                            MAX(
                                fencing_token,
                                ?
                            ) + 1,
                        state = 'recovering',
                        updated_at = ?
                    WHERE resource_id = ?
                    """,
                    (
                        int(row["fencing_token"]),
                        now,
                        row["resource_id"],
                    ),
                )

        recovered = len(rows)

        with self._lock:
            self._stats[
                "expired_leases_recovered"
            ] += recovered
            self._stats["fences"] += recovered

        return recovered

    def fence_resource(
        self,
        resource_id: str,
    ) -> Optional[int]:
        now = time.time()

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")

            row = connection.execute(
                """
                SELECT fencing_token
                FROM recovery_resources
                WHERE resource_id = ?
                """,
                (resource_id,),
            ).fetchone()

            if row is None:
                connection.rollback()
                return None

            token = int(row["fencing_token"]) + 1

            connection.execute(
                """
                UPDATE recovery_resources
                SET fencing_token = ?,
                    generation = generation + 1,
                    state = 'recovering',
                    updated_at = ?
                WHERE resource_id = ?
                """,
                (
                    token,
                    now,
                    resource_id,
                ),
            )

            connection.execute(
                """
                DELETE FROM recovery_leases
                WHERE resource_id = ?
                """,
                (resource_id,),
            )

            connection.commit()

        with self._lock:
            self._stats["fences"] += 1

        return token

    def create_rebalance_plan(
        self,
        resource_id: str,
        new_owner: str,
    ) -> Optional[RebalancePlan]:
        new_owner = str(new_owner).strip()

        if not new_owner:
            return None

        now = time.time()

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")

            row = connection.execute(
                """
                SELECT owner_id, fencing_token
                FROM recovery_resources
                WHERE resource_id = ?
                """,
                (resource_id,),
            ).fetchone()

            if row is None:
                connection.rollback()
                return None

            old_owner = row["owner_id"]

            token = int(
                row["fencing_token"]
            ) + 1

            plan_id = self._stable_id(
                resource_id,
                new_owner,
                str(token),
            )

            connection.execute(
                """
                INSERT OR REPLACE INTO rebalance_plans (
                    plan_id,
                    resource_id,
                    old_owner,
                    new_owner,
                    fencing_token,
                    created_at,
                    applied_at
                )
                VALUES (?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    plan_id,
                    resource_id,
                    old_owner,
                    new_owner,
                    token,
                    now,
                ),
            )

            connection.execute(
                """
                UPDATE recovery_resources
                SET owner_id = ?,
                    fencing_token = ?,
                    generation = generation + 1,
                    state = 'healthy',
                    updated_at = ?
                WHERE resource_id = ?
                """,
                (
                    new_owner,
                    token,
                    now,
                    resource_id,
                ),
            )

            connection.commit()

        plan = RebalancePlan(
            plan_id=plan_id,
            resource_id=resource_id,
            old_owner=old_owner,
            new_owner=new_owner,
            fencing_token=token,
            created_at=now,
        )

        with self._lock:
            self._stats["rebalance_plans"] += 1

        return plan

    def record_fault(
        self,
        resource_id: str,
        fault_type: str,
        error: str,
        owner_id: Optional[str] = None,
    ) -> FaultRecord:
        now = time.time()

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT fault_count
                FROM recovery_resources
                WHERE resource_id = ?
                """,
                (resource_id,),
            ).fetchone()

            previous_count = (
                int(row["fault_count"])
                if row is not None
                else 0
            )

            fault_count = previous_count + 1

            backoff = min(
                self.max_fault_backoff_seconds,
                self.fault_backoff_seconds
                * (2 ** min(fault_count - 1, 10)),
            )

            isolated_until = now + backoff

            connection.execute(
                """
                INSERT OR IGNORE INTO recovery_resources (
                    resource_id,
                    owner_id,
                    generation,
                    fencing_token,
                    state,
                    fault_count,
                    isolation_until,
                    updated_at
                )
                VALUES (?, ?, 0, 0, 'isolated', ?, ?, ?)
                """,
                (
                    resource_id,
                    owner_id,
                    fault_count,
                    isolated_until,
                    now,
                ),
            )

            connection.execute(
                """
                UPDATE recovery_resources
                SET owner_id = COALESCE(?, owner_id),
                    state = 'isolated',
                    fault_count = ?,
                    isolation_until = ?,
                    fencing_token =
                        fencing_token + 1,
                    updated_at = ?
                WHERE resource_id = ?
                """,
                (
                    owner_id,
                    fault_count,
                    isolated_until,
                    now,
                    resource_id,
                ),
            )

            connection.execute(
                """
                INSERT INTO fault_records (
                    resource_id,
                    owner_id,
                    fault_type,
                    error,
                    detected_at,
                    isolated_until
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    resource_id,
                    owner_id,
                    str(fault_type),
                    str(error),
                    now,
                    isolated_until,
                ),
            )

            connection.execute(
                """
                DELETE FROM recovery_leases
                WHERE resource_id = ?
                """,
                (resource_id,),
            )

        with self._lock:
            self._stats["faults_recorded"] += 1
            self._stats["resources_isolated"] += 1

        return FaultRecord(
            resource_id=resource_id,
            owner_id=owner_id,
            fault_type=str(fault_type),
            error=str(error),
            detected_at=now,
            isolated_until=isolated_until,
        )

    def release_expired_isolations(self) -> int:
        now = time.time()

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE recovery_resources
                SET state = 'healthy',
                    isolation_until = 0,
                    updated_at = ?
                WHERE state = 'isolated'
                  AND isolation_until <= ?
                """,
                (
                    now,
                    now,
                ),
            )

            released = cursor.rowcount

        if released:
            with self._lock:
                self._stats[
                    "resources_released"
                ] += released

        return released

    def recover_resource(
        self,
        resource_id: str,
    ) -> bool:
        now = time.time()

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE recovery_resources
                SET state = 'healthy',
                    isolation_until = 0,
                    generation = generation + 1,
                    fencing_token =
                        fencing_token + 1,
                    updated_at = ?
                WHERE resource_id = ?
                """,
                (
                    now,
                    resource_id,
                ),
            )

            return cursor.rowcount > 0

    def resource(
        self,
        resource_id: str,
    ) -> Optional[dict]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM recovery_resources
                WHERE resource_id = ?
                """,
                (resource_id,),
            ).fetchone()

            if row is None:
                return None

            return dict(row)

    def healthy_resources(
        self,
        limit: Optional[int] = None,
    ) -> list[dict]:
        sql = """
            SELECT *
            FROM recovery_resources
            WHERE state = 'healthy'
               OR (
                    state = 'isolated'
                    AND isolation_until <= ?
               )
            ORDER BY updated_at ASC
        """

        params = [time.time()]

        if limit is not None:
            if limit < 1:
                return []

            sql += " LIMIT ?"
            params.append(int(limit))

        with self._connect() as connection:
            rows = connection.execute(
                sql,
                params,
            ).fetchall()

            return [dict(row) for row in rows]

    def tick(self) -> dict[str, int]:
        recovered = self.recover_expired_leases()
        released = self.release_expired_isolations()

        return {
            "expired_leases_recovered": recovered,
            "isolations_released": released,
        }

    def stats(self) -> dict:
        with self._lock:
            result = dict(self._stats)

        with self._connect() as connection:
            resources = connection.execute(
                """
                SELECT state, COUNT(*) AS count
                FROM recovery_resources
                GROUP BY state
                """
            ).fetchall()

            active_leases = connection.execute(
                """
                SELECT COUNT(*)
                FROM recovery_leases
                """
            ).fetchone()[0]

            result["resources"] = {
                row["state"]: int(row["count"])
                for row in resources
            }

            result["active_leases"] = int(
                active_leases
            )

        result["version"] = self.VERSION

        return result
