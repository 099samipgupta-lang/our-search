from __future__ import annotations

"""
OUR SEARCH — End-to-End Failure / Recovery Consistency

Phase 7
Brick 7.4

Version:
    end-to-end-failure-recovery.v1

Purpose:
    Provide one durable consistency coordinator across the complete
    discovery → crawler → storage/index pipeline.

Design target:
    Billions of public-Web domains and an enormous URL universe.

Core principle:

    A work item is not considered safely completed merely because one
    subsystem reports success.

    The lifecycle is tracked explicitly so failures between subsystem
    boundaries can be recovered, replayed, fenced, or reconciled.

Lifecycle:

    DISCOVERED
        ↓
    QUEUED
        ↓
    ROUTED
        ↓
    LEASED
        ↓
    CRAWL_ACCEPTED
        ↓
    CRAWL_COMPLETED
        ↓
    STORAGE_ACCEPTED
        ↓
    INDEX_HANDOFF
        ↓
    COMPLETE

Any intermediate failure can return work to a durable recoverable state.

This module does not replace the queue, router, worker supervisor,
crawler, or storage/index implementation.
"""

import sqlite3
import threading
from dataclasses import dataclass
from time import time
from typing import Any, Mapping, Optional


CONSISTENCY_VERSION = "end-to-end-failure-recovery.v1"


DISCOVERED = "discovered"
QUEUED = "queued"
ROUTED = "routed"
LEASED = "leased"
CRAWL_ACCEPTED = "crawl_accepted"
CRAWL_COMPLETED = "crawl_completed"
STORAGE_ACCEPTED = "storage_accepted"
INDEX_HANDOFF = "index_handoff"
COMPLETE = "complete"
FAILED = "failed"
RECOVERABLE = "recoverable"
FENCED = "fenced"


@dataclass(frozen=True)
class RecoveryRecord:
    work_id: str
    hostname: str
    url: str
    source: str
    state: str
    worker_id: Optional[str]
    worker_generation: Optional[int]
    fencing_token: Optional[int]
    attempt: int
    updated_at: float
    last_error: Optional[str]


@dataclass(frozen=True)
class RecoveryAction:
    work_id: str
    action: str
    state: str
    reason: Optional[str] = None


class EndToEndFailureRecovery:
    """
    Durable lifecycle coordinator for discovery/crawler/index work.

    SQLite is used only as the durable local consistency journal.
    Global horizontal scaling remains owned by the surrounding
    distributed queue, routing, worker, recovery, and storage layers.
    """

    def __init__(
        self,
        storage_root: str = "crawler_storage",
        lease_timeout: float = 300.0,
        max_attempts: int = 8,
        recovery_batch_size: int = 10000,
    ):
        if lease_timeout <= 0:
            raise ValueError(
                "lease_timeout must be positive"
            )

        if max_attempts <= 0:
            raise ValueError(
                "max_attempts must be positive"
            )

        if recovery_batch_size <= 0:
            raise ValueError(
                "recovery_batch_size must be positive"
            )

        self.storage_root = storage_root
        self.lease_timeout = float(lease_timeout)
        self.max_attempts = int(max_attempts)
        self.recovery_batch_size = int(
            recovery_batch_size
        )

        self._lock = threading.RLock()

        self._db_path = (
            f"{storage_root}/"
            "end_to_end_failure_recovery.db"
        )

        self._stats = {
            "registered": 0,
            "transitions": 0,
            "failures": 0,
            "recoveries": 0,
            "fenced": 0,
            "completed": 0,
            "replays": 0,
            "reconciliations": 0,
        }

        self._initialize()

    # ------------------------------------------------------------------
    # DATABASE
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._db_path,
            timeout=30,
            isolation_level=None,
        )

        connection.row_factory = sqlite3.Row

        connection.execute(
            "PRAGMA journal_mode=WAL"
        )

        connection.execute(
            "PRAGMA synchronous=NORMAL"
        )

        return connection

    def _initialize(self) -> None:
        with self._lock:
            connection = self._connect()

            try:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS recovery_state (
                        work_id TEXT PRIMARY KEY,
                        hostname TEXT NOT NULL,
                        url TEXT NOT NULL,
                        source TEXT NOT NULL,
                        state TEXT NOT NULL,
                        worker_id TEXT,
                        worker_generation INTEGER,
                        fencing_token INTEGER,
                        attempt INTEGER NOT NULL DEFAULT 0,
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL,
                        lease_until REAL,
                        last_error TEXT,
                        crawl_accepted_at REAL,
                        crawl_completed_at REAL,
                        storage_accepted_at REAL,
                        index_handoff_at REAL,
                        completed_at REAL
                    );

                    CREATE INDEX IF NOT EXISTS
                    idx_recovery_state_state
                    ON recovery_state(state);

                    CREATE INDEX IF NOT EXISTS
                    idx_recovery_state_lease
                    ON recovery_state(lease_until);

                    CREATE INDEX IF NOT EXISTS
                    idx_recovery_state_hostname
                    ON recovery_state(hostname);

                    CREATE INDEX IF NOT EXISTS
                    idx_recovery_state_updated
                    ON recovery_state(updated_at);

                    CREATE TABLE IF NOT EXISTS
                    recovery_events (
                        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        work_id TEXT NOT NULL,
                        previous_state TEXT,
                        new_state TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        worker_id TEXT,
                        fencing_token INTEGER,
                        error TEXT,
                        created_at REAL NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS
                    idx_recovery_events_work
                    ON recovery_events(work_id);

                    CREATE INDEX IF NOT EXISTS
                    idx_recovery_events_created
                    ON recovery_events(created_at);

                    CREATE TABLE IF NOT EXISTS
                    recovery_checkpoints (
                        checkpoint_key TEXT PRIMARY KEY,
                        checkpoint_value TEXT NOT NULL,
                        updated_at REAL NOT NULL
                    );
                    """
                )
            finally:
                connection.close()

    # ------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------

    @staticmethod
    def _required(
        value: Any,
        name: str,
    ) -> str:
        result = str(value or "").strip()

        if not result:
            raise ValueError(
                f"{name} is required"
            )

        return result

    # ------------------------------------------------------------------
    # REGISTRATION
    # ------------------------------------------------------------------

    def register(
        self,
        work_id: str,
        hostname: str,
        url: str,
        source: str,
        attempt: int = 0,
    ) -> RecoveryRecord:
        work_id = self._required(
            work_id,
            "work_id",
        )

        hostname = self._required(
            hostname,
            "hostname",
        )

        url = self._required(
            url,
            "url",
        )

        source = self._required(
            source,
            "source",
        ).lower()

        if attempt < 0:
            raise ValueError(
                "attempt cannot be negative"
            )

        now = time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    """
                    INSERT INTO recovery_state (
                        work_id,
                        hostname,
                        url,
                        source,
                        state,
                        attempt,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(work_id)
                    DO UPDATE SET
                        hostname = excluded.hostname,
                        url = excluded.url,
                        source = excluded.source,
                        updated_at = excluded.updated_at
                    """,
                    (
                        work_id,
                        hostname,
                        url,
                        source,
                        DISCOVERED,
                        int(attempt),
                        now,
                        now,
                    ),
                )

                connection.execute(
                    """
                    INSERT INTO recovery_events (
                        work_id,
                        previous_state,
                        new_state,
                        event_type,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        work_id,
                        None,
                        DISCOVERED,
                        "register",
                        now,
                    ),
                )

                self._stats[
                    "registered"
                ] += 1

                return self.get(work_id)

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # STATE TRANSITIONS
    # ------------------------------------------------------------------

    _ALLOWED_TRANSITIONS = {
        DISCOVERED: {
            QUEUED,
            FAILED,
            RECOVERABLE,
        },
        QUEUED: {
            ROUTED,
            FAILED,
            RECOVERABLE,
        },
        ROUTED: {
            LEASED,
            QUEUED,
            FAILED,
            RECOVERABLE,
        },
        LEASED: {
            CRAWL_ACCEPTED,
            QUEUED,
            FAILED,
            RECOVERABLE,
            FENCED,
        },
        CRAWL_ACCEPTED: {
            CRAWL_COMPLETED,
            QUEUED,
            FAILED,
            RECOVERABLE,
            FENCED,
        },
        CRAWL_COMPLETED: {
            STORAGE_ACCEPTED,
            QUEUED,
            FAILED,
            RECOVERABLE,
        },
        STORAGE_ACCEPTED: {
            INDEX_HANDOFF,
            QUEUED,
            FAILED,
            RECOVERABLE,
        },
        INDEX_HANDOFF: {
            COMPLETE,
            QUEUED,
            FAILED,
            RECOVERABLE,
        },
        FAILED: {
            RECOVERABLE,
            QUEUED,
        },
        RECOVERABLE: {
            QUEUED,
            ROUTED,
            FAILED,
        },
        FENCED: {
            RECOVERABLE,
            QUEUED,
            FAILED,
        },
        COMPLETE: set(),
    }

    def transition(
        self,
        work_id: str,
        new_state: str,
        *,
        worker_id: Optional[str] = None,
        fencing_token: Optional[int] = None,
        error: Optional[str] = None,
        event_type: str = "transition",
    ) -> bool:
        work_id = self._required(
            work_id,
            "work_id",
        )

        new_state = self._required(
            new_state,
            "new_state",
        ).lower()

        now = time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                row = connection.execute(
                    """
                    SELECT
                        state,
                        worker_id,
                        fencing_token
                    FROM recovery_state
                    WHERE work_id = ?
                    """,
                    (work_id,),
                ).fetchone()

                if row is None:
                    connection.rollback()
                    return False

                previous_state = row["state"]

                if new_state == previous_state:
                    connection.commit()
                    return True

                allowed = self._ALLOWED_TRANSITIONS.get(
                    previous_state,
                    set(),
                )

                if new_state not in allowed:
                    connection.rollback()
                    return False

                current_worker = row["worker_id"]
                current_token = row[
                    "fencing_token"
                ]

                if (
                    current_token is not None
                    and fencing_token is not None
                    and int(fencing_token)
                    != int(current_token)
                ):
                    connection.rollback()
                    return False

                if (
                    current_worker is not None
                    and worker_id is not None
                    and str(worker_id)
                    != str(current_worker)
                ):
                    connection.rollback()
                    return False

                updates = [
                    "state = ?",
                    "updated_at = ?",
                ]

                parameters: list[Any] = [
                    new_state,
                    now,
                ]

                if worker_id is not None:
                    updates.append(
                        "worker_id = ?"
                    )
                    parameters.append(
                        str(worker_id)
                    )

                if fencing_token is not None:
                    updates.append(
                        "fencing_token = ?"
                    )
                    parameters.append(
                        int(fencing_token)
                    )

                if error is not None:
                    updates.append(
                        "last_error = ?"
                    )
                    parameters.append(
                        str(error)
                    )

                timestamp_columns = {
                    CRAWL_ACCEPTED:
                        "crawl_accepted_at",
                    CRAWL_COMPLETED:
                        "crawl_completed_at",
                    STORAGE_ACCEPTED:
                        "storage_accepted_at",
                    INDEX_HANDOFF:
                        "index_handoff_at",
                    COMPLETE:
                        "completed_at",
                }

                timestamp_column = (
                    timestamp_columns.get(
                        new_state
                    )
                )

                if timestamp_column:
                    updates.append(
                        f"{timestamp_column} = ?"
                    )
                    parameters.append(now)

                parameters.append(
                    work_id
                )

                connection.execute(
                    f"""
                    UPDATE recovery_state
                    SET {", ".join(updates)}
                    WHERE work_id = ?
                    """,
                    tuple(parameters),
                )

                connection.execute(
                    """
                    INSERT INTO recovery_events (
                        work_id,
                        previous_state,
                        new_state,
                        event_type,
                        worker_id,
                        fencing_token,
                        error,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        work_id,
                        previous_state,
                        new_state,
                        event_type,
                        worker_id,
                        fencing_token,
                        error,
                        now,
                    ),
                )

                connection.commit()

                self._stats[
                    "transitions"
                ] += 1

                if new_state == COMPLETE:
                    self._stats[
                        "completed"
                    ] += 1

                return True

            except Exception:
                connection.rollback()
                raise

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # LEASE
    # ------------------------------------------------------------------

    def lease(
        self,
        work_id: str,
        worker_id: str,
        worker_generation: int,
        fencing_token: int,
        attempt: Optional[int] = None,
    ) -> bool:
        work_id = self._required(
            work_id,
            "work_id",
        )

        worker_id = self._required(
            worker_id,
            "worker_id",
        )

        now = time()
        lease_until = (
            now + self.lease_timeout
        )

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                row = connection.execute(
                    """
                    SELECT state, attempt
                    FROM recovery_state
                    WHERE work_id = ?
                    """,
                    (work_id,),
                ).fetchone()

                if row is None:
                    connection.rollback()
                    return False

                if row["state"] not in {
                    ROUTED,
                    QUEUED,
                    RECOVERABLE,
                }:
                    connection.rollback()
                    return False

                next_attempt = (
                    int(attempt)
                    if attempt is not None
                    else int(row["attempt"]) + 1
                )

                connection.execute(
                    """
                    UPDATE recovery_state
                    SET
                        state = ?,
                        worker_id = ?,
                        worker_generation = ?,
                        fencing_token = ?,
                        attempt = ?,
                        lease_until = ?,
                        updated_at = ?,
                        last_error = NULL
                    WHERE work_id = ?
                    """,
                    (
                        LEASED,
                        worker_id,
                        int(worker_generation),
                        int(fencing_token),
                        next_attempt,
                        lease_until,
                        now,
                        work_id,
                    ),
                )

                connection.execute(
                    """
                    INSERT INTO recovery_events (
                        work_id,
                        previous_state,
                        new_state,
                        event_type,
                        worker_id,
                        fencing_token,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        work_id,
                        row["state"],
                        LEASED,
                        "lease",
                        worker_id,
                        int(fencing_token),
                        now,
                    ),
                )

                connection.commit()

                self._stats[
                    "transitions"
                ] += 1

                return True

            except Exception:
                connection.rollback()
                raise

            finally:
                connection.close()

    def renew_lease(
        self,
        work_id: str,
        worker_id: str,
        fencing_token: int,
    ) -> bool:
        now = time()
        lease_until = (
            now + self.lease_timeout
        )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE recovery_state
                    SET
                        lease_until = ?,
                        updated_at = ?
                    WHERE
                        work_id = ?
                        AND state = ?
                        AND worker_id = ?
                        AND fencing_token = ?
                    """,
                    (
                        lease_until,
                        now,
                        work_id,
                        LEASED,
                        worker_id,
                        int(fencing_token),
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # FAILURE
    # ------------------------------------------------------------------

    def fail(
        self,
        work_id: str,
        error: str,
        *,
        worker_id: Optional[str] = None,
        fencing_token: Optional[int] = None,
        retryable: bool = True,
    ) -> bool:
        with self._lock:
            record = self.get(work_id)

            if record is None:
                return False

            if (
                fencing_token is not None
                and record.fencing_token is not None
                and int(fencing_token)
                != int(record.fencing_token)
            ):
                return False

            if (
                worker_id is not None
                and record.worker_id is not None
                and str(worker_id)
                != str(record.worker_id)
            ):
                return False

            self._stats[
                "failures"
            ] += 1

            if (
                retryable
                and record.attempt < self.max_attempts
            ):
                return self.transition(
                    work_id,
                    RECOVERABLE,
                    worker_id=worker_id,
                    fencing_token=fencing_token,
                    error=error,
                    event_type="recoverable_failure",
                )

            return self.transition(
                work_id,
                FAILED,
                worker_id=worker_id,
                fencing_token=fencing_token,
                error=error,
                event_type="terminal_failure",
            )

    # ------------------------------------------------------------------
    # FENCING
    # ------------------------------------------------------------------

    def fence(
        self,
        work_id: str,
        reason: str,
    ) -> bool:
        record = self.get(work_id)

        if record is None:
            return False

        if record.state == COMPLETE:
            return False

        result = self.transition(
            work_id,
            FENCED,
            error=reason,
            event_type="fence",
        )

        if result:
            self._stats[
                "fenced"
            ] += 1

        return result

    # ------------------------------------------------------------------
    # RECOVERY
    # ------------------------------------------------------------------

    def recover_expired(
        self,
        limit: Optional[int] = None,
    ) -> list[RecoveryAction]:
        limit = (
            self.recovery_batch_size
            if limit is None
            else int(limit)
        )

        if limit <= 0:
            return []

        now = time()

        actions: list[
            RecoveryAction
        ] = []

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT
                        work_id,
                        state,
                        attempt
                    FROM recovery_state
                    WHERE
                        lease_until IS NOT NULL
                        AND lease_until <= ?
                        AND state IN (?, ?)
                    ORDER BY
                        lease_until ASC,
                        work_id ASC
                    LIMIT ?
                    """,
                    (
                        now,
                        LEASED,
                        CRAWL_ACCEPTED,
                        limit,
                    ),
                ).fetchall()

            finally:
                connection.close()

        for row in rows:
            work_id = row["work_id"]

            result = self.transition(
                work_id,
                RECOVERABLE,
                error="lease expired",
                event_type="lease_recovery",
            )

            if result:
                self._stats[
                    "recoveries"
                ] += 1

                actions.append(
                    RecoveryAction(
                        work_id=work_id,
                        action="requeue",
                        state=RECOVERABLE,
                        reason="lease expired",
                    )
                )

        return actions

    def recover_failed(
        self,
        limit: Optional[int] = None,
    ) -> list[RecoveryAction]:
        limit = (
            self.recovery_batch_size
            if limit is None
            else int(limit)
        )

        if limit <= 0:
            return []

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT
                        work_id,
                        attempt
                    FROM recovery_state
                    WHERE state = ?
                      AND attempt < ?
                    ORDER BY
                        updated_at ASC,
                        work_id ASC
                    LIMIT ?
                    """,
                    (
                        FAILED,
                        self.max_attempts,
                        limit,
                    ),
                ).fetchall()

            finally:
                connection.close()

        actions: list[
            RecoveryAction
        ] = []

        for row in rows:
            work_id = row["work_id"]

            if self.transition(
                work_id,
                RECOVERABLE,
                error="failure replay",
                event_type="failure_replay",
            ):
                self._stats[
                    "replays"
                ] += 1

                actions.append(
                    RecoveryAction(
                        work_id=work_id,
                        action="replay",
                        state=RECOVERABLE,
                        reason="failure replay",
                    )
                )

        return actions

    # ------------------------------------------------------------------
    # RECONCILIATION
    # ------------------------------------------------------------------

    def reconcile(
        self,
        work_id: str,
        *,
        crawler_completed: bool = False,
        storage_accepted: bool = False,
        index_handoff: bool = False,
        index_complete: bool = False,
    ) -> Optional[RecoveryAction]:
        """
        Reconcile externally observed subsystem state into the durable
        lifecycle journal.

        This operation is idempotent: observing an already-applied state
        does not regress completed work.
        """

        record = self.get(work_id)

        if record is None:
            return None

        self._stats[
            "reconciliations"
        ] += 1

        if record.state == COMPLETE:
            return RecoveryAction(
                work_id=work_id,
                action="none",
                state=COMPLETE,
                reason="already complete",
            )

        if index_complete:
            if record.state == INDEX_HANDOFF:
                self.transition(
                    work_id,
                    COMPLETE,
                    event_type="reconcile_complete",
                )

                return RecoveryAction(
                    work_id=work_id,
                    action="complete",
                    state=COMPLETE,
                    reason="index completion observed",
                )

        if index_handoff:
            if record.state == STORAGE_ACCEPTED:
                self.transition(
                    work_id,
                    INDEX_HANDOFF,
                    event_type="reconcile_index_handoff",
                )

                return RecoveryAction(
                    work_id=work_id,
                    action="advance",
                    state=INDEX_HANDOFF,
                    reason="index handoff observed",
                )

        if storage_accepted:
            if record.state == CRAWL_COMPLETED:
                self.transition(
                    work_id,
                    STORAGE_ACCEPTED,
                    event_type="reconcile_storage",
                )

                return RecoveryAction(
                    work_id=work_id,
                    action="advance",
                    state=STORAGE_ACCEPTED,
                    reason="storage acceptance observed",
                )

        if crawler_completed:
            if record.state == CRAWL_ACCEPTED:
                self.transition(
                    work_id,
                    CRAWL_COMPLETED,
                    event_type="reconcile_crawler",
                )

                return RecoveryAction(
                    work_id=work_id,
                    action="advance",
                    state=CRAWL_COMPLETED,
                    reason="crawler completion observed",
                )

        return RecoveryAction(
            work_id=work_id,
            action="none",
            state=record.state,
            reason="no safe advancement",
        )

    # ------------------------------------------------------------------
    # CHECKPOINTS
    # ------------------------------------------------------------------

    def set_checkpoint(
        self,
        key: str,
        value: str,
    ) -> None:
        now = time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    """
                    INSERT INTO recovery_checkpoints (
                        checkpoint_key,
                        checkpoint_value,
                        updated_at
                    )
                    VALUES (?, ?, ?)
                    ON CONFLICT(checkpoint_key)
                    DO UPDATE SET
                        checkpoint_value =
                            excluded.checkpoint_value,
                        updated_at =
                            excluded.updated_at
                    """,
                    (
                        str(key),
                        str(value),
                        now,
                    ),
                )
            finally:
                connection.close()

    def get_checkpoint(
        self,
        key: str,
    ) -> Optional[str]:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT checkpoint_value
                    FROM recovery_checkpoints
                    WHERE checkpoint_key = ?
                    """,
                    (str(key),),
                ).fetchone()

                if row is None:
                    return None

                return str(
                    row["checkpoint_value"]
                )

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # LOOKUP
    # ------------------------------------------------------------------

    def get(
        self,
        work_id: str,
    ) -> Optional[RecoveryRecord]:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT
                        work_id,
                        hostname,
                        url,
                        source,
                        state,
                        worker_id,
                        worker_generation,
                        fencing_token,
                        attempt,
                        updated_at,
                        last_error
                    FROM recovery_state
                    WHERE work_id = ?
                    """,
                    (work_id,),
                ).fetchone()

                if row is None:
                    return None

                return RecoveryRecord(
                    work_id=row["work_id"],
                    hostname=row["hostname"],
                    url=row["url"],
                    source=row["source"],
                    state=row["state"],
                    worker_id=row["worker_id"],
                    worker_generation=(
                        row["worker_generation"]
                    ),
                    fencing_token=(
                        row["fencing_token"]
                    ),
                    attempt=int(
                        row["attempt"]
                    ),
                    updated_at=float(
                        row["updated_at"]
                    ),
                    last_error=row[
                        "last_error"
                    ],
                )

            finally:
                connection.close()

    def list_state(
        self,
        state: Optional[str] = None,
        limit: int = 1000,
    ) -> list[RecoveryRecord]:
        if limit <= 0:
            return []

        with self._lock:
            connection = self._connect()

            try:
                if state is None:
                    rows = connection.execute(
                        """
                        SELECT
                            work_id,
                            hostname,
                            url,
                            source,
                            state,
                            worker_id,
                            worker_generation,
                            fencing_token,
                            attempt,
                            updated_at,
                            last_error
                        FROM recovery_state
                        ORDER BY
                            updated_at ASC,
                            work_id ASC
                        LIMIT ?
                        """,
                        (int(limit),),
                    ).fetchall()
                else:
                    rows = connection.execute(
                        """
                        SELECT
                            work_id,
                            hostname,
                            url,
                            source,
                            state,
                            worker_id,
                            worker_generation,
                            fencing_token,
                            attempt,
                            updated_at,
                            last_error
                        FROM recovery_state
                        WHERE state = ?
                        ORDER BY
                            updated_at ASC,
                            work_id ASC
                        LIMIT ?
                        """,
                        (
                            str(state),
                            int(limit),
                        ),
                    ).fetchall()

                return [
                    RecoveryRecord(
                        work_id=row["work_id"],
                        hostname=row["hostname"],
                        url=row["url"],
                        source=row["source"],
                        state=row["state"],
                        worker_id=row["worker_id"],
                        worker_generation=(
                            row["worker_generation"]
                        ),
                        fencing_token=(
                            row["fencing_token"]
                        ),
                        attempt=int(
                            row["attempt"]
                        ),
                        updated_at=float(
                            row["updated_at"]
                        ),
                        last_error=row[
                            "last_error"
                        ],
                    )
                    for row in rows
                ]

            finally:
                connection.close()

    # ------------------------------------------------------------------
    # STATS
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT
                        state,
                        COUNT(*) AS count
                    FROM recovery_state
                    GROUP BY state
                    """
                ).fetchall()

                state_counts = {
                    str(row["state"]): int(
                        row["count"]
                    )
                    for row in rows
                }

            finally:
                connection.close()

        return {
            **self._stats,
            "states": state_counts,
            "consistency_version": (
                CONSISTENCY_VERSION
            ),
        }


__all__ = [
    "CONSISTENCY_VERSION",
    "RecoveryRecord",
    "RecoveryAction",
    "EndToEndFailureRecovery",
    "DISCOVERED",
    "QUEUED",
    "ROUTED",
    "LEASED",
    "CRAWL_ACCEPTED",
    "CRAWL_COMPLETED",
    "STORAGE_ACCEPTED",
    "INDEX_HANDOFF",
    "COMPLETE",
    "FAILED",
    "RECOVERABLE",
    "FENCED",
]
