from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional


@dataclass(frozen=True)
class SourceLease:
    lease_id: str
    source: str
    worker_id: str
    generation: int
    fencing_epoch: int
    acquired_at: float
    lease_until: float


@dataclass(frozen=True)
class SourceState:
    source: str
    enabled: bool
    priority: float
    next_run_at: float
    last_run_at: float
    success_count: int
    failure_count: int
    consecutive_failures: int
    backoff_until: float
    scheduling_epoch: int


@dataclass(frozen=True)
class SourceCheckpoint:
    source: str
    checkpoint_name: str
    checkpoint_value: str
    updated_at: float


@dataclass(frozen=True)
class FederatedDiscoveryResult:
    source: str
    run_id: str
    candidates_seen: int
    candidates_accepted: int
    started_at: float
    completed_at: float
    success: bool


class GlobalSourceFederation:
    """
    Global public-Web discovery source federation.

    This layer coordinates independent discovery mechanisms such as:

        certificate transparency
        public registries
        sitemaps
        feeds
        link discovery
        public datasets
        future discovery sources

    It does not own the global URL corpus.

    It owns source scheduling metadata, source health, leases,
    checkpoints, backoff and fairness state.

    Source execution remains independent from the federation so one
    source cannot become a global failure or coordination bottleneck.
    """

    VERSION = "global-source-federation.v1"

    DEFAULT_LEASE_SECONDS = 300.0
    DEFAULT_FAILURE_BACKOFF = 30.0
    DEFAULT_MAX_BACKOFF = 3600.0

    def __init__(
        self,
        storage_path: str,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        failure_backoff: float = DEFAULT_FAILURE_BACKOFF,
        max_backoff: float = DEFAULT_MAX_BACKOFF,
    ):
        if not storage_path:
            raise ValueError(
                "storage_path is required"
            )

        self.storage_path = storage_path
        self.lease_seconds = float(
            lease_seconds
        )
        self.failure_backoff = max(
            0.0,
            float(failure_backoff),
        )
        self.max_backoff = max(
            self.failure_backoff,
            float(max_backoff),
        )

        if self.lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive"
            )

        self._lock = threading.RLock()

        self._sources: dict[
            str,
            Callable[..., Any],
        ] = {}

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
            CREATE TABLE IF NOT EXISTS sources (
                source TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1,
                priority REAL NOT NULL DEFAULT 50.0,
                next_run_at REAL NOT NULL DEFAULT 0,
                last_run_at REAL NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                backoff_until REAL NOT NULL DEFAULT 0,
                scheduling_epoch INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_sources_ready
            ON sources(
                enabled,
                next_run_at,
                backoff_until,
                priority DESC,
                last_run_at
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS source_leases (
                lease_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                fencing_epoch INTEGER NOT NULL,
                acquired_at REAL NOT NULL,
                lease_until REAL NOT NULL,
                state TEXT NOT NULL DEFAULT 'active'
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_source_leases_source
            ON source_leases(
                source,
                state
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_source_leases_expiry
            ON source_leases(
                state,
                lease_until
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS source_checkpoints (
                source TEXT NOT NULL,
                checkpoint_name TEXT NOT NULL,
                checkpoint_value TEXT NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (
                    source,
                    checkpoint_name
                )
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS source_runs (
                run_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                started_at REAL NOT NULL,
                completed_at REAL,
                candidates_seen INTEGER NOT NULL DEFAULT 0,
                candidates_accepted INTEGER NOT NULL DEFAULT 0,
                success INTEGER,
                error TEXT
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_source_runs_source
            ON source_runs(
                source,
                started_at DESC
            )
            """
        )

    # ============================================================
    # Stable source identity
    # ============================================================

    @staticmethod
    def source_hash(
        source: str,
    ) -> int:
        digest = hashlib.sha256(
            source.strip().lower().encode(
                "utf-8"
            )
        ).digest()

        return int.from_bytes(
            digest[:8],
            "big",
        )

    # ============================================================
    # Source registration
    # ============================================================

    def register_source(
        self,
        source: str,
        handler: Optional[
            Callable[..., Any]
        ] = None,
        priority: float = 50.0,
    ) -> SourceState:
        source = source.strip()

        if not source:
            raise ValueError(
                "source is required"
            )

        if handler is not None:
            with self._lock:
                self._sources[source] = handler

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    """
                    INSERT INTO sources (
                        source,
                        enabled,
                        priority,
                        next_run_at
                    )
                    VALUES (?, 1, ?, ?)
                    ON CONFLICT(source)
                    DO UPDATE SET
                        priority = excluded.priority,
                        enabled = 1
                    """,
                    (
                        source,
                        float(priority),
                        now,
                    ),
                )

                return self._get_source_locked(
                    connection,
                    source,
                )

            finally:
                connection.close()

    def unregister_source(
        self,
        source: str,
    ) -> bool:
        with self._lock:
            self._sources.pop(
                source,
                None,
            )

            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE sources
                    SET
                        enabled = 0,
                        scheduling_epoch =
                            scheduling_epoch + 1
                    WHERE source = ?
                    """,
                    (source,),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Source lookup
    # ============================================================

    @staticmethod
    def _state_from_row(
        row: sqlite3.Row,
    ) -> SourceState:
        return SourceState(
            source=row["source"],
            enabled=bool(
                row["enabled"]
            ),
            priority=float(
                row["priority"]
            ),
            next_run_at=float(
                row["next_run_at"]
            ),
            last_run_at=float(
                row["last_run_at"]
            ),
            success_count=int(
                row["success_count"]
            ),
            failure_count=int(
                row["failure_count"]
            ),
            consecutive_failures=int(
                row["consecutive_failures"]
            ),
            backoff_until=float(
                row["backoff_until"]
            ),
            scheduling_epoch=int(
                row["scheduling_epoch"]
            ),
        )

    def _get_source_locked(
        self,
        connection: sqlite3.Connection,
        source: str,
    ) -> SourceState:
        row = connection.execute(
            """
            SELECT *
            FROM sources
            WHERE source = ?
            """,
            (source,),
        ).fetchone()

        if row is None:
            raise KeyError(
                source
            )

        return self._state_from_row(
            row
        )

    def get_source(
        self,
        source: str,
    ) -> Optional[SourceState]:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM sources
                    WHERE source = ?
                    """,
                    (source,),
                ).fetchone()

                if row is None:
                    return None

                return self._state_from_row(
                    row
                )

            finally:
                connection.close()

    # ============================================================
    # Source scheduling
    # ============================================================

    def schedule_source(
        self,
        source: str,
        next_run_at: float,
        priority: Optional[float] = None,
    ) -> bool:
        with self._lock:
            connection = self._connect()

            try:
                if priority is None:
                    cursor = connection.execute(
                        """
                        UPDATE sources
                        SET
                            next_run_at = ?,
                            scheduling_epoch =
                                scheduling_epoch + 1
                        WHERE source = ?
                          AND enabled = 1
                        """,
                        (
                            float(next_run_at),
                            source,
                        ),
                    )
                else:
                    cursor = connection.execute(
                        """
                        UPDATE sources
                        SET
                            next_run_at = ?,
                            priority = ?,
                            scheduling_epoch =
                                scheduling_epoch + 1
                        WHERE source = ?
                          AND enabled = 1
                        """,
                        (
                            float(next_run_at),
                            float(priority),
                            source,
                        ),
                    )

                return cursor.rowcount == 1

            finally:
                connection.close()

    def ready_sources(
        self,
        limit: int = 1000,
    ) -> list[SourceState]:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM sources
                    WHERE enabled = 1
                      AND next_run_at <= ?
                      AND backoff_until <= ?
                    ORDER BY
                        priority DESC,
                        next_run_at ASC,
                        last_run_at ASC,
                        scheduling_epoch ASC,
                        source ASC
                    LIMIT ?
                    """,
                    (
                        now,
                        now,
                        max(1, int(limit)),
                    ),
                ).fetchall()

                return [
                    self._state_from_row(row)
                    for row in rows
                ]

            finally:
                connection.close()

    # ============================================================
    # Source leases
    # ============================================================

    def acquire_source(
        self,
        source: str,
        worker_id: str,
        generation: int,
        fencing_epoch: int,
        lease_seconds: Optional[float] = None,
    ) -> Optional[SourceLease]:
        now = time.time()

        if lease_seconds is None:
            lease_seconds = (
                self.lease_seconds
            )

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                row = connection.execute(
                    """
                    SELECT *
                    FROM sources
                    WHERE source = ?
                      AND enabled = 1
                      AND next_run_at <= ?
                      AND backoff_until <= ?
                    """,
                    (
                        source,
                        now,
                        now,
                    ),
                ).fetchone()

                if row is None:
                    connection.execute(
                        "ROLLBACK"
                    )
                    return None

                existing = connection.execute(
                    """
                    SELECT *
                    FROM source_leases
                    WHERE source = ?
                      AND state = 'active'
                      AND lease_until > ?
                    """,
                    (
                        source,
                        now,
                    ),
                ).fetchone()

                if existing is not None:
                    connection.execute(
                        "ROLLBACK"
                    )
                    return None

                lease_id = uuid.uuid4().hex
                lease_until = (
                    now
                    + float(lease_seconds)
                )

                connection.execute(
                    """
                    INSERT INTO source_leases (
                        lease_id,
                        source,
                        worker_id,
                        generation,
                        fencing_epoch,
                        acquired_at,
                        lease_until,
                        state
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'active')
                    """,
                    (
                        lease_id,
                        source,
                        worker_id,
                        int(generation),
                        int(fencing_epoch),
                        now,
                        lease_until,
                    ),
                )

                connection.execute(
                    """
                    UPDATE sources
                    SET
                        last_run_at = ?,
                        scheduling_epoch =
                            scheduling_epoch + 1
                    WHERE source = ?
                    """,
                    (
                        now,
                        source,
                    ),
                )

                connection.execute(
                    "COMMIT"
                )

                return SourceLease(
                    lease_id=lease_id,
                    source=source,
                    worker_id=worker_id,
                    generation=int(
                        generation
                    ),
                    fencing_epoch=int(
                        fencing_epoch
                    ),
                    acquired_at=now,
                    lease_until=lease_until,
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

    def renew_source(
        self,
        lease: SourceLease,
        lease_seconds: Optional[float] = None,
    ) -> bool:
        now = time.time()

        if lease_seconds is None:
            lease_seconds = (
                self.lease_seconds
            )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE source_leases
                    SET
                        lease_until = ?
                    WHERE lease_id = ?
                      AND source = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND state = 'active'
                      AND lease_until > ?
                    """,
                    (
                        now
                        + float(lease_seconds),
                        lease.lease_id,
                        lease.source,
                        lease.worker_id,
                        int(
                            lease.generation
                        ),
                        int(
                            lease.fencing_epoch
                        ),
                        now,
                    ),
                )

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ============================================================
    # Source run lifecycle
    # ============================================================

    def begin_run(
        self,
        lease: SourceLease,
    ) -> str:
        run_id = uuid.uuid4().hex
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    """
                    INSERT INTO source_runs (
                        run_id,
                        source,
                        started_at
                    )
                    VALUES (?, ?, ?)
                    """,
                    (
                        run_id,
                        lease.source,
                        now,
                    ),
                )

                return run_id

            finally:
                connection.close()

    def finish_run(
        self,
        lease: SourceLease,
        run_id: str,
        candidates_seen: int,
        candidates_accepted: int,
        success: bool,
        error: Optional[str] = None,
        next_run_at: Optional[float] = None,
    ) -> bool:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                lease_row = connection.execute(
                    """
                    SELECT *
                    FROM source_leases
                    WHERE lease_id = ?
                      AND source = ?
                      AND worker_id = ?
                      AND generation = ?
                      AND fencing_epoch = ?
                      AND state = 'active'
                    """,
                    (
                        lease.lease_id,
                        lease.source,
                        lease.worker_id,
                        int(
                            lease.generation
                        ),
                        int(
                            lease.fencing_epoch
                        ),
                    ),
                ).fetchone()

                if lease_row is None:
                    connection.execute(
                        "ROLLBACK"
                    )
                    return False

                connection.execute(
                    """
                    UPDATE source_runs
                    SET
                        completed_at = ?,
                        candidates_seen = ?,
                        candidates_accepted = ?,
                        success = ?,
                        error = ?
                    WHERE run_id = ?
                      AND source = ?
                    """,
                    (
                        now,
                        max(
                            0,
                            int(
                                candidates_seen
                            ),
                        ),
                        max(
                            0,
                            int(
                                candidates_accepted
                            ),
                        ),
                        1 if success else 0,
                        error,
                        run_id,
                        lease.source,
                    ),
                )

                if success:
                    backoff_until = 0.0
                    consecutive_failures = 0
                    next_value = (
                        now
                        if next_run_at is None
                        else float(
                            next_run_at
                        )
                    )

                    connection.execute(
                        """
                        UPDATE sources
                        SET
                            success_count =
                                success_count + 1,
                            consecutive_failures = ?,
                            backoff_until = ?,
                            next_run_at = ?,
                            scheduling_epoch =
                                scheduling_epoch + 1
                        WHERE source = ?
                        """,
                        (
                            consecutive_failures,
                            backoff_until,
                            next_value,
                            lease.source,
                        ),
                    )
                else:
                    current = connection.execute(
                        """
                        SELECT consecutive_failures
                        FROM sources
                        WHERE source = ?
                        """,
                        (lease.source,),
                    ).fetchone()

                    failures = (
                        1
                        if current is None
                        else int(
                            current[
                                "consecutive_failures"
                            ]
                        )
                        + 1
                    )

                    delay = min(
                        self.max_backoff,
                        self.failure_backoff
                        * (
                            2
                            ** min(
                                failures - 1,
                                16,
                            )
                        ),
                    )

                    next_value = (
                        now + delay
                        if next_run_at is None
                        else float(
                            next_run_at
                        )
                    )

                    connection.execute(
                        """
                        UPDATE sources
                        SET
                            failure_count =
                                failure_count + 1,
                            consecutive_failures = ?,
                            backoff_until = ?,
                            next_run_at = ?,
                            scheduling_epoch =
                                scheduling_epoch + 1
                        WHERE source = ?
                        """,
                        (
                            failures,
                            now + delay,
                            next_value,
                            lease.source,
                        ),
                    )

                connection.execute(
                    """
                    UPDATE source_leases
                    SET state = ?
                    WHERE lease_id = ?
                      AND state = 'active'
                    """,
                    (
                        "complete"
                        if success
                        else "failed",
                        lease.lease_id,
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
    # Expired lease recovery
    # ============================================================

    def recover_expired_leases(
        self,
        limit: int = 10_000,
    ) -> int:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                rows = connection.execute(
                    """
                    SELECT *
                    FROM source_leases
                    WHERE state = 'active'
                      AND lease_until <= ?
                    ORDER BY lease_until ASC
                    LIMIT ?
                    """,
                    (
                        now,
                        max(1, int(limit)),
                    ),
                ).fetchall()

                recovered = 0

                for row in rows:
                    cursor = connection.execute(
                        """
                        UPDATE source_leases
                        SET state = 'expired'
                        WHERE lease_id = ?
                          AND state = 'active'
                          AND lease_until <= ?
                        """,
                        (
                            row["lease_id"],
                            now,
                        ),
                    )

                    if cursor.rowcount != 1:
                        continue

                    connection.execute(
                        """
                        UPDATE sources
                        SET
                            next_run_at = ?,
                            backoff_until = 0,
                            scheduling_epoch =
                                scheduling_epoch + 1
                        WHERE source = ?
                        """,
                        (
                            now,
                            row["source"],
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
    # Durable checkpoints
    # ============================================================

    def write_checkpoint(
        self,
        source: str,
        checkpoint_name: str,
        checkpoint_value: str,
    ) -> bool:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT INTO source_checkpoints (
                        source,
                        checkpoint_name,
                        checkpoint_value,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(
                        source,
                        checkpoint_name
                    )
                    DO UPDATE SET
                        checkpoint_value =
                            excluded.checkpoint_value,
                        updated_at =
                            excluded.updated_at
                    """,
                    (
                        source,
                        checkpoint_name,
                        str(
                            checkpoint_value
                        ),
                        now,
                    ),
                )

                return cursor.rowcount >= 1

            finally:
                connection.close()

    def read_checkpoint(
        self,
        source: str,
        checkpoint_name: str,
    ) -> Optional[
        SourceCheckpoint
    ]:
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT *
                    FROM source_checkpoints
                    WHERE source = ?
                      AND checkpoint_name = ?
                    """,
                    (
                        source,
                        checkpoint_name,
                    ),
                ).fetchone()

                if row is None:
                    return None

                return SourceCheckpoint(
                    source=row["source"],
                    checkpoint_name=(
                        row["checkpoint_name"]
                    ),
                    checkpoint_value=(
                        row["checkpoint_value"]
                    ),
                    updated_at=float(
                        row["updated_at"]
                    ),
                )

            finally:
                connection.close()

    # ============================================================
    # Fair source selection
    # ============================================================

    def select_next_source(
        self,
    ) -> Optional[SourceState]:
        ready = self.ready_sources(
            limit=256
        )

        if not ready:
            return None

        now = time.time()

        def effective_priority(
            state: SourceState,
        ) -> tuple[float, float, float]:
            waiting = max(
                0.0,
                now - state.last_run_at,
            )

            aging = min(
                waiting
                / max(
                    1.0,
                    self.failure_backoff,
                ),
                1_000_000.0,
            )

            return (
                state.priority + aging,
                waiting,
                -state.scheduling_epoch,
            )

        return max(
            ready,
            key=effective_priority,
        )

    # ============================================================
    # Handler execution
    # ============================================================

    def execute_source(
        self,
        source: str,
        worker_id: str,
        generation: int,
        fencing_epoch: int,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[FederatedDiscoveryResult]:
        with self._lock:
            handler = self._sources.get(
                source
            )

        if handler is None:
            return None

        lease = self.acquire_source(
            source=source,
            worker_id=worker_id,
            generation=generation,
            fencing_epoch=fencing_epoch,
        )

        if lease is None:
            return None

        run_id = self.begin_run(
            lease
        )

        started = time.time()

        seen = 0
        accepted = 0

        try:
            result = handler(
                *args,
                **kwargs,
            )

            if isinstance(
                result,
                dict,
            ):
                seen = int(
                    result.get(
                        "candidates_seen",
                        result.get(
                            "seen",
                            0,
                        ),
                    )
                )

                accepted = int(
                    result.get(
                        "candidates_accepted",
                        result.get(
                            "accepted",
                            0,
                        ),
                    )
                )

            elif result is not None:
                try:
                    candidates = list(
                        result
                    )
                    seen = len(
                        candidates
                    )
                    accepted = seen
                except TypeError:
                    pass

            finished = time.time()

            self.finish_run(
                lease=lease,
                run_id=run_id,
                candidates_seen=seen,
                candidates_accepted=accepted,
                success=True,
            )

            return FederatedDiscoveryResult(
                source=source,
                run_id=run_id,
                candidates_seen=seen,
                candidates_accepted=accepted,
                started_at=started,
                completed_at=finished,
                success=True,
            )

        except Exception as exc:
            finished = time.time()

            self.finish_run(
                lease=lease,
                run_id=run_id,
                candidates_seen=seen,
                candidates_accepted=accepted,
                success=False,
                error=str(exc),
            )

            return FederatedDiscoveryResult(
                source=source,
                run_id=run_id,
                candidates_seen=seen,
                candidates_accepted=accepted,
                started_at=started,
                completed_at=finished,
                success=False,
            )

    # ============================================================
    # Federation statistics
    # ============================================================

    def stats(
        self,
    ) -> dict[str, int | float | str]:
        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                source_count = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM sources
                    """
                ).fetchone()

                enabled_count = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM sources
                    WHERE enabled = 1
                    """
                ).fetchone()

                ready_count = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM sources
                    WHERE enabled = 1
                      AND next_run_at <= ?
                      AND backoff_until <= ?
                    """,
                    (
                        now,
                        now,
                    ),
                ).fetchone()

                active_leases = connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM source_leases
                    WHERE state = 'active'
                      AND lease_until > ?
                    """,
                    (now,),
                ).fetchone()

                runs = connection.execute(
                    """
                    SELECT
                        COUNT(*) AS count,
                        COALESCE(
                            SUM(candidates_seen),
                            0
                        ) AS seen,
                        COALESCE(
                            SUM(candidates_accepted),
                            0
                        ) AS accepted
                    FROM source_runs
                    """
                ).fetchone()

                return {
                    "version": self.VERSION,
                    "registered_sources": int(
                        source_count["count"]
                    ),
                    "enabled_sources": int(
                        enabled_count["count"]
                    ),
                    "ready_sources": int(
                        ready_count["count"]
                    ),
                    "active_source_leases": int(
                        active_leases["count"]
                    ),
                    "total_source_runs": int(
                        runs["count"]
                    ),
                    "candidates_seen": int(
                        runs["seen"]
                    ),
                    "candidates_accepted": int(
                        runs["accepted"]
                    ),
                }

            finally:
                connection.close()
