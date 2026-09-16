"""
OUR SEARCH — Global Web Coverage Expansion
Brick 8.5 — Coverage Gap Detection

Architecture:
    Coverage Control Plane
            ↓
    Coverage Gap Detection
            ↓
    Gap Classification
            ↓
    Gap Priority / Aging
            ↓
    Durable Gap Work
            ↓
    Domain / URL Expansion
            ↓
    Discovery Work Queue
            ↓
    Placement-aware Routing
            ↓
    Existing Crawler

This component identifies incomplete or weakly covered areas of the
public-Web discovery universe and converts them into durable, retry-safe,
partition-aware work.

Design goals:
    - enormous-scale architecture
    - deterministic partitioning
    - durable state
    - restart safety
    - idempotent gap creation
    - bounded batch processing
    - priority + aging
    - backpressure awareness
    - lease/fencing support
    - recovery of abandoned work
    - epoch-based progress tracking
    - no fixed small-Web execution ceiling

This module is an orchestration/control-plane component.
It does not itself crawl the Web.
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


VERSION = "coverage-gap-detection.v1"

DEFAULT_PARTITION_COUNT = 1_048_576
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_LEASE_SECONDS = 300.0
DEFAULT_MAX_ATTEMPTS = 8

MIN_GAP_PRIORITY = 0.0
MAX_GAP_PRIORITY = 1_000_000_000.0


@dataclass(frozen=True)
class CoverageGap:
    gap_id: str
    partition_id: int
    scope: str
    scope_key: str
    gap_type: str
    severity: float
    priority: float
    expected: float
    observed: float
    deficit: float
    discovered_at: float
    detected_at: float
    epoch: int
    status: str = "queued"
    attempts: int = 0
    worker_id: Optional[str] = None
    fencing_token: Optional[int] = None
    lease_until: Optional[float] = None
    next_attempt_at: Optional[float] = None
    last_error: Optional[str] = None


@dataclass(frozen=True)
class GapWorkItem:
    gap_id: str
    partition_id: int
    scope: str
    scope_key: str
    gap_type: str
    priority: float
    epoch: int
    worker_id: str
    fencing_token: int
    lease_until: float


@dataclass(frozen=True)
class GapDetectionResult:
    epoch: int
    scanned_partitions: int
    detected_gaps: int
    new_gaps: int
    reopened_gaps: int
    queued_gaps: int
    processing_gaps: int
    completed_gaps: int
    failed_gaps: int


@dataclass(frozen=True)
class GapCapacity:
    max_inflight: Optional[int]
    inflight: int
    available: Optional[int]
    can_accept: bool


class CoverageGapDetection:
    """
    Durable global coverage-gap detector.

    The detector consumes coverage state from the existing coverage
    control plane when available, while maintaining its own durable
    gap catalog so detection survives process restarts.

    SQLite is intentionally used as a durable local/control-plane
    implementation. The architecture is partition-oriented and does
    not require a single global execution queue.
    """

    def __init__(
        self,
        storage_root: str,
        coverage_control_plane: Any = None,
        domain_expansion: Any = None,
        url_expansion: Any = None,
        work_queue: Any = None,
        work_router: Any = None,
        partition_count: int = DEFAULT_PARTITION_COUNT,
        max_batch_size: int = DEFAULT_BATCH_SIZE,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        admission_limit: Optional[int] = None,
        detection_interval: float = 300.0,
    ) -> None:
        if partition_count <= 0:
            raise ValueError("partition_count must be positive")

        if max_batch_size <= 0:
            raise ValueError("max_batch_size must be positive")

        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")

        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")

        if admission_limit is not None and admission_limit <= 0:
            raise ValueError("admission_limit must be positive")

        if detection_interval <= 0:
            raise ValueError("detection_interval must be positive")

        self.storage_root = storage_root
        self.coverage_control_plane = coverage_control_plane
        self.domain_expansion = domain_expansion
        self.url_expansion = url_expansion
        self.work_queue = work_queue
        self.work_router = work_router

        self.partition_count = partition_count
        self.max_batch_size = max_batch_size
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.admission_limit = admission_limit
        self.detection_interval = detection_interval

        self.db_path = f"{storage_root}/coverage_gap_detection.db"

        self._lock = threading.RLock()
        self._running = False
        self._epoch = 0
        self._last_detection_at: Optional[float] = None

        self.stats_data = {
            "version": VERSION,
            "epochs": 0,
            "partitions_scanned": 0,
            "gaps_detected": 0,
            "new_gaps": 0,
            "reopened_gaps": 0,
            "claims": 0,
            "completions": 0,
            "failures": 0,
            "recoveries": 0,
            "runtime_errors": 0,
            "fixed_global_execution_limit": False,
        }

        self._initialize()

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            check_same_thread=False,
        )

        connection.row_factory = sqlite3.Row

        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")

        return connection

    def _initialize(self) -> None:
        connection = self._connect()

        try:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS gaps (
                    gap_id TEXT PRIMARY KEY,
                    partition_id INTEGER NOT NULL,
                    scope TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    gap_type TEXT NOT NULL,
                    severity REAL NOT NULL,
                    priority REAL NOT NULL,
                    expected REAL NOT NULL,
                    observed REAL NOT NULL,
                    deficit REAL NOT NULL,
                    discovered_at REAL NOT NULL,
                    detected_at REAL NOT NULL,
                    epoch INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    worker_id TEXT,
                    fencing_token INTEGER,
                    lease_until REAL,
                    next_attempt_at REAL,
                    last_error TEXT
                );

                CREATE UNIQUE INDEX IF NOT EXISTS
                    idx_gaps_scope
                    ON gaps(scope, scope_key, gap_type);

                CREATE INDEX IF NOT EXISTS
                    idx_gaps_ready
                    ON gaps(
                        status,
                        next_attempt_at,
                        priority DESC,
                        detected_at
                    );

                CREATE INDEX IF NOT EXISTS
                    idx_gaps_partition
                    ON gaps(partition_id, status);

                CREATE INDEX IF NOT EXISTS
                    idx_gaps_worker
                    ON gaps(worker_id, status);

                CREATE INDEX IF NOT EXISTS
                    idx_gaps_epoch
                    ON gaps(epoch, status);

                CREATE TABLE IF NOT EXISTS
                    gap_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gap_id TEXT NOT NULL,
                        event TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        epoch INTEGER NOT NULL,
                        metadata_json TEXT
                    );

                CREATE INDEX IF NOT EXISTS
                    idx_gap_history_gap
                    ON gap_history(gap_id, timestamp);

                CREATE TABLE IF NOT EXISTS
                    gap_checkpoints (
                        detector_id TEXT PRIMARY KEY,
                        epoch INTEGER NOT NULL,
                        partition_cursor INTEGER NOT NULL,
                        updated_at REAL NOT NULL
                    );

                CREATE TABLE IF NOT EXISTS
                    gap_epochs (
                        epoch INTEGER PRIMARY KEY,
                        started_at REAL NOT NULL,
                        completed_at REAL,
                        scanned_partitions INTEGER NOT NULL DEFAULT 0,
                        detected_gaps INTEGER NOT NULL DEFAULT 0,
                        created_gaps INTEGER NOT NULL DEFAULT 0
                    );
                """
            )

            connection.commit()

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Deterministic partitioning
    # ------------------------------------------------------------------

    def partition_for(self, value: str) -> int:
        normalized = str(value).strip().lower()

        digest = hashlib.sha256(
            normalized.encode("utf-8")
        ).digest()

        return int.from_bytes(
            digest[:8],
            "big",
        ) % self.partition_count

    # ------------------------------------------------------------------
    # Stable identifiers
    # ------------------------------------------------------------------

    def gap_id_for(
        self,
        scope: str,
        scope_key: str,
        gap_type: str,
    ) -> str:
        raw = (
            f"{scope}|"
            f"{scope_key.strip().lower()}|"
            f"{gap_type.strip().lower()}"
        )

        return hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

    # ------------------------------------------------------------------
    # Coverage extraction
    # ------------------------------------------------------------------

    def _coverage_state(
        self,
        partition_id: int,
    ) -> Optional[Any]:
        control_plane = self.coverage_control_plane

        if control_plane is None:
            return None

        getter = getattr(
            control_plane,
            "get",
            None,
        )

        if getter is None:
            return None

        try:
            return getter(partition_id)
        except Exception:
            return None

    def _state_value(
        self,
        state: Any,
        name: str,
        default: float = 0.0,
    ) -> float:
        if state is None:
            return default

        if isinstance(state, dict):
            value = state.get(name, default)
        else:
            value = getattr(
                state,
                name,
                default,
            )

        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _state_status(
        self,
        state: Any,
    ) -> str:
        if state is None:
            return "unknown"

        if isinstance(state, dict):
            return str(
                state.get(
                    "status",
                    "unknown",
                )
            )

        return str(
            getattr(
                state,
                "status",
                "unknown",
            )
        )

    # ------------------------------------------------------------------
    # Gap classification
    # ------------------------------------------------------------------

    def _classify_partition_gap(
        self,
        partition_id: int,
        state: Any,
        epoch: int,
        now: float,
    ) -> list[CoverageGap]:
        gaps: list[CoverageGap] = []

        discovered = self._state_value(
            state,
            "discovered",
        )

        activated = self._state_value(
            state,
            "activated",
        )

        completed = self._state_value(
            state,
            "completed",
        )

        failures = self._state_value(
            state,
            "failures",
        )

        unknown = self._state_value(
            state,
            "unknown",
        )

        expected = self._state_value(
            state,
            "expected",
        )

        if expected <= 0:
            expected = max(
                discovered,
                activated,
                completed,
                1.0,
            )

        status = self._state_status(state)

        # --------------------------------------------------------------
        # Discovery gap
        # --------------------------------------------------------------

        if expected > 0 and discovered < expected:
            deficit = max(
                0.0,
                expected - discovered,
            )

            severity = min(
                1_000_000.0,
                deficit / max(expected, 1.0),
            )

            gaps.append(
                self._make_gap(
                    partition_id=partition_id,
                    scope="partition",
                    scope_key=str(partition_id),
                    gap_type="discovery",
                    severity=severity,
                    expected=expected,
                    observed=discovered,
                    deficit=deficit,
                    epoch=epoch,
                    now=now,
                )
            )

        # --------------------------------------------------------------
        # Activation gap
        # --------------------------------------------------------------

        if discovered > activated:
            deficit = max(
                0.0,
                discovered - activated,
            )

            severity = min(
                1_000_000.0,
                deficit / max(discovered, 1.0),
            )

            gaps.append(
                self._make_gap(
                    partition_id=partition_id,
                    scope="partition",
                    scope_key=str(partition_id),
                    gap_type="activation",
                    severity=severity,
                    expected=discovered,
                    observed=activated,
                    deficit=deficit,
                    epoch=epoch,
                    now=now,
                )
            )

        # --------------------------------------------------------------
        # Completion gap
        # --------------------------------------------------------------

        if activated > completed:
            deficit = max(
                0.0,
                activated - completed,
            )

            severity = min(
                1_000_000.0,
                deficit / max(activated, 1.0),
            )

            gaps.append(
                self._make_gap(
                    partition_id=partition_id,
                    scope="partition",
                    scope_key=str(partition_id),
                    gap_type="completion",
                    severity=severity,
                    expected=activated,
                    observed=completed,
                    deficit=deficit,
                    epoch=epoch,
                    now=now,
                )
            )

        # --------------------------------------------------------------
        # Failure concentration
        # --------------------------------------------------------------

        if failures > 0:
            severity = min(
                1_000_000.0,
                failures / max(
                    discovered,
                    activated,
                    1.0,
                ),
            )

            gaps.append(
                self._make_gap(
                    partition_id=partition_id,
                    scope="partition",
                    scope_key=str(partition_id),
                    gap_type="failure_concentration",
                    severity=severity,
                    expected=max(
                        discovered,
                        activated,
                        1.0,
                    ),
                    observed=failures,
                    deficit=failures,
                    epoch=epoch,
                    now=now,
                )
            )

        # --------------------------------------------------------------
        # Unknown coverage
        # --------------------------------------------------------------

        if unknown > 0:
            severity = min(
                1_000_000.0,
                unknown / max(
                    expected,
                    discovered,
                    1.0,
                ),
            )

            gaps.append(
                self._make_gap(
                    partition_id=partition_id,
                    scope="partition",
                    scope_key=str(partition_id),
                    gap_type="unknown",
                    severity=severity,
                    expected=max(
                        expected,
                        discovered,
                        1.0,
                    ),
                    observed=unknown,
                    deficit=unknown,
                    epoch=epoch,
                    now=now,
                )
            )

        if status == "unknown" and not gaps:
            gaps.append(
                self._make_gap(
                    partition_id=partition_id,
                    scope="partition",
                    scope_key=str(partition_id),
                    gap_type="untracked",
                    severity=1.0,
                    expected=1.0,
                    observed=0.0,
                    deficit=1.0,
                    epoch=epoch,
                    now=now,
                )
            )

        return gaps

    def _make_gap(
        self,
        partition_id: int,
        scope: str,
        scope_key: str,
        gap_type: str,
        severity: float,
        expected: float,
        observed: float,
        deficit: float,
        epoch: int,
        now: float,
    ) -> CoverageGap:
        gap_id = self.gap_id_for(
            scope,
            scope_key,
            gap_type,
        )

        priority = self._priority(
            severity=severity,
            deficit=deficit,
            expected=expected,
            detected_at=now,
        )

        return CoverageGap(
            gap_id=gap_id,
            partition_id=partition_id,
            scope=scope,
            scope_key=scope_key,
            gap_type=gap_type,
            severity=severity,
            priority=priority,
            expected=expected,
            observed=observed,
            deficit=deficit,
            discovered_at=now,
            detected_at=now,
            epoch=epoch,
        )

    def _priority(
        self,
        severity: float,
        deficit: float,
        expected: float,
        detected_at: float,
    ) -> float:
        normalized_deficit = (
            deficit / max(expected, 1.0)
        )

        age_bonus = max(
            0.0,
            time.time() - detected_at,
        ) / 3600.0

        priority = (
            severity * 1_000.0
            + normalized_deficit * 100.0
            + age_bonus
        )

        return min(
            MAX_GAP_PRIORITY,
            max(
                MIN_GAP_PRIORITY,
                priority,
            ),
        )

    # ------------------------------------------------------------------
    # Durable gap insertion
    # ------------------------------------------------------------------

    def _record_history(
        self,
        connection: sqlite3.Connection,
        gap_id: str,
        event: str,
        epoch: int,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO gap_history (
                gap_id,
                event,
                timestamp,
                epoch,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                gap_id,
                event,
                time.time(),
                epoch,
                json.dumps(
                    metadata or {},
                    sort_keys=True,
                    default=str,
                ),
            ),
        )

    def _upsert_gap(
        self,
        connection: sqlite3.Connection,
        gap: CoverageGap,
    ) -> tuple[bool, bool]:
        existing = connection.execute(
            """
            SELECT
                status,
                priority,
                attempts,
                epoch
            FROM gaps
            WHERE gap_id = ?
            """,
            (gap.gap_id,),
        ).fetchone()

        if existing is None:
            connection.execute(
                """
                INSERT INTO gaps (
                    gap_id,
                    partition_id,
                    scope,
                    scope_key,
                    gap_type,
                    severity,
                    priority,
                    expected,
                    observed,
                    deficit,
                    discovered_at,
                    detected_at,
                    epoch,
                    status,
                    attempts,
                    worker_id,
                    fencing_token,
                    lease_until,
                    next_attempt_at,
                    last_error
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, 'queued', 0,
                    NULL, NULL, NULL, NULL, NULL
                )
                """,
                (
                    gap.gap_id,
                    gap.partition_id,
                    gap.scope,
                    gap.scope_key,
                    gap.gap_type,
                    gap.severity,
                    gap.priority,
                    gap.expected,
                    gap.observed,
                    gap.deficit,
                    gap.discovered_at,
                    gap.detected_at,
                    gap.epoch,
                ),
            )

            self._record_history(
                connection,
                gap.gap_id,
                "created",
                gap.epoch,
            )

            return True, False

        status = str(existing["status"])

        reopened = status in {
            "completed",
            "failed",
        }

        if reopened:
            connection.execute(
                """
                UPDATE gaps
                SET
                    partition_id = ?,
                    severity = ?,
                    priority = ?,
                    expected = ?,
                    observed = ?,
                    deficit = ?,
                    detected_at = ?,
                    epoch = ?,
                    status = 'queued',
                    attempts = 0,
                    worker_id = NULL,
                    fencing_token = NULL,
                    lease_until = NULL,
                    next_attempt_at = NULL,
                    last_error = NULL
                WHERE gap_id = ?
                """,
                (
                    gap.partition_id,
                    gap.severity,
                    gap.priority,
                    gap.expected,
                    gap.observed,
                    gap.deficit,
                    gap.detected_at,
                    gap.epoch,
                    gap.gap_id,
                ),
            )

            self._record_history(
                connection,
                gap.gap_id,
                "reopened",
                gap.epoch,
            )

            return False, True

        connection.execute(
            """
            UPDATE gaps
            SET
                partition_id = ?,
                severity = ?,
                priority = ?,
                expected = ?,
                observed = ?,
                deficit = ?,
                detected_at = ?,
                epoch = ?
            WHERE gap_id = ?
            """,
            (
                gap.partition_id,
                gap.severity,
                gap.priority,
                gap.expected,
                gap.observed,
                gap.deficit,
                gap.detected_at,
                gap.epoch,
                gap.gap_id,
            ),
        )

        return False, False

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def begin_epoch(
        self,
        epoch: Optional[int] = None,
    ) -> int:
        with self._lock:
            if epoch is None:
                self._epoch += 1
            else:
                self._epoch = max(
                    self._epoch,
                    int(epoch),
                )

            current = self._epoch

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT OR IGNORE INTO gap_epochs (
                    epoch,
                    started_at
                )
                VALUES (?, ?)
                """,
                (
                    current,
                    time.time(),
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data["epochs"] += 1

        return current

    def detect_partition(
        self,
        partition_id: int,
        epoch: Optional[int] = None,
    ) -> list[CoverageGap]:
        if partition_id < 0:
            raise ValueError(
                "partition_id must be non-negative"
            )

        if epoch is None:
            epoch = self._epoch or self.begin_epoch()

        now = time.time()

        state = self._coverage_state(
            partition_id,
        )

        return self._classify_partition_gap(
            partition_id=partition_id,
            state=state,
            epoch=epoch,
            now=now,
        )

    def detect_many(
        self,
        partition_ids: Iterable[int],
        epoch: Optional[int] = None,
    ) -> GapDetectionResult:
        if epoch is None:
            epoch = self._epoch or self.begin_epoch()

        partition_list = list(
            dict.fromkeys(
                int(partition)
                for partition in partition_ids
            )
        )

        if not partition_list:
            return GapDetectionResult(
                epoch=epoch,
                scanned_partitions=0,
                detected_gaps=0,
                new_gaps=0,
                reopened_gaps=0,
                queued_gaps=self.count("queued"),
                processing_gaps=self.count("processing"),
                completed_gaps=self.count("completed"),
                failed_gaps=self.count("failed"),
            )

        connection = self._connect()

        scanned = 0
        detected = 0
        created = 0
        reopened = 0

        try:
            connection.execute("BEGIN")

            for partition_id in partition_list:
                gaps = self.detect_partition(
                    partition_id,
                    epoch=epoch,
                )

                scanned += 1
                detected += len(gaps)

                for gap in gaps:
                    is_new, was_reopened = (
                        self._upsert_gap(
                            connection,
                            gap,
                        )
                    )

                    if is_new:
                        created += 1

                    if was_reopened:
                        reopened += 1

            connection.execute(
                """
                INSERT INTO gap_epochs (
                    epoch,
                    started_at,
                    completed_at,
                    scanned_partitions,
                    detected_gaps,
                    created_gaps
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(epoch)
                DO UPDATE SET
                    completed_at = excluded.completed_at,
                    scanned_partitions =
                        excluded.scanned_partitions,
                    detected_gaps =
                        excluded.detected_gaps,
                    created_gaps =
                        excluded.created_gaps
                """,
                (
                    epoch,
                    time.time(),
                    time.time(),
                    scanned,
                    detected,
                    created,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data["partitions_scanned"] += scanned
        self.stats_data["gaps_detected"] += detected
        self.stats_data["new_gaps"] += created
        self.stats_data["reopened_gaps"] += reopened

        self._last_detection_at = time.time()

        return GapDetectionResult(
            epoch=epoch,
            scanned_partitions=scanned,
            detected_gaps=detected,
            new_gaps=created,
            reopened_gaps=reopened,
            queued_gaps=self.count("queued"),
            processing_gaps=self.count("processing"),
            completed_gaps=self.count("completed"),
            failed_gaps=self.count("failed"),
        )

    def detect_active_partitions(
        self,
        limit: Optional[int] = None,
    ) -> GapDetectionResult:
        partitions: list[int] = []

        control_plane = self.coverage_control_plane

        if control_plane is not None:
            getter = getattr(
                control_plane,
                "active_partitions",
                None,
            )

            if getter is not None:
                try:
                    result = getter()

                    if result is not None:
                        partitions.extend(
                            int(item)
                            for item in result
                        )
                except Exception:
                    self.stats_data[
                        "runtime_errors"
                    ] += 1

        if not partitions:
            partitions = list(
                range(
                    min(
                        self.partition_count,
                        self.max_batch_size,
                    )
                )
            )

        if limit is not None:
            partitions = partitions[:limit]

        return self.detect_many(
            partitions,
        )

    # ------------------------------------------------------------------
    # Capacity / backpressure
    # ------------------------------------------------------------------

    def capacity(self) -> GapCapacity:
        inflight = self.count("processing")

        if self.admission_limit is None:
            return GapCapacity(
                max_inflight=None,
                inflight=inflight,
                available=None,
                can_accept=True,
            )

        available = max(
            0,
            self.admission_limit - inflight,
        )

        return GapCapacity(
            max_inflight=self.admission_limit,
            inflight=inflight,
            available=available,
            can_accept=available > 0,
        )

    def can_accept(self) -> bool:
        capacity = self.capacity()
        return capacity.can_accept

    # ------------------------------------------------------------------
    # Durable claiming
    # ------------------------------------------------------------------

    def claim(
        self,
        worker_id: str,
        limit: int = 1,
        partition_id: Optional[int] = None,
    ) -> list[GapWorkItem]:
        if not worker_id:
            raise ValueError(
                "worker_id must not be empty"
            )

        if limit <= 0:
            return []

        capacity = self.capacity()

        if (
            capacity.available is not None
            and capacity.available <= 0
        ):
            return []

        now = time.time()

        connection = self._connect()
        claimed: list[GapWorkItem] = []

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            query = """
                SELECT *
                FROM gaps
                WHERE
                    status = 'queued'
                    AND (
                        next_attempt_at IS NULL
                        OR next_attempt_at <= ?
                    )
            """

            params: list[Any] = [now]

            if partition_id is not None:
                query += """
                    AND partition_id = ?
                """
                params.append(
                    int(partition_id)
                )

            query += """
                ORDER BY
                    priority DESC,
                    detected_at ASC,
                    gap_id ASC
                LIMIT ?
            """

            params.append(limit)

            rows = connection.execute(
                query,
                tuple(params),
            ).fetchall()

            fencing_token = int(
                time.time_ns()
            )

            lease_until = (
                now + self.lease_seconds
            )

            for row in rows:
                gap_id = str(
                    row["gap_id"]
                )

                updated = connection.execute(
                    """
                    UPDATE gaps
                    SET
                        status = 'processing',
                        attempts = attempts + 1,
                        worker_id = ?,
                        fencing_token = ?,
                        lease_until = ?,
                        last_error = NULL
                    WHERE
                        gap_id = ?
                        AND status = 'queued'
                    """,
                    (
                        worker_id,
                        fencing_token,
                        lease_until,
                        gap_id,
                    ),
                ).rowcount

                if updated != 1:
                    continue

                claimed.append(
                    GapWorkItem(
                        gap_id=gap_id,
                        partition_id=int(
                            row["partition_id"]
                        ),
                        scope=str(
                            row["scope"]
                        ),
                        scope_key=str(
                            row["scope_key"]
                        ),
                        gap_type=str(
                            row["gap_type"]
                        ),
                        priority=float(
                            row["priority"]
                        ),
                        epoch=int(
                            row["epoch"]
                        ),
                        worker_id=worker_id,
                        fencing_token=fencing_token,
                        lease_until=lease_until,
                    )
                )

                self._record_history(
                    connection,
                    gap_id,
                    "claimed",
                    int(row["epoch"]),
                    {
                        "worker_id": worker_id,
                        "fencing_token": fencing_token,
                    },
                )

            connection.commit()

        finally:
            connection.close()

        self.stats_data["claims"] += len(
            claimed
        )

        return claimed

    def renew(
        self,
        work: GapWorkItem,
    ) -> bool:
        now = time.time()
        lease_until = (
            now + self.lease_seconds
        )

        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE gaps
                SET lease_until = ?
                WHERE
                    gap_id = ?
                    AND status = 'processing'
                    AND worker_id = ?
                    AND fencing_token = ?
                """,
                (
                    lease_until,
                    work.gap_id,
                    work.worker_id,
                    work.fencing_token,
                ),
            ).rowcount

            connection.commit()

            return updated == 1

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Completion / failure
    # ------------------------------------------------------------------

    def complete(
        self,
        work: GapWorkItem,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE gaps
                SET
                    status = 'completed',
                    worker_id = NULL,
                    fencing_token = NULL,
                    lease_until = NULL,
                    next_attempt_at = NULL,
                    last_error = NULL
                WHERE
                    gap_id = ?
                    AND status = 'processing'
                    AND worker_id = ?
                    AND fencing_token = ?
                """,
                (
                    work.gap_id,
                    work.worker_id,
                    work.fencing_token,
                ),
            ).rowcount

            if updated == 1:
                self._record_history(
                    connection,
                    work.gap_id,
                    "completed",
                    work.epoch,
                    metadata,
                )

            connection.commit()

        finally:
            connection.close()

        if updated == 1:
            self.stats_data[
                "completions"
            ] += 1

            return True

        return False

    def fail(
        self,
        work: GapWorkItem,
        error: str,
    ) -> bool:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT attempts, epoch
                FROM gaps
                WHERE gap_id = ?
                """,
                (work.gap_id,),
            ).fetchone()

            if row is None:
                connection.rollback()
                return False

            attempts = int(
                row["attempts"]
            )

            if attempts >= self.max_attempts:
                status = "failed"
                next_attempt = None
            else:
                status = "queued"

                backoff = min(
                    3600.0,
                    2.0 ** min(
                        attempts,
                        12,
                    ),
                )

                next_attempt = (
                    time.time() + backoff
                )

            updated = connection.execute(
                """
                UPDATE gaps
                SET
                    status = ?,
                    worker_id = NULL,
                    fencing_token = NULL,
                    lease_until = NULL,
                    next_attempt_at = ?,
                    last_error = ?
                WHERE
                    gap_id = ?
                    AND status = 'processing'
                    AND worker_id = ?
                    AND fencing_token = ?
                """,
                (
                    status,
                    next_attempt,
                    str(error)[:4000],
                    work.gap_id,
                    work.worker_id,
                    work.fencing_token,
                ),
            ).rowcount

            if updated == 1:
                self._record_history(
                    connection,
                    work.gap_id,
                    "failed",
                    int(row["epoch"]),
                    {
                        "error": str(error)[:4000],
                        "status": status,
                    },
                )

            connection.commit()

        finally:
            connection.close()

        if updated == 1:
            self.stats_data[
                "failures"
            ] += 1

            return True

        return False

    # ------------------------------------------------------------------
    # Lease recovery
    # ------------------------------------------------------------------

    def recover_expired(
        self,
        now: Optional[float] = None,
    ) -> int:
        if now is None:
            now = time.time()

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            rows = connection.execute(
                """
                SELECT
                    gap_id,
                    attempts,
                    epoch
                FROM gaps
                WHERE
                    status = 'processing'
                    AND lease_until IS NOT NULL
                    AND lease_until <= ?
                """,
                (now,),
            ).fetchall()

            recovered = 0

            for row in rows:
                attempts = int(
                    row["attempts"]
                )

                if attempts >= self.max_attempts:
                    status = "failed"
                    next_attempt = None
                else:
                    status = "queued"

                    backoff = min(
                        3600.0,
                        2.0 ** min(
                            attempts,
                            12,
                        ),
                    )

                    next_attempt = (
                        now + backoff
                    )

                updated = connection.execute(
                    """
                    UPDATE gaps
                    SET
                        status = ?,
                        worker_id = NULL,
                        fencing_token = NULL,
                        lease_until = NULL,
                        next_attempt_at = ?,
                        last_error = ?
                    WHERE
                        gap_id = ?
                        AND status = 'processing'
                    """,
                    (
                        status,
                        next_attempt,
                        "expired lease recovered",
                        str(row["gap_id"]),
                    ),
                ).rowcount

                if updated == 1:
                    recovered += 1

                    self._record_history(
                        connection,
                        str(row["gap_id"]),
                        "lease_recovered",
                        int(row["epoch"]),
                    )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "recoveries"
        ] += recovered

        return recovered

    # ------------------------------------------------------------------
    # Work generation
    # ------------------------------------------------------------------

    def _downstream_capacity(self) -> bool:
        for downstream in (
            self.work_router,
            self.work_queue,
            self.domain_expansion,
            self.url_expansion,
        ):
            if downstream is None:
                continue

            checker = getattr(
                downstream,
                "can_accept",
                None,
            )

            if checker is None:
                checker = getattr(
                    downstream,
                    "can_accept_work",
                    None,
                )

            if checker is None:
                continue

            try:
                if not bool(checker()):
                    return False
            except Exception:
                continue

        return True

    def _gap_to_scope_value(
        self,
        gap: CoverageGap,
    ) -> str:
        return (
            f"{gap.scope}:"
            f"{gap.scope_key}:"
            f"{gap.gap_type}"
        )

    def prepare_work(
        self,
        limit: Optional[int] = None,
    ) -> int:
        """
        Prepare durable gap work.

        Gap records themselves are the durable work catalog. This method
        optionally forwards gap identities into a downstream work system
        when that system exposes an appropriate admission API.

        It intentionally avoids forcing a particular queue schema.
        """
        if limit is None:
            limit = self.max_batch_size

        if limit <= 0:
            return 0

        if not self._downstream_capacity():
            return 0

        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT *
                FROM gaps
                WHERE status = 'queued'
                ORDER BY
                    priority DESC,
                    detected_at ASC,
                    gap_id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        finally:
            connection.close()

        if not rows:
            return 0

        admitted = 0

        for row in rows:
            gap = CoverageGap(
                gap_id=str(
                    row["gap_id"]
                ),
                partition_id=int(
                    row["partition_id"]
                ),
                scope=str(
                    row["scope"]
                ),
                scope_key=str(
                    row["scope_key"]
                ),
                gap_type=str(
                    row["gap_type"]
                ),
                severity=float(
                    row["severity"]
                ),
                priority=float(
                    row["priority"]
                ),
                expected=float(
                    row["expected"]
                ),
                observed=float(
                    row["observed"]
                ),
                deficit=float(
                    row["deficit"]
                ),
                discovered_at=float(
                    row["discovered_at"]
                ),
                detected_at=float(
                    row["detected_at"]
                ),
                epoch=int(
                    row["epoch"]
                ),
                status=str(
                    row["status"]
                ),
                attempts=int(
                    row["attempts"]
                ),
                worker_id=row["worker_id"],
                fencing_token=row[
                    "fencing_token"
                ],
                lease_until=row[
                    "lease_until"
                ],
                next_attempt_at=row[
                    "next_attempt_at"
                ],
                last_error=row[
                    "last_error"
                ],
            )

            forwarded = False

            for downstream in (
                self.domain_expansion,
                self.url_expansion,
                self.work_queue,
            ):
                if downstream is None:
                    continue

                method = getattr(
                    downstream,
                    "enqueue_gap",
                    None,
                )

                if method is None:
                    method = getattr(
                        downstream,
                        "enqueue",
                        None,
                    )

                if method is None:
                    continue

                try:
                    result = method(
                        self._gap_to_scope_value(
                            gap
                        )
                    )

                    forwarded = (
                        result is not False
                    )

                    if forwarded:
                        break

                except TypeError:
                    continue
                except Exception:
                    self.stats_data[
                        "runtime_errors"
                    ] += 1
                    continue

            if forwarded:
                admitted += 1

        return admitted

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def count(
        self,
        status: Optional[str] = None,
    ) -> int:
        connection = self._connect()

        try:
            if status is None:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM gaps
                    """
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM gaps
                    WHERE status = ?
                    """,
                    (status,),
                ).fetchone()

            return int(row[0])

        finally:
            connection.close()

    def list_gaps(
        self,
        status: Optional[str] = None,
        partition_id: Optional[int] = None,
        limit: int = 100,
    ) -> list[CoverageGap]:
        if limit <= 0:
            return []

        connection = self._connect()

        try:
            query = """
                SELECT *
                FROM gaps
                WHERE 1 = 1
            """

            params: list[Any] = []

            if status is not None:
                query += """
                    AND status = ?
                """
                params.append(status)

            if partition_id is not None:
                query += """
                    AND partition_id = ?
                """
                params.append(
                    int(partition_id)
                )

            query += """
                ORDER BY
                    priority DESC,
                    detected_at ASC,
                    gap_id ASC
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
            CoverageGap(
                gap_id=str(row["gap_id"]),
                partition_id=int(
                    row["partition_id"]
                ),
                scope=str(row["scope"]),
                scope_key=str(
                    row["scope_key"]
                ),
                gap_type=str(
                    row["gap_type"]
                ),
                severity=float(
                    row["severity"]
                ),
                priority=float(
                    row["priority"]
                ),
                expected=float(
                    row["expected"]
                ),
                observed=float(
                    row["observed"]
                ),
                deficit=float(
                    row["deficit"]
                ),
                discovered_at=float(
                    row["discovered_at"]
                ),
                detected_at=float(
                    row["detected_at"]
                ),
                epoch=int(row["epoch"]),
                status=str(
                    row["status"]
                ),
                attempts=int(
                    row["attempts"]
                ),
                worker_id=row[
                    "worker_id"
                ],
                fencing_token=row[
                    "fencing_token"
                ],
                lease_until=row[
                    "lease_until"
                ],
                next_attempt_at=row[
                    "next_attempt_at"
                ],
                last_error=row[
                    "last_error"
                ],
            )
            for row in rows
        ]

    def stale_partitions(
        self,
        limit: int = 100,
    ) -> list[int]:
        control_plane = (
            self.coverage_control_plane
        )

        if control_plane is not None:
            getter = getattr(
                control_plane,
                "stale_partitions",
                None,
            )

            if getter is not None:
                try:
                    result = getter(
                        limit=limit
                    )

                    return [
                        int(item)
                        for item in result
                    ]
                except TypeError:
                    try:
                        result = getter()

                        return [
                            int(item)
                            for item in list(result)[
                                :limit
                            ]
                        ]
                    except Exception:
                        pass
                except Exception:
                    pass

        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT
                    partition_id,
                    MAX(detected_at) AS latest
                FROM gaps
                GROUP BY partition_id
                ORDER BY latest ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        finally:
            connection.close()

        return [
            int(row["partition_id"])
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def checkpoint(
        self,
        detector_id: str = "global",
        partition_cursor: int = 0,
    ) -> None:
        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO gap_checkpoints (
                    detector_id,
                    epoch,
                    partition_cursor,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(detector_id)
                DO UPDATE SET
                    epoch = excluded.epoch,
                    partition_cursor =
                        excluded.partition_cursor,
                    updated_at =
                        excluded.updated_at
                """,
                (
                    detector_id,
                    self._epoch,
                    int(partition_cursor),
                    time.time(),
                ),
            )

            connection.commit()

        finally:
            connection.close()

    def read_checkpoint(
        self,
        detector_id: str = "global",
    ) -> Optional[dict[str, Any]]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT
                    detector_id,
                    epoch,
                    partition_cursor,
                    updated_at
                FROM gap_checkpoints
                WHERE detector_id = ?
                """,
                (detector_id,),
            ).fetchone()

        finally:
            connection.close()

        if row is None:
            return None

        return {
            "detector_id": str(
                row["detector_id"]
            ),
            "epoch": int(
                row["epoch"]
            ),
            "partition_cursor": int(
                row["partition_cursor"]
            ),
            "updated_at": float(
                row["updated_at"]
            ),
        }

    # ------------------------------------------------------------------
    # Continuous operation
    # ------------------------------------------------------------------

    def cycle(
        self,
        partitions: Optional[
            Iterable[int]
        ] = None,
        limit: Optional[int] = None,
    ) -> GapDetectionResult:
        recovered = self.recover_expired()

        if partitions is None:
            if limit is None:
                limit = self.max_batch_size

            partitions = self.stale_partitions(
                limit=limit
            )

            if not partitions:
                partitions = list(
                    range(
                        min(
                            self.partition_count,
                            limit,
                        )
                    )
                )

        result = self.detect_many(
            partitions
        )

        if recovered:
            self.stats_data[
                "recoveries"
            ] += 0

        return result

    def run(
        self,
        interval: Optional[float] = None,
    ) -> None:
        if interval is None:
            interval = self.detection_interval

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
                    self.cycle()
                except Exception:
                    self.stats_data[
                        "runtime_errors"
                    ] += 1

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

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        capacity = self.capacity()

        return {
            **self.stats_data,
            "epoch": self._epoch,
            "last_detection_at": (
                self._last_detection_at
            ),
            "queued": self.count("queued"),
            "processing": self.count(
                "processing"
            ),
            "completed": self.count(
                "completed"
            ),
            "failed": self.count(
                "failed"
            ),
            "total": self.count(),
            "capacity": asdict(
                capacity
            ),
        }


__all__ = [
    "VERSION",
    "CoverageGap",
    "GapWorkItem",
    "GapDetectionResult",
    "GapCapacity",
    "CoverageGapDetection",
]
