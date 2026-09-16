"""
OUR SEARCH — Global Web Coverage Expansion
Brick 8.6 — Adaptive Discovery Expansion

Architecture:

    Crawler Results
          ↓
    Coverage Signals
          ↓
    Freshness Signals
          ↓
    Coverage Gaps
          ↓
    Adaptive Decision Engine
          ↓
    Discovery Strategy
          ↓
    Domain / URL Expansion
          ↓
    Durable Discovery Work
          ↓
    Placement-aware Routing
          ↓
    Existing Crawler
          ↓
    New Results
          ↺

Purpose:
    Dynamically adjust discovery effort based on observed coverage,
    freshness, failures, gaps, source quality, domain activity and
    downstream capacity.

This module is an adaptive control-plane component.
It does not crawl the Web itself.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from typing import Any, Iterable, Optional


VERSION = "adaptive-discovery-expansion.v1"

DEFAULT_PARTITION_COUNT = 1_048_576
DEFAULT_BATCH_SIZE = 10_000

MIN_DISCOVERY_RATE = 0.05
MAX_DISCOVERY_RATE = 100.0

MIN_PRIORITY = 0.0
MAX_PRIORITY = 1_000_000_000.0


@dataclass(frozen=True)
class AdaptiveSignal:
    partition_id: int
    coverage_ratio: float
    discovery_rate: float
    activation_rate: float
    completion_rate: float
    failure_rate: float
    freshness_pressure: float
    gap_pressure: float
    source_pressure: float
    age_pressure: float
    observed_at: float


@dataclass(frozen=True)
class AdaptiveDecision:
    partition_id: int
    discovery_rate: float
    priority_multiplier: float
    expansion_depth: int
    strategy: str
    reason: str
    epoch: int
    created_at: float


@dataclass(frozen=True)
class AdaptiveWorkItem:
    work_id: str
    partition_id: int
    scope: str
    scope_key: str
    strategy: str
    priority: float
    expansion_depth: int
    epoch: int
    created_at: float
    status: str = "queued"
    attempts: int = 0
    worker_id: Optional[str] = None
    fencing_token: Optional[int] = None
    lease_until: Optional[float] = None
    next_attempt_at: Optional[float] = None
    last_error: Optional[str] = None


@dataclass(frozen=True)
class AdaptiveCapacity:
    max_inflight: Optional[int]
    inflight: int
    available: Optional[int]
    can_accept: bool


class AdaptiveDiscoveryExpansion:
    """
    Durable adaptive discovery controller.

    The controller continuously converts observed Web-coverage signals
    into deterministic discovery decisions and durable work.

    It is intentionally decoupled from any particular crawler worker
    implementation.
    """

    def __init__(
        self,
        storage_root: str,
        coverage_control_plane: Any = None,
        gap_detector: Any = None,
        freshness: Any = None,
        domain_expansion: Any = None,
        url_expansion: Any = None,
        work_queue: Any = None,
        work_router: Any = None,
        partition_count: int = DEFAULT_PARTITION_COUNT,
        max_batch_size: int = DEFAULT_BATCH_SIZE,
        lease_seconds: float = 300.0,
        max_attempts: int = 8,
        admission_limit: Optional[int] = None,
        adaptation_interval: float = 300.0,
    ) -> None:
        if partition_count <= 0:
            raise ValueError(
                "partition_count must be positive"
            )

        if max_batch_size <= 0:
            raise ValueError(
                "max_batch_size must be positive"
            )

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive"
            )

        if max_attempts <= 0:
            raise ValueError(
                "max_attempts must be positive"
            )

        if admission_limit is not None and admission_limit <= 0:
            raise ValueError(
                "admission_limit must be positive"
            )

        if adaptation_interval <= 0:
            raise ValueError(
                "adaptation_interval must be positive"
            )

        self.storage_root = storage_root
        self.coverage_control_plane = (
            coverage_control_plane
        )
        self.gap_detector = gap_detector
        self.freshness = freshness
        self.domain_expansion = domain_expansion
        self.url_expansion = url_expansion
        self.work_queue = work_queue
        self.work_router = work_router

        self.partition_count = partition_count
        self.max_batch_size = max_batch_size
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.admission_limit = admission_limit
        self.adaptation_interval = adaptation_interval

        self.db_path = (
            f"{storage_root}/"
            "adaptive_discovery_expansion.db"
        )

        self._lock = threading.RLock()
        self._running = False
        self._epoch = 0
        self._last_adaptation_at: Optional[float] = None

        self.stats_data = {
            "version": VERSION,
            "epochs": 0,
            "signals_observed": 0,
            "decisions_created": 0,
            "work_created": 0,
            "claims": 0,
            "completions": 0,
            "failures": 0,
            "recoveries": 0,
            "runtime_errors": 0,
            "fixed_global_execution_limit": False,
        }

        self._initialize()

    # ------------------------------------------------------------------
    # Durable storage
    # ------------------------------------------------------------------

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
                CREATE TABLE IF NOT EXISTS
                adaptive_signals (
                    partition_id INTEGER NOT NULL,
                    observed_at REAL NOT NULL,
                    coverage_ratio REAL NOT NULL,
                    discovery_rate REAL NOT NULL,
                    activation_rate REAL NOT NULL,
                    completion_rate REAL NOT NULL,
                    failure_rate REAL NOT NULL,
                    freshness_pressure REAL NOT NULL,
                    gap_pressure REAL NOT NULL,
                    source_pressure REAL NOT NULL,
                    age_pressure REAL NOT NULL,
                    PRIMARY KEY (
                        partition_id,
                        observed_at
                    )
                );

                CREATE INDEX IF NOT EXISTS
                idx_adaptive_signals_partition
                ON adaptive_signals(
                    partition_id,
                    observed_at DESC
                );

                CREATE TABLE IF NOT EXISTS
                adaptive_decisions (
                    partition_id INTEGER NOT NULL,
                    epoch INTEGER NOT NULL,
                    discovery_rate REAL NOT NULL,
                    priority_multiplier REAL NOT NULL,
                    expansion_depth INTEGER NOT NULL,
                    strategy TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (
                        partition_id,
                        epoch
                    )
                );

                CREATE INDEX IF NOT EXISTS
                idx_adaptive_decisions_priority
                ON adaptive_decisions(
                    priority_multiplier DESC,
                    created_at
                );

                CREATE TABLE IF NOT EXISTS
                adaptive_work (
                    work_id TEXT PRIMARY KEY,
                    partition_id INTEGER NOT NULL,
                    scope TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    priority REAL NOT NULL,
                    expansion_depth INTEGER NOT NULL,
                    epoch INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    worker_id TEXT,
                    fencing_token INTEGER,
                    lease_until REAL,
                    next_attempt_at REAL,
                    last_error TEXT
                );

                CREATE INDEX IF NOT EXISTS
                idx_adaptive_work_ready
                ON adaptive_work(
                    status,
                    next_attempt_at,
                    priority DESC,
                    created_at
                );

                CREATE INDEX IF NOT EXISTS
                idx_adaptive_work_partition
                ON adaptive_work(
                    partition_id,
                    status
                );

                CREATE INDEX IF NOT EXISTS
                idx_adaptive_work_worker
                ON adaptive_work(
                    worker_id,
                    status
                );

                CREATE TABLE IF NOT EXISTS
                adaptive_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    work_id TEXT,
                    partition_id INTEGER,
                    epoch INTEGER,
                    event TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    metadata_json TEXT
                );

                CREATE INDEX IF NOT EXISTS
                idx_adaptive_history_work
                ON adaptive_history(
                    work_id,
                    timestamp
                );

                CREATE TABLE IF NOT EXISTS
                adaptive_checkpoints (
                    controller_id TEXT PRIMARY KEY,
                    epoch INTEGER NOT NULL,
                    partition_cursor INTEGER NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS
                adaptive_epochs (
                    epoch INTEGER PRIMARY KEY,
                    started_at REAL NOT NULL,
                    completed_at REAL,
                    partitions_processed INTEGER NOT NULL DEFAULT 0,
                    decisions_created INTEGER NOT NULL DEFAULT 0,
                    work_created INTEGER NOT NULL DEFAULT 0
                );
                """
            )

            connection.commit()

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Deterministic partitioning
    # ------------------------------------------------------------------

    def partition_for(
        self,
        value: str,
    ) -> int:
        normalized = (
            str(value)
            .strip()
            .lower()
        )

        digest = hashlib.sha256(
            normalized.encode("utf-8")
        ).digest()

        return (
            int.from_bytes(
                digest[:8],
                "big",
            )
            % self.partition_count
        )

    # ------------------------------------------------------------------
    # Epochs
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
                INSERT OR IGNORE INTO
                adaptive_epochs (
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

    # ------------------------------------------------------------------
    # Generic state extraction
    # ------------------------------------------------------------------

    def _value(
        self,
        state: Any,
        name: str,
        default: float = 0.0,
    ) -> float:
        if state is None:
            return default

        if isinstance(state, dict):
            value = state.get(
                name,
                default,
            )
        else:
            value = getattr(
                state,
                name,
                default,
            )

        try:
            return float(value)
        except (
            TypeError,
            ValueError,
        ):
            return default

    def _coverage_state(
        self,
        partition_id: int,
    ) -> Any:
        control = (
            self.coverage_control_plane
        )

        if control is None:
            return None

        getter = getattr(
            control,
            "get",
            None,
        )

        if getter is None:
            return None

        try:
            return getter(
                partition_id
            )
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Gap pressure
    # ------------------------------------------------------------------

    def _gap_pressure(
        self,
        partition_id: int,
    ) -> float:
        detector = self.gap_detector

        if detector is None:
            return 0.0

        try:
            gaps = detector.list_gaps(
                status="queued",
                partition_id=partition_id,
                limit=32,
            )

            if not gaps:
                return 0.0

            pressure = 0.0

            for gap in gaps:
                severity = self._value(
                    gap,
                    "severity",
                )
                deficit = self._value(
                    gap,
                    "deficit",
                )
                expected = max(
                    self._value(
                        gap,
                        "expected",
                    ),
                    1.0,
                )

                pressure += (
                    severity
                    + deficit / expected
                )

            return min(
                1.0,
                pressure / 32.0,
            )

        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Freshness pressure
    # ------------------------------------------------------------------

    def _freshness_pressure(
        self,
        partition_id: int,
    ) -> float:
        freshness = self.freshness

        if freshness is None:
            return 0.0

        stale_method = getattr(
            freshness,
            "stale_partitions",
            None,
        )

        if stale_method is None:
            return 0.0

        try:
            stale = {
                int(item)
                for item in stale_method(
                    limit=10_000
                )
            }

            return (
                1.0
                if partition_id in stale
                else 0.0
            )

        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Signal collection
    # ------------------------------------------------------------------

    def observe(
        self,
        partition_id: int,
        now: Optional[float] = None,
    ) -> AdaptiveSignal:
        if now is None:
            now = time.time()

        state = self._coverage_state(
            partition_id
        )

        discovered = max(
            0.0,
            self._value(
                state,
                "discovered",
            ),
        )

        activated = max(
            0.0,
            self._value(
                state,
                "activated",
            ),
        )

        completed = max(
            0.0,
            self._value(
                state,
                "completed",
            ),
        )

        failures = max(
            0.0,
            self._value(
                state,
                "failures",
            ),
        )

        expected = max(
            self._value(
                state,
                "expected",
            ),
            discovered,
            1.0,
        )

        coverage_ratio = min(
            1.0,
            completed / expected,
        )

        discovery_rate = min(
            1.0,
            discovered / expected,
        )

        activation_rate = min(
            1.0,
            activated / max(
                discovered,
                1.0,
            ),
        )

        completion_rate = min(
            1.0,
            completed / max(
                activated,
                1.0,
            ),
        )

        failure_rate = min(
            1.0,
            failures / max(
                discovered,
                1.0,
            ),
        )

        gap_pressure = (
            self._gap_pressure(
                partition_id
            )
        )

        freshness_pressure = (
            self._freshness_pressure(
                partition_id
            )
        )

        age_pressure = min(
            1.0,
            (
                gap_pressure
                + freshness_pressure
            )
            / 2.0,
        )

        source_pressure = min(
            1.0,
            (
                discovery_rate
                * 0.5
                + activation_rate
                * 0.25
                + completion_rate
                * 0.25
            ),
        )

        signal = AdaptiveSignal(
            partition_id=partition_id,
            coverage_ratio=coverage_ratio,
            discovery_rate=discovery_rate,
            activation_rate=activation_rate,
            completion_rate=completion_rate,
            failure_rate=failure_rate,
            freshness_pressure=freshness_pressure,
            gap_pressure=gap_pressure,
            source_pressure=source_pressure,
            age_pressure=age_pressure,
            observed_at=now,
        )

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO adaptive_signals (
                    partition_id,
                    observed_at,
                    coverage_ratio,
                    discovery_rate,
                    activation_rate,
                    completion_rate,
                    failure_rate,
                    freshness_pressure,
                    gap_pressure,
                    source_pressure,
                    age_pressure
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    signal.partition_id,
                    signal.observed_at,
                    signal.coverage_ratio,
                    signal.discovery_rate,
                    signal.activation_rate,
                    signal.completion_rate,
                    signal.failure_rate,
                    signal.freshness_pressure,
                    signal.gap_pressure,
                    signal.source_pressure,
                    signal.age_pressure,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "signals_observed"
        ] += 1

        return signal

    # ------------------------------------------------------------------
    # Adaptive strategy
    # ------------------------------------------------------------------

    def decide(
        self,
        signal: AdaptiveSignal,
        epoch: Optional[int] = None,
    ) -> AdaptiveDecision:
        if epoch is None:
            epoch = (
                self._epoch
                or self.begin_epoch()
            )

        pressure = (
            (1.0 - signal.coverage_ratio)
            * 0.30
            + (1.0 - signal.discovery_rate)
            * 0.15
            + (1.0 - signal.activation_rate)
            * 0.10
            + (1.0 - signal.completion_rate)
            * 0.10
            + signal.failure_rate
            * 0.15
            + signal.freshness_pressure
            * 0.08
            + signal.gap_pressure
            * 0.10
            + signal.age_pressure
            * 0.02
        )

        pressure = min(
            1.0,
            max(
                0.0,
                pressure,
            ),
        )

        discovery_rate = (
            MIN_DISCOVERY_RATE
            + (
                MAX_DISCOVERY_RATE
                - MIN_DISCOVERY_RATE
            )
            * pressure
        )

        priority_multiplier = (
            1.0
            + pressure * 9.0
        )

        if (
            signal.failure_rate >= 0.75
        ):
            strategy = "protect"
            expansion_depth = 1
            reason = (
                "high failure pressure"
            )

        elif (
            signal.gap_pressure >= 0.75
        ):
            strategy = "aggressive_gap_recovery"
            expansion_depth = 4
            reason = (
                "severe coverage gap"
            )

        elif (
            signal.freshness_pressure >= 0.75
        ):
            strategy = "freshness_recovery"
            expansion_depth = 3
            reason = (
                "high freshness pressure"
            )

        elif (
            signal.coverage_ratio < 0.25
        ):
            strategy = "deep_discovery"
            expansion_depth = 4
            reason = (
                "low completed coverage"
            )

        elif (
            signal.discovery_rate < 0.50
        ):
            strategy = "domain_expansion"
            expansion_depth = 3
            reason = (
                "low discovery coverage"
            )

        elif (
            signal.activation_rate < 0.50
        ):
            strategy = "activation_recovery"
            expansion_depth = 2
            reason = (
                "low activation conversion"
            )

        elif (
            signal.completion_rate < 0.50
        ):
            strategy = "completion_recovery"
            expansion_depth = 2
            reason = (
                "low completion conversion"
            )

        elif pressure < 0.15:
            strategy = "maintenance"
            expansion_depth = 1
            reason = (
                "coverage pressure is low"
            )

        else:
            strategy = "balanced_expansion"
            expansion_depth = 2
            reason = (
                "mixed coverage pressure"
            )

        return AdaptiveDecision(
            partition_id=signal.partition_id,
            discovery_rate=discovery_rate,
            priority_multiplier=priority_multiplier,
            expansion_depth=expansion_depth,
            strategy=strategy,
            reason=reason,
            epoch=epoch,
            created_at=time.time(),
        )

    # ------------------------------------------------------------------
    # Durable decision
    # ------------------------------------------------------------------

    def persist_decision(
        self,
        decision: AdaptiveDecision,
    ) -> None:
        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO adaptive_decisions (
                    partition_id,
                    epoch,
                    discovery_rate,
                    priority_multiplier,
                    expansion_depth,
                    strategy,
                    reason,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    partition_id,
                    epoch
                )
                DO UPDATE SET
                    discovery_rate =
                        excluded.discovery_rate,
                    priority_multiplier =
                        excluded.priority_multiplier,
                    expansion_depth =
                        excluded.expansion_depth,
                    strategy =
                        excluded.strategy,
                    reason =
                        excluded.reason,
                    created_at =
                        excluded.created_at
                """,
                (
                    decision.partition_id,
                    decision.epoch,
                    decision.discovery_rate,
                    decision.priority_multiplier,
                    decision.expansion_depth,
                    decision.strategy,
                    decision.reason,
                    decision.created_at,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "decisions_created"
        ] += 1

    # ------------------------------------------------------------------
    # Work identity
    # ------------------------------------------------------------------

    def work_id_for(
        self,
        partition_id: int,
        strategy: str,
        epoch: int,
    ) -> str:
        raw = (
            f"{partition_id}|"
            f"{strategy}|"
            f"{epoch}"
        )

        return hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

    # ------------------------------------------------------------------
    # Work creation
    # ------------------------------------------------------------------

    def create_work(
        self,
        decision: AdaptiveDecision,
    ) -> Optional[AdaptiveWorkItem]:
        if not self.can_accept():
            return None

        work_id = self.work_id_for(
            decision.partition_id,
            decision.strategy,
            decision.epoch,
        )

        priority = min(
            MAX_PRIORITY,
            max(
                MIN_PRIORITY,
                decision.priority_multiplier
                * (
                    100.0
                    + decision.expansion_depth
                    * 25.0
                ),
            ),
        )

        item = AdaptiveWorkItem(
            work_id=work_id,
            partition_id=decision.partition_id,
            scope="partition",
            scope_key=str(
                decision.partition_id
            ),
            strategy=decision.strategy,
            priority=priority,
            expansion_depth=decision.expansion_depth,
            epoch=decision.epoch,
            created_at=time.time(),
        )

        connection = self._connect()

        try:
            inserted = connection.execute(
                """
                INSERT OR IGNORE INTO
                adaptive_work (
                    work_id,
                    partition_id,
                    scope,
                    scope_key,
                    strategy,
                    priority,
                    expansion_depth,
                    epoch,
                    created_at,
                    status,
                    attempts,
                    worker_id,
                    fencing_token,
                    lease_until,
                    next_attempt_at,
                    last_error
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    'queued', 0,
                    NULL, NULL, NULL, NULL, NULL
                )
                """,
                (
                    item.work_id,
                    item.partition_id,
                    item.scope,
                    item.scope_key,
                    item.strategy,
                    item.priority,
                    item.expansion_depth,
                    item.epoch,
                    item.created_at,
                ),
            ).rowcount

            if inserted == 1:
                self._history(
                    connection,
                    item.work_id,
                    item.partition_id,
                    item.epoch,
                    "created",
                )

            connection.commit()

        finally:
            connection.close()

        if inserted == 1:
            self.stats_data[
                "work_created"
            ] += 1

            return item

        return None

    def _history(
        self,
        connection: sqlite3.Connection,
        work_id: str,
        partition_id: int,
        epoch: int,
        event: str,
        metadata: Optional[
            dict[str, Any]
        ] = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO adaptive_history (
                work_id,
                partition_id,
                epoch,
                event,
                timestamp,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                work_id,
                partition_id,
                epoch,
                event,
                time.time(),
                json.dumps(
                    metadata or {},
                    sort_keys=True,
                    default=str,
                ),
            ),
        )

    # ------------------------------------------------------------------
    # Capacity
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
                    FROM adaptive_work
                    """
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM adaptive_work
                    WHERE status = ?
                    """,
                    (status,),
                ).fetchone()

            return int(row[0])

        finally:
            connection.close()

    def capacity(self) -> AdaptiveCapacity:
        inflight = self.count(
            "processing"
        )

        if self.admission_limit is None:
            return AdaptiveCapacity(
                max_inflight=None,
                inflight=inflight,
                available=None,
                can_accept=True,
            )

        available = max(
            0,
            self.admission_limit - inflight,
        )

        return AdaptiveCapacity(
            max_inflight=self.admission_limit,
            inflight=inflight,
            available=available,
            can_accept=available > 0,
        )

    def can_accept(self) -> bool:
        return self.capacity().can_accept

    # ------------------------------------------------------------------
    # Durable claiming
    # ------------------------------------------------------------------

    def claim(
        self,
        worker_id: str,
        limit: int = 1,
        partition_id: Optional[int] = None,
    ) -> list[AdaptiveWorkItem]:
        if not worker_id:
            raise ValueError(
                "worker_id must not be empty"
            )

        if limit <= 0:
            return []

        if not self.can_accept():
            return []

        now = time.time()

        connection = self._connect()
        claimed: list[
            AdaptiveWorkItem
        ] = []

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            query = """
                SELECT *
                FROM adaptive_work
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
                    created_at ASC,
                    work_id ASC
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
                work_id = str(
                    row["work_id"]
                )

                updated = connection.execute(
                    """
                    UPDATE adaptive_work
                    SET
                        status = 'processing',
                        attempts = attempts + 1,
                        worker_id = ?,
                        fencing_token = ?,
                        lease_until = ?,
                        last_error = NULL
                    WHERE
                        work_id = ?
                        AND status = 'queued'
                    """,
                    (
                        worker_id,
                        fencing_token,
                        lease_until,
                        work_id,
                    ),
                ).rowcount

                if updated != 1:
                    continue

                item = AdaptiveWorkItem(
                    work_id=work_id,
                    partition_id=int(
                        row["partition_id"]
                    ),
                    scope=str(
                        row["scope"]
                    ),
                    scope_key=str(
                        row["scope_key"]
                    ),
                    strategy=str(
                        row["strategy"]
                    ),
                    priority=float(
                        row["priority"]
                    ),
                    expansion_depth=int(
                        row["expansion_depth"]
                    ),
                    epoch=int(
                        row["epoch"]
                    ),
                    created_at=float(
                        row["created_at"]
                    ),
                    status="processing",
                    attempts=int(
                        row["attempts"]
                    ) + 1,
                    worker_id=worker_id,
                    fencing_token=fencing_token,
                    lease_until=lease_until,
                )

                claimed.append(item)

                self._history(
                    connection,
                    work_id,
                    item.partition_id,
                    item.epoch,
                    "claimed",
                    {
                        "worker_id": worker_id,
                        "fencing_token":
                            fencing_token,
                    },
                )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "claims"
        ] += len(claimed)

        return claimed

    # ------------------------------------------------------------------
    # Lease management
    # ------------------------------------------------------------------

    def renew(
        self,
        work: AdaptiveWorkItem,
    ) -> bool:
        lease_until = (
            time.time()
            + self.lease_seconds
        )

        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE adaptive_work
                SET lease_until = ?
                WHERE
                    work_id = ?
                    AND status = 'processing'
                    AND worker_id = ?
                    AND fencing_token = ?
                """,
                (
                    lease_until,
                    work.work_id,
                    work.worker_id,
                    work.fencing_token,
                ),
            ).rowcount

            connection.commit()

            return updated == 1

        finally:
            connection.close()

    def complete(
        self,
        work: AdaptiveWorkItem,
        metadata: Optional[
            dict[str, Any]
        ] = None,
    ) -> bool:
        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE adaptive_work
                SET
                    status = 'completed',
                    worker_id = NULL,
                    fencing_token = NULL,
                    lease_until = NULL,
                    next_attempt_at = NULL,
                    last_error = NULL
                WHERE
                    work_id = ?
                    AND status = 'processing'
                    AND worker_id = ?
                    AND fencing_token = ?
                """,
                (
                    work.work_id,
                    work.worker_id,
                    work.fencing_token,
                ),
            ).rowcount

            if updated == 1:
                self._history(
                    connection,
                    work.work_id,
                    work.partition_id,
                    work.epoch,
                    "completed",
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
        work: AdaptiveWorkItem,
        error: str,
    ) -> bool:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT attempts, epoch
                FROM adaptive_work
                WHERE work_id = ?
                """,
                (work.work_id,),
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
                    time.time()
                    + backoff
                )

            updated = connection.execute(
                """
                UPDATE adaptive_work
                SET
                    status = ?,
                    worker_id = NULL,
                    fencing_token = NULL,
                    lease_until = NULL,
                    next_attempt_at = ?,
                    last_error = ?
                WHERE
                    work_id = ?
                    AND status = 'processing'
                    AND worker_id = ?
                    AND fencing_token = ?
                """,
                (
                    status,
                    next_attempt,
                    str(error)[:4000],
                    work.work_id,
                    work.worker_id,
                    work.fencing_token,
                ),
            ).rowcount

            if updated == 1:
                self._history(
                    connection,
                    work.work_id,
                    work.partition_id,
                    int(row["epoch"]),
                    "failed",
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
    # Recovery
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
                    work_id,
                    partition_id,
                    attempts,
                    epoch
                FROM adaptive_work
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
                    UPDATE adaptive_work
                    SET
                        status = ?,
                        worker_id = NULL,
                        fencing_token = NULL,
                        lease_until = NULL,
                        next_attempt_at = ?,
                        last_error = ?
                    WHERE
                        work_id = ?
                        AND status = 'processing'
                    """,
                    (
                        status,
                        next_attempt,
                        "expired lease recovered",
                        str(row["work_id"]),
                    ),
                ).rowcount

                if updated == 1:
                    recovered += 1

                    self._history(
                        connection,
                        str(row["work_id"]),
                        int(
                            row["partition_id"]
                        ),
                        int(row["epoch"]),
                        "lease_recovered",
                    )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "recoveries"
        ] += recovered

        return recovered

    # ------------------------------------------------------------------
    # Adaptive cycle
    # ------------------------------------------------------------------

    def adapt_partition(
        self,
        partition_id: int,
        epoch: Optional[int] = None,
    ) -> AdaptiveDecision:
        if epoch is None:
            epoch = (
                self._epoch
                or self.begin_epoch()
            )

        signal = self.observe(
            partition_id
        )

        decision = self.decide(
            signal,
            epoch=epoch,
        )

        self.persist_decision(
            decision
        )

        self.create_work(
            decision
        )

        return decision

    def adapt_many(
        self,
        partition_ids: Iterable[int],
        epoch: Optional[int] = None,
    ) -> list[AdaptiveDecision]:
        if epoch is None:
            epoch = (
                self._epoch
                or self.begin_epoch()
            )

        partitions = list(
            dict.fromkeys(
                int(item)
                for item in partition_ids
            )
        )

        decisions: list[
            AdaptiveDecision
        ] = []

        for partition_id in partitions[
            :self.max_batch_size
        ]:
            try:
                decision = (
                    self.adapt_partition(
                        partition_id,
                        epoch=epoch,
                    )
                )

                decisions.append(
                    decision
                )

            except Exception:
                self.stats_data[
                    "runtime_errors"
                ] += 1

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO adaptive_epochs (
                    epoch,
                    started_at,
                    completed_at,
                    partitions_processed,
                    decisions_created,
                    work_created
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(epoch)
                DO UPDATE SET
                    completed_at =
                        excluded.completed_at,
                    partitions_processed =
                        excluded.partitions_processed,
                    decisions_created =
                        excluded.decisions_created,
                    work_created =
                        excluded.work_created
                """,
                (
                    epoch,
                    time.time(),
                    time.time(),
                    len(partitions),
                    len(decisions),
                    len(decisions),
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self._last_adaptation_at = (
            time.time()
        )

        return decisions

    # ------------------------------------------------------------------
    # Work retrieval
    # ------------------------------------------------------------------

    def list_work(
        self,
        status: Optional[str] = None,
        partition_id: Optional[int] = None,
        limit: int = 100,
    ) -> list[AdaptiveWorkItem]:
        if limit <= 0:
            return []

        connection = self._connect()

        try:
            query = """
                SELECT *
                FROM adaptive_work
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
                    created_at ASC,
                    work_id ASC
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
            AdaptiveWorkItem(
                work_id=str(
                    row["work_id"]
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
                strategy=str(
                    row["strategy"]
                ),
                priority=float(
                    row["priority"]
                ),
                expansion_depth=int(
                    row["expansion_depth"]
                ),
                epoch=int(
                    row["epoch"]
                ),
                created_at=float(
                    row["created_at"]
                ),
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

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def checkpoint(
        self,
        controller_id: str = "global",
        partition_cursor: int = 0,
    ) -> None:
        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO adaptive_checkpoints (
                    controller_id,
                    epoch,
                    partition_cursor,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(controller_id)
                DO UPDATE SET
                    epoch = excluded.epoch,
                    partition_cursor =
                        excluded.partition_cursor,
                    updated_at =
                        excluded.updated_at
                """,
                (
                    controller_id,
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
        controller_id: str = "global",
    ) -> Optional[
        dict[str, Any]
    ]:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT
                    controller_id,
                    epoch,
                    partition_cursor,
                    updated_at
                FROM adaptive_checkpoints
                WHERE controller_id = ?
                """,
                (controller_id,),
            ).fetchone()

        finally:
            connection.close()

        if row is None:
            return None

        return {
            "controller_id": str(
                row["controller_id"]
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
    # Continuous control loop
    # ------------------------------------------------------------------

    def cycle(
        self,
        partitions: Optional[
            Iterable[int]
        ] = None,
        limit: Optional[int] = None,
    ) -> list[AdaptiveDecision]:
        self.recover_expired()

        if partitions is None:
            if limit is None:
                limit = self.max_batch_size

            stale = []

            if self.gap_detector is not None:
                try:
                    stale = [
                        int(item)
                        for item in
                        self.gap_detector.stale_partitions(
                            limit=limit
                        )
                    ]
                except Exception:
                    stale = []

            if not stale:
                stale = list(
                    range(
                        min(
                            self.partition_count,
                            limit,
                        )
                    )
                )

            partitions = stale

        return self.adapt_many(
            partitions
        )

    def run(
        self,
        interval: Optional[float] = None,
    ) -> None:
        if interval is None:
            interval = (
                self.adaptation_interval
            )

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
            "last_adaptation_at":
                self._last_adaptation_at,
            "queued": self.count(
                "queued"
            ),
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
    "AdaptiveSignal",
    "AdaptiveDecision",
    "AdaptiveWorkItem",
    "AdaptiveCapacity",
    "AdaptiveDiscoveryExpansion",
]
