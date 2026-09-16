"""
OUR SEARCH — Global Web Coverage Expansion
Brick 8.7 — Final Global Coverage Architecture

Final Phase-8 integration layer.

Architecture:

                         PUBLIC WEB
                             │
                 ┌───────────┴───────────┐
                 │   GLOBAL DISCOVERY    │
                 └───────────┬───────────┘
                             ↓
                  DOMAIN / URL UNIVERSE
                             ↓
              ┌──────── COVERAGE ────────┐
              │                          │
              ↓                          ↓
        GAP DETECTION              FRESHNESS
              │                          │
              └──────────┬───────────────┘
                         ↓
               ADAPTIVE DISCOVERY
                         ↓
                DURABLE WORK FABRIC
                         ↓
             PLACEMENT / SCHEDULING
                         ↓
                      WORKERS
                         ↓
                  EXISTING CRAWLER
                         ↓
                  CRAWL RESULTS
                         ↓
          COVERAGE / FRESHNESS SIGNALS
                         ↺

This component is an architecture-level orchestrator.
It does not implement crawling itself.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from typing import Any, Iterable, Optional


VERSION = "final-global-coverage-architecture.v1"

DEFAULT_PARTITION_COUNT = 1_048_576
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_LEASE_SECONDS = 300.0
DEFAULT_MAX_ATTEMPTS = 8


@dataclass(frozen=True)
class CoverageArchitectureSignal:
    partition_id: int
    coverage: float
    freshness: float
    gap_pressure: float
    adaptive_pressure: float
    observed_at: float


@dataclass(frozen=True)
class CoverageArchitectureDecision:
    partition_id: int
    strategy: str
    priority: float
    expansion_depth: int
    recrawl_required: bool
    discovery_required: bool
    epoch: int
    created_at: float


@dataclass(frozen=True)
class CoverageArchitectureWork:
    work_id: str
    partition_id: int
    strategy: str
    priority: float
    expansion_depth: int
    recrawl_required: bool
    discovery_required: bool
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
class CoverageArchitectureResult:
    epoch: int
    partitions_processed: int
    signals_created: int
    decisions_created: int
    work_created: int
    recovered_work: int
    queued: int
    processing: int
    completed: int
    failed: int


@dataclass(frozen=True)
class CoverageArchitectureCapacity:
    max_inflight: Optional[int]
    inflight: int
    available: Optional[int]
    can_accept: bool


class FinalGlobalCoverageArchitecture:
    """
    Final Phase-8 orchestration/control-plane layer.

    Responsibilities:

        8.1 Coverage Control Plane
        8.2 Continuous Domain Expansion
        8.3 URL Universe Expansion
        8.4 Recrawl / Freshness
        8.5 Coverage Gap Detection
        8.6 Adaptive Discovery
        8.7 Final architecture integration

    The implementation is deliberately interface-driven so each existing
    subsystem can evolve independently.
    """

    def __init__(
        self,
        storage_root: str,
        coverage_control_plane: Any = None,
        domain_expansion: Any = None,
        url_expansion: Any = None,
        freshness: Any = None,
        gap_detection: Any = None,
        adaptive_discovery: Any = None,
        work_queue: Any = None,
        work_router: Any = None,
        crawler: Any = None,
        partition_count: int = DEFAULT_PARTITION_COUNT,
        max_batch_size: int = DEFAULT_BATCH_SIZE,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        admission_limit: Optional[int] = None,
        cycle_interval: float = 300.0,
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

        if cycle_interval <= 0:
            raise ValueError(
                "cycle_interval must be positive"
            )

        self.storage_root = storage_root

        self.coverage_control_plane = (
            coverage_control_plane
        )
        self.domain_expansion = domain_expansion
        self.url_expansion = url_expansion
        self.freshness = freshness
        self.gap_detection = gap_detection
        self.adaptive_discovery = (
            adaptive_discovery
        )
        self.work_queue = work_queue
        self.work_router = work_router
        self.crawler = crawler

        self.partition_count = partition_count
        self.max_batch_size = max_batch_size
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.admission_limit = admission_limit
        self.cycle_interval = cycle_interval

        self.db_path = (
            f"{storage_root}/"
            "final_global_coverage_architecture.db"
        )

        self._lock = threading.RLock()
        self._running = False
        self._epoch = 0
        self._last_cycle_at: Optional[float] = None

        self.stats_data = {
            "version": VERSION,
            "cycles": 0,
            "epochs": 0,
            "partitions_processed": 0,
            "signals_created": 0,
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

    # ================================================================
    # Durable database
    # ================================================================

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
                architecture_signals (
                    partition_id INTEGER NOT NULL,
                    observed_at REAL NOT NULL,
                    coverage REAL NOT NULL,
                    freshness REAL NOT NULL,
                    gap_pressure REAL NOT NULL,
                    adaptive_pressure REAL NOT NULL,
                    PRIMARY KEY (
                        partition_id,
                        observed_at
                    )
                );

                CREATE INDEX IF NOT EXISTS
                idx_architecture_signals_partition
                ON architecture_signals(
                    partition_id,
                    observed_at DESC
                );

                CREATE TABLE IF NOT EXISTS
                architecture_decisions (
                    partition_id INTEGER NOT NULL,
                    epoch INTEGER NOT NULL,
                    strategy TEXT NOT NULL,
                    priority REAL NOT NULL,
                    expansion_depth INTEGER NOT NULL,
                    recrawl_required INTEGER NOT NULL,
                    discovery_required INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (
                        partition_id,
                        epoch
                    )
                );

                CREATE INDEX IF NOT EXISTS
                idx_architecture_decisions_priority
                ON architecture_decisions(
                    priority DESC,
                    created_at
                );

                CREATE TABLE IF NOT EXISTS
                architecture_work (
                    work_id TEXT PRIMARY KEY,
                    partition_id INTEGER NOT NULL,
                    strategy TEXT NOT NULL,
                    priority REAL NOT NULL,
                    expansion_depth INTEGER NOT NULL,
                    recrawl_required INTEGER NOT NULL,
                    discovery_required INTEGER NOT NULL,
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
                idx_architecture_work_ready
                ON architecture_work(
                    status,
                    next_attempt_at,
                    priority DESC,
                    created_at
                );

                CREATE INDEX IF NOT EXISTS
                idx_architecture_work_partition
                ON architecture_work(
                    partition_id,
                    status
                );

                CREATE INDEX IF NOT EXISTS
                idx_architecture_work_worker
                ON architecture_work(
                    worker_id,
                    status
                );

                CREATE TABLE IF NOT EXISTS
                architecture_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    work_id TEXT,
                    partition_id INTEGER,
                    epoch INTEGER,
                    event TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    metadata_json TEXT
                );

                CREATE INDEX IF NOT EXISTS
                idx_architecture_history_work
                ON architecture_history(
                    work_id,
                    timestamp
                );

                CREATE TABLE IF NOT EXISTS
                architecture_checkpoints (
                    controller_id TEXT PRIMARY KEY,
                    epoch INTEGER NOT NULL,
                    partition_cursor INTEGER NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS
                architecture_epochs (
                    epoch INTEGER PRIMARY KEY,
                    started_at REAL NOT NULL,
                    completed_at REAL,
                    partitions_processed INTEGER NOT NULL DEFAULT 0,
                    signals_created INTEGER NOT NULL DEFAULT 0,
                    decisions_created INTEGER NOT NULL DEFAULT 0,
                    work_created INTEGER NOT NULL DEFAULT 0
                );
                """
            )

            connection.commit()

        finally:
            connection.close()

    # ================================================================
    # Deterministic partitioning
    # ================================================================

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

    # ================================================================
    # Epoch management
    # ================================================================

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
                architecture_epochs (
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

    # ================================================================
    # Generic subsystem access
    # ================================================================

    def _value(
        self,
        obj: Any,
        name: str,
        default: float = 0.0,
    ) -> float:
        if obj is None:
            return default

        if isinstance(obj, dict):
            value = obj.get(
                name,
                default,
            )
        else:
            value = getattr(
                obj,
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

    def _call(
        self,
        obj: Any,
        method_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if obj is None:
            return None

        method = getattr(
            obj,
            method_name,
            None,
        )

        if method is None:
            return None

        try:
            return method(
                *args,
                **kwargs,
            )
        except Exception:
            return None

    # ================================================================
    # Coverage signal
    # ================================================================

    def _coverage_signal(
        self,
        partition_id: int,
    ) -> float:
        state = self._call(
            self.coverage_control_plane,
            "get",
            partition_id,
        )

        if state is None:
            return 0.0

        completed = max(
            0.0,
            self._value(
                state,
                "completed",
            ),
        )

        expected = max(
            1.0,
            self._value(
                state,
                "expected",
            ),
        )

        discovered = max(
            0.0,
            self._value(
                state,
                "discovered",
            ),
        )

        if expected < discovered:
            expected = discovered

        return min(
            1.0,
            completed / expected,
        )

    # ================================================================
    # Freshness signal
    # ================================================================

    def _freshness_signal(
        self,
        partition_id: int,
    ) -> float:
        freshness = self.freshness

        if freshness is None:
            return 0.0

        stale = self._call(
            freshness,
            "stale_partitions",
            limit=self.max_batch_size,
        )

        if stale is None:
            return 0.0

        try:
            stale_set = {
                int(item)
                for item in stale
            }
        except Exception:
            return 0.0

        return (
            1.0
            if partition_id in stale_set
            else 0.0
        )

    # ================================================================
    # Gap signal
    # ================================================================

    def _gap_signal(
        self,
        partition_id: int,
    ) -> float:
        detector = self.gap_detection

        if detector is None:
            return 0.0

        gaps = self._call(
            detector,
            "list_gaps",
            status="queued",
            partition_id=partition_id,
            limit=64,
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
                1.0,
                self._value(
                    gap,
                    "expected",
                ),
            )

            pressure += min(
                1.0,
                severity,
            )

            pressure += min(
                1.0,
                deficit / expected,
            )

        return min(
            1.0,
            pressure / (
                max(len(gaps), 1) * 2.0
            ),
        )

    # ================================================================
    # Adaptive signal
    # ================================================================

    def _adaptive_signal(
        self,
        partition_id: int,
    ) -> float:
        adaptive = (
            self.adaptive_discovery
        )

        if adaptive is None:
            return 0.0

        decisions = self._call(
            adaptive,
            "list_work",
            status="queued",
            partition_id=partition_id,
            limit=32,
        )

        if not decisions:
            return 0.0

        total = 0.0

        for item in decisions:
            priority = self._value(
                item,
                "priority",
            )

            total += min(
                1.0,
                priority / 1000.0,
            )

        return min(
            1.0,
            total / max(
                len(decisions),
                1,
            ),
        )

    # ================================================================
    # Observe complete architecture
    # ================================================================

    def observe(
        self,
        partition_id: int,
    ) -> CoverageArchitectureSignal:
        now = time.time()

        coverage = (
            self._coverage_signal(
                partition_id
            )
        )

        freshness = (
            self._freshness_signal(
                partition_id
            )
        )

        gap_pressure = (
            self._gap_signal(
                partition_id
            )
        )

        adaptive_pressure = (
            self._adaptive_signal(
                partition_id
            )
        )

        signal = CoverageArchitectureSignal(
            partition_id=partition_id,
            coverage=coverage,
            freshness=freshness,
            gap_pressure=gap_pressure,
            adaptive_pressure=adaptive_pressure,
            observed_at=now,
        )

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO architecture_signals (
                    partition_id,
                    observed_at,
                    coverage,
                    freshness,
                    gap_pressure,
                    adaptive_pressure
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.partition_id,
                    signal.observed_at,
                    signal.coverage,
                    signal.freshness,
                    signal.gap_pressure,
                    signal.adaptive_pressure,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "signals_created"
        ] += 1

        return signal

    # ================================================================
    # Decision engine
    # ================================================================

    def decide(
        self,
        signal: CoverageArchitectureSignal,
        epoch: int,
    ) -> CoverageArchitectureDecision:
        coverage_deficit = (
            1.0 - signal.coverage
        )

        pressure = (
            coverage_deficit * 0.35
            + signal.freshness * 0.20
            + signal.gap_pressure * 0.30
            + signal.adaptive_pressure * 0.15
        )

        pressure = min(
            1.0,
            max(
                0.0,
                pressure,
            ),
        )

        priority = (
            100.0
            + pressure * 900.0
        )

        discovery_required = (
            coverage_deficit >= 0.20
            or signal.gap_pressure >= 0.35
        )

        recrawl_required = (
            signal.freshness >= 0.50
        )

        if signal.gap_pressure >= 0.75:
            strategy = (
                "coverage_gap_recovery"
            )
            expansion_depth = 4

        elif signal.freshness >= 0.75:
            strategy = (
                "freshness_recovery"
            )
            expansion_depth = 3

        elif coverage_deficit >= 0.75:
            strategy = (
                "deep_web_expansion"
            )
            expansion_depth = 4

        elif coverage_deficit >= 0.50:
            strategy = (
                "domain_and_url_expansion"
            )
            expansion_depth = 3

        elif discovery_required:
            strategy = (
                "adaptive_discovery"
            )
            expansion_depth = 2

        elif recrawl_required:
            strategy = (
                "adaptive_recrawl"
            )
            expansion_depth = 2

        else:
            strategy = (
                "coverage_maintenance"
            )
            expansion_depth = 1

        return CoverageArchitectureDecision(
            partition_id=signal.partition_id,
            strategy=strategy,
            priority=priority,
            expansion_depth=expansion_depth,
            recrawl_required=recrawl_required,
            discovery_required=discovery_required,
            epoch=epoch,
            created_at=time.time(),
        )

    # ================================================================
    # Durable decisions
    # ================================================================

    def persist_decision(
        self,
        decision: CoverageArchitectureDecision,
    ) -> None:
        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO architecture_decisions (
                    partition_id,
                    epoch,
                    strategy,
                    priority,
                    expansion_depth,
                    recrawl_required,
                    discovery_required,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    partition_id,
                    epoch
                )
                DO UPDATE SET
                    strategy =
                        excluded.strategy,
                    priority =
                        excluded.priority,
                    expansion_depth =
                        excluded.expansion_depth,
                    recrawl_required =
                        excluded.recrawl_required,
                    discovery_required =
                        excluded.discovery_required,
                    created_at =
                        excluded.created_at
                """,
                (
                    decision.partition_id,
                    decision.epoch,
                    decision.strategy,
                    decision.priority,
                    decision.expansion_depth,
                    int(
                        decision.recrawl_required
                    ),
                    int(
                        decision.discovery_required
                    ),
                    decision.created_at,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "decisions_created"
        ] += 1

    # ================================================================
    # Work identity
    # ================================================================

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

    # ================================================================
    # Durable work creation
    # ================================================================

    def create_work(
        self,
        decision: CoverageArchitectureDecision,
    ) -> Optional[
        CoverageArchitectureWork
    ]:
        if not self.can_accept():
            return None

        work_id = self.work_id_for(
            decision.partition_id,
            decision.strategy,
            decision.epoch,
        )

        connection = self._connect()

        try:
            created_at = time.time()

            inserted = connection.execute(
                """
                INSERT OR IGNORE INTO
                architecture_work (
                    work_id,
                    partition_id,
                    strategy,
                    priority,
                    expansion_depth,
                    recrawl_required,
                    discovery_required,
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
                    work_id,
                    decision.partition_id,
                    decision.strategy,
                    decision.priority,
                    decision.expansion_depth,
                    int(
                        decision.recrawl_required
                    ),
                    int(
                        decision.discovery_required
                    ),
                    decision.epoch,
                    created_at,
                ),
            ).rowcount

            if inserted == 1:
                self._history(
                    connection,
                    work_id,
                    decision.partition_id,
                    decision.epoch,
                    "created",
                )

            connection.commit()

        finally:
            connection.close()

        if inserted != 1:
            return None

        self.stats_data[
            "work_created"
        ] += 1

        return CoverageArchitectureWork(
            work_id=work_id,
            partition_id=decision.partition_id,
            strategy=decision.strategy,
            priority=decision.priority,
            expansion_depth=decision.expansion_depth,
            recrawl_required=(
                decision.recrawl_required
            ),
            discovery_required=(
                decision.discovery_required
            ),
            epoch=decision.epoch,
            created_at=created_at,
        )

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
            INSERT INTO architecture_history (
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

    # ================================================================
    # Capacity / backpressure
    # ================================================================

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
                    FROM architecture_work
                    """
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM architecture_work
                    WHERE status = ?
                    """,
                    (status,),
                ).fetchone()

            return int(row[0])

        finally:
            connection.close()

    def capacity(
        self,
    ) -> CoverageArchitectureCapacity:
        inflight = self.count(
            "processing"
        )

        if self.admission_limit is None:
            return CoverageArchitectureCapacity(
                max_inflight=None,
                inflight=inflight,
                available=None,
                can_accept=True,
            )

        available = max(
            0,
            self.admission_limit - inflight,
        )

        return CoverageArchitectureCapacity(
            max_inflight=self.admission_limit,
            inflight=inflight,
            available=available,
            can_accept=available > 0,
        )

    def can_accept(self) -> bool:
        return self.capacity().can_accept

    # ================================================================
    # Work claiming
    # ================================================================

    def claim(
        self,
        worker_id: str,
        limit: int = 1,
        partition_id: Optional[int] = None,
    ) -> list[
        CoverageArchitectureWork
    ]:
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
            CoverageArchitectureWork
        ] = []

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            query = """
                SELECT *
                FROM architecture_work
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
                    UPDATE architecture_work
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

                attempts = (
                    int(row["attempts"])
                    + 1
                )

                item = (
                    CoverageArchitectureWork(
                        work_id=work_id,
                        partition_id=int(
                            row["partition_id"]
                        ),
                        strategy=str(
                            row["strategy"]
                        ),
                        priority=float(
                            row["priority"]
                        ),
                        expansion_depth=int(
                            row[
                                "expansion_depth"
                            ]
                        ),
                        recrawl_required=bool(
                            row[
                                "recrawl_required"
                            ]
                        ),
                        discovery_required=bool(
                            row[
                                "discovery_required"
                            ]
                        ),
                        epoch=int(
                            row["epoch"]
                        ),
                        created_at=float(
                            row["created_at"]
                        ),
                        status="processing",
                        attempts=attempts,
                        worker_id=worker_id,
                        fencing_token=(
                            fencing_token
                        ),
                        lease_until=(
                            lease_until
                        ),
                    )
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

    # ================================================================
    # Lease renewal
    # ================================================================

    def renew(
        self,
        work: CoverageArchitectureWork,
    ) -> bool:
        lease_until = (
            time.time()
            + self.lease_seconds
        )

        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE architecture_work
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

    # ================================================================
    # Completion
    # ================================================================

    def complete(
        self,
        work: CoverageArchitectureWork,
        metadata: Optional[
            dict[str, Any]
        ] = None,
    ) -> bool:
        connection = self._connect()

        try:
            updated = connection.execute(
                """
                UPDATE architecture_work
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

        if updated != 1:
            return False

        self.stats_data[
            "completions"
        ] += 1

        return True

    # ================================================================
    # Failure
    # ================================================================

    def fail(
        self,
        work: CoverageArchitectureWork,
        error: str,
    ) -> bool:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT attempts, epoch
                FROM architecture_work
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
                UPDATE architecture_work
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

        if updated != 1:
            return False

        self.stats_data[
            "failures"
        ] += 1

        return True

    # ================================================================
    # Recovery
    # ================================================================

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
                FROM architecture_work
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
                    UPDATE architecture_work
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

    # ================================================================
    # Downstream integration
    # ================================================================

    def _forward_to_subsystem(
        self,
        work: CoverageArchitectureWork,
    ) -> bool:
        """
        Forward adaptive intent to existing subsystems when they expose
        compatible interfaces.

        The architecture remains operationally decoupled: absence of a
        particular downstream adapter does not invalidate the durable
        control-plane record.
        """

        payload = {
            "partition_id": work.partition_id,
            "strategy": work.strategy,
            "priority": work.priority,
            "expansion_depth": (
                work.expansion_depth
            ),
            "recrawl_required": (
                work.recrawl_required
            ),
            "discovery_required": (
                work.discovery_required
            ),
            "epoch": work.epoch,
            "work_id": work.work_id,
        }

        forwarded = False

        targets = []

        if work.discovery_required:
            targets.extend(
                [
                    self.adaptive_discovery,
                    self.domain_expansion,
                    self.url_expansion,
                    self.work_router,
                    self.work_queue,
                ]
            )

        if work.recrawl_required:
            targets.append(
                self.freshness
            )

        for target in targets:
            if target is None:
                continue

            for method_name in (
                "accept_adaptive_work",
                "accept_work",
                "enqueue_adaptive",
                "enqueue",
            ):
                method = getattr(
                    target,
                    method_name,
                    None,
                )

                if method is None:
                    continue

                try:
                    result = method(
                        payload
                    )

                    if result is not False:
                        forwarded = True
                        break

                except TypeError:
                    continue

                except Exception:
                    self.stats_data[
                        "runtime_errors"
                    ] += 1
                    continue

            if forwarded:
                break

        return forwarded

    # ================================================================
    # Architecture cycle
    # ================================================================

    def cycle(
        self,
        partitions: Optional[
            Iterable[int]
        ] = None,
        limit: Optional[int] = None,
    ) -> CoverageArchitectureResult:
        if limit is None:
            limit = self.max_batch_size

        if limit <= 0:
            limit = self.max_batch_size

        recovered = (
            self.recover_expired()
        )

        epoch = (
            self._epoch
            or self.begin_epoch()
        )

        if partitions is None:
            candidate_partitions: list[int] = []

            # Prefer coverage-control-plane active partitions.
            active = self._call(
                self.coverage_control_plane,
                "active_partitions",
            )

            if active is not None:
                try:
                    candidate_partitions.extend(
                        int(item)
                        for item in active
                    )
                except Exception:
                    pass

            # Then include gap detector partitions.
            if not candidate_partitions:
                stale = self._call(
                    self.gap_detection,
                    "stale_partitions",
                    limit=limit,
                )

                if stale is not None:
                    try:
                        candidate_partitions.extend(
                            int(item)
                            for item in stale
                        )
                    except Exception:
                        pass

            # Deterministic bounded cursor fallback.
            if not candidate_partitions:
                candidate_partitions = list(
                    range(
                        min(
                            self.partition_count,
                            limit,
                        )
                    )
                )

            partitions = candidate_partitions

        unique_partitions = list(
            dict.fromkeys(
                int(item)
                for item in partitions
            )
        )[:limit]

        decisions = 0
        work_created = 0

        for partition_id in unique_partitions:
            try:
                signal = self.observe(
                    partition_id
                )

                decision = self.decide(
                    signal,
                    epoch,
                )

                self.persist_decision(
                    decision
                )

                decisions += 1

                work = self.create_work(
                    decision
                )

                if work is not None:
                    work_created += 1

            except Exception:
                self.stats_data[
                    "runtime_errors"
                ] += 1

        now = time.time()

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO architecture_epochs (
                    epoch,
                    started_at,
                    completed_at,
                    partitions_processed,
                    signals_created,
                    decisions_created,
                    work_created
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(epoch)
                DO UPDATE SET
                    completed_at =
                        excluded.completed_at,
                    partitions_processed =
                        excluded.partitions_processed,
                    signals_created =
                        excluded.signals_created,
                    decisions_created =
                        excluded.decisions_created,
                    work_created =
                        excluded.work_created
                """,
                (
                    epoch,
                    now,
                    now,
                    len(unique_partitions),
                    len(unique_partitions),
                    decisions,
                    work_created,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        self.stats_data[
            "cycles"
        ] += 1

        self.stats_data[
            "partitions_processed"
        ] += len(unique_partitions)

        self._last_cycle_at = now

        return CoverageArchitectureResult(
            epoch=epoch,
            partitions_processed=(
                len(unique_partitions)
            ),
            signals_created=(
                len(unique_partitions)
            ),
            decisions_created=decisions,
            work_created=work_created,
            recovered_work=recovered,
            queued=self.count("queued"),
            processing=self.count(
                "processing"
            ),
            completed=self.count(
                "completed"
            ),
            failed=self.count(
                "failed"
            ),
        )

    # ================================================================
    # Continuous operation
    # ================================================================

    def run(
        self,
        interval: Optional[float] = None,
    ) -> None:
        if interval is None:
            interval = self.cycle_interval

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

    # ================================================================
    # Checkpoints
    # ================================================================

    def checkpoint(
        self,
        controller_id: str = "global",
        partition_cursor: int = 0,
    ) -> None:
        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO architecture_checkpoints (
                    controller_id,
                    epoch,
                    partition_cursor,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(controller_id)
                DO UPDATE SET
                    epoch =
                        excluded.epoch,
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
                FROM architecture_checkpoints
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

    # ================================================================
    # Work inspection
    # ================================================================

    def list_work(
        self,
        status: Optional[str] = None,
        partition_id: Optional[int] = None,
        limit: int = 100,
    ) -> list[
        CoverageArchitectureWork
    ]:
        if limit <= 0:
            return []

        connection = self._connect()

        try:
            query = """
                SELECT *
                FROM architecture_work
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
            CoverageArchitectureWork(
                work_id=str(
                    row["work_id"]
                ),
                partition_id=int(
                    row["partition_id"]
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
                recrawl_required=bool(
                    row[
                        "recrawl_required"
                    ]
                ),
                discovery_required=bool(
                    row[
                        "discovery_required"
                    ]
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

    # ================================================================
    # Final architecture status
    # ================================================================

    def stats(self) -> dict[str, Any]:
        capacity = self.capacity()

        return {
            **self.stats_data,
            "epoch": self._epoch,
            "last_cycle_at": (
                self._last_cycle_at
            ),
            "partition_count": (
                self.partition_count
            ),
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
            "phase8_complete": True,
        }


__all__ = [
    "VERSION",
    "CoverageArchitectureSignal",
    "CoverageArchitectureDecision",
    "CoverageArchitectureWork",
    "CoverageArchitectureResult",
    "CoverageArchitectureCapacity",
    "FinalGlobalCoverageArchitecture",
]
