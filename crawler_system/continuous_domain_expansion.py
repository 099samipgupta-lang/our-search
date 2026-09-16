"""
OUR SEARCH — Continuous Domain Expansion

Phase 8 / Brick 8.2

Continuously converts durable domain discoveries into additional
public-Web coverage work.

Architecture:

    Global Discovery
          ↓
    Coverage Control Plane
          ↓
    Domain Expansion
          ↓
    Candidate Generation
          ↓
    Normalization / Validation / Dedup
          ↓
    Durable Discovery Work Queue
          ↓
    Placement-Aware Router
          ↓
    Worker Supervisor
          ↓
    Whole-Web Crawler

Design goals:

- continuous operation
- durable progress
- deterministic partitioning
- bounded batches
- backpressure awareness
- idempotent expansion
- retry-safe processing
- failure isolation
- restart-safe cursors
- no fixed small-web execution ceiling
- compatible with existing OUR SEARCH discovery architecture
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional


VERSION = "continuous-domain-expansion.v1"


@dataclass(frozen=True)
class DomainExpansionTask:
    hostname: str
    source: str
    priority: float = 50.0
    discovered_at: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def stable_id(self) -> str:
        payload = (
            self.hostname.strip().lower(),
            self.source.strip().lower(),
        )

        return hashlib.sha256(
            "|".join(payload).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class ExpansionResult:
    task_id: str
    hostname: str
    source: str
    generated_urls: tuple[str, ...]
    generated_domains: tuple[str, ...]
    accepted: int
    rejected: int
    duplicate: int
    failed: bool = False
    error: Optional[str] = None


@dataclass(frozen=True)
class ExpansionCapacity:
    queued: int
    processing: int
    completed: int
    failed: int
    max_batch_size: int
    admission_limit: Optional[int]
    admission_available: bool


class ContinuousDomainExpansion:
    """
    Durable continuous domain-expansion controller.

    This component intentionally does not own crawler execution.
    It produces durable expansion work for the existing discovery
    queue/router/worker architecture.
    """

    def __init__(
        self,
        storage_root: str,
        work_queue: Any,
        work_router: Any = None,
        coverage_control_plane: Any = None,
        max_batch_size: int = 10000,
        admission_limit: Optional[int] = None,
        expansion_interval: float = 5.0,
        max_attempts: int = 8,
    ):
        if not storage_root:
            raise ValueError("storage_root is required")

        if max_batch_size < 1:
            raise ValueError(
                "max_batch_size must be >= 1"
            )

        if admission_limit is not None and admission_limit < 1:
            raise ValueError(
                "admission_limit must be >= 1"
            )

        if expansion_interval < 0:
            raise ValueError(
                "expansion_interval must be >= 0"
            )

        if max_attempts < 1:
            raise ValueError(
                "max_attempts must be >= 1"
            )

        self.storage_root = storage_root
        self.work_queue = work_queue
        self.work_router = work_router
        self.coverage_control_plane = (
            coverage_control_plane
        )

        self.max_batch_size = max_batch_size
        self.admission_limit = admission_limit
        self.expansion_interval = expansion_interval
        self.max_attempts = max_attempts

        self.db_path = (
            f"{storage_root}/continuous_domain_expansion.db"
        )

        self._lock = threading.RLock()
        self._running = False
        self._last_cycle_at = 0.0
        self._cycle_count = 0
        self._last_error: Optional[str] = None

        self._init_db()

    # ------------------------------------------------------------------
    # Durable state
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
            timeout=30,
            check_same_thread=False,
        )

        connection.row_factory = sqlite3.Row

        connection.execute(
            "PRAGMA journal_mode=WAL"
        )

        connection.execute(
            "PRAGMA synchronous=NORMAL"
        )

        return connection

    def _init_db(self) -> None:
        connection = self._connect()

        try:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS expansion_tasks (
                    task_id TEXT PRIMARY KEY,
                    hostname TEXT NOT NULL,
                    source TEXT NOT NULL,
                    priority REAL NOT NULL DEFAULT 50.0,
                    discovered_at REAL NOT NULL,
                    metadata_json TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    available_at REAL NOT NULL,
                    worker_id TEXT,
                    lease_until REAL,
                    fencing_token INTEGER,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    completed_at REAL,
                    failed_at REAL,
                    last_error TEXT
                );

                CREATE INDEX IF NOT EXISTS
                idx_expansion_tasks_ready
                ON expansion_tasks(
                    status,
                    available_at,
                    priority DESC,
                    task_id
                );

                CREATE INDEX IF NOT EXISTS
                idx_expansion_tasks_hostname
                ON expansion_tasks(hostname);

                CREATE INDEX IF NOT EXISTS
                idx_expansion_tasks_worker
                ON expansion_tasks(worker_id);

                CREATE TABLE IF NOT EXISTS expansion_outputs (
                    output_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    output_type TEXT NOT NULL,
                    value TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    UNIQUE(task_id, output_type, value)
                );

                CREATE INDEX IF NOT EXISTS
                idx_expansion_outputs_task
                ON expansion_outputs(task_id);

                CREATE TABLE IF NOT EXISTS expansion_checkpoints (
                    checkpoint_key TEXT PRIMARY KEY,
                    checkpoint_value TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS expansion_epochs (
                    epoch_id TEXT PRIMARY KEY,
                    started_at REAL NOT NULL,
                    completed_at REAL,
                    status TEXT NOT NULL
                );
                """
            )

            connection.commit()

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Deterministic identity
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_hostname(hostname: str) -> str:
        value = str(hostname).strip().lower()

        if value.startswith("*."):
            value = value[2:]

        value = value.rstrip(".")

        return value

    @classmethod
    def task_id_for(
        cls,
        hostname: str,
        source: str,
    ) -> str:
        normalized = cls.normalize_hostname(
            hostname
        )

        payload = (
            normalized
            + "|"
            + str(source).strip().lower()
        )

        return hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()

    # ------------------------------------------------------------------
    # Admission / backpressure
    # ------------------------------------------------------------------

    def _queue_capacity(self) -> Optional[dict[str, Any]]:
        capacity_method = getattr(
            self.work_queue,
            "capacity",
            None,
        )

        if not callable(capacity_method):
            return None

        try:
            result = capacity_method()

            if isinstance(result, dict):
                return result

            return {
                "queued": getattr(
                    result,
                    "queued",
                    0,
                ),
                "processing": getattr(
                    result,
                    "processing",
                    0,
                ),
            }

        except Exception:
            return None

    def can_admit(self, amount: int = 1) -> bool:
        if amount < 1:
            return True

        if self.admission_limit is None:
            return True

        capacity = self._queue_capacity()

        if capacity is None:
            return True

        queued = int(
            capacity.get(
                "queued",
                0,
            )
        )

        processing = int(
            capacity.get(
                "processing",
                0,
            )
        )

        return (
            queued
            + processing
            + amount
            <= self.admission_limit
        )

    # ------------------------------------------------------------------
    # Durable task admission
    # ------------------------------------------------------------------

    def enqueue(
        self,
        task: DomainExpansionTask,
    ) -> bool:
        hostname = self.normalize_hostname(
            task.hostname
        )

        if not hostname:
            return False

        task_id = self.task_id_for(
            hostname,
            task.source,
        )

        now = time.time()

        metadata_json = json.dumps(
            task.metadata or {},
            sort_keys=True,
            separators=(",", ":"),
        )

        connection = self._connect()

        try:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO expansion_tasks (
                    task_id,
                    hostname,
                    source,
                    priority,
                    discovered_at,
                    metadata_json,
                    status,
                    attempts,
                    available_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?)
                """,
                (
                    task_id,
                    hostname,
                    str(task.source),
                    float(task.priority),
                    float(
                        task.discovered_at
                        or now
                    ),
                    metadata_json,
                    now,
                    now,
                    now,
                ),
            )

            connection.commit()

            return cursor.rowcount > 0

        finally:
            connection.close()

    def enqueue_many(
        self,
        tasks: Iterable[DomainExpansionTask],
    ) -> int:
        accepted = 0

        batch = []

        for task in tasks:
            batch.append(task)

            if len(batch) >= self.max_batch_size:
                accepted += self._enqueue_batch(
                    batch
                )
                batch.clear()

        if batch:
            accepted += self._enqueue_batch(
                batch
            )

        return accepted

    def _enqueue_batch(
        self,
        tasks: list[DomainExpansionTask],
    ) -> int:
        if not tasks:
            return 0

        if not self.can_admit(len(tasks)):
            return 0

        now = time.time()
        accepted = 0

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            for task in tasks:
                hostname = self.normalize_hostname(
                    task.hostname
                )

                if not hostname:
                    continue

                task_id = self.task_id_for(
                    hostname,
                    task.source,
                )

                metadata_json = json.dumps(
                    task.metadata or {},
                    sort_keys=True,
                    separators=(",", ":"),
                )

                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO expansion_tasks (
                        task_id,
                        hostname,
                        source,
                        priority,
                        discovered_at,
                        metadata_json,
                        status,
                        attempts,
                        available_at,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?)
                    """,
                    (
                        task_id,
                        hostname,
                        str(task.source),
                        float(task.priority),
                        float(
                            task.discovered_at
                            or now
                        ),
                        metadata_json,
                        now,
                        now,
                        now,
                    ),
                )

                if cursor.rowcount > 0:
                    accepted += 1

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        return accepted

    # ------------------------------------------------------------------
    # Task claiming
    # ------------------------------------------------------------------

    def claim(
        self,
        worker_id: str,
        limit: Optional[int] = None,
        lease_seconds: float = 300.0,
    ) -> list[DomainExpansionTask]:
        if not worker_id:
            raise ValueError(
                "worker_id is required"
            )

        limit = min(
            limit or self.max_batch_size,
            self.max_batch_size,
        )

        now = time.time()
        lease_until = now + lease_seconds

        claimed: list[DomainExpansionTask] = []

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            rows = connection.execute(
                """
                SELECT *
                FROM expansion_tasks
                WHERE status = 'queued'
                  AND available_at <= ?
                ORDER BY
                    priority DESC,
                    available_at ASC,
                    task_id ASC
                LIMIT ?
                """,
                (
                    now,
                    limit,
                ),
            ).fetchall()

            for row in rows:
                attempts = (
                    int(row["attempts"]) + 1
                )

                if attempts > self.max_attempts:
                    connection.execute(
                        """
                        UPDATE expansion_tasks
                        SET status = 'failed',
                            failed_at = ?,
                            updated_at = ?,
                            last_error = ?
                        WHERE task_id = ?
                        """,
                        (
                            now,
                            now,
                            "maximum attempts exceeded",
                            row["task_id"],
                        ),
                    )

                    continue

                fencing_token = (
                    attempts
                )

                connection.execute(
                    """
                    UPDATE expansion_tasks
                    SET status = 'processing',
                        attempts = ?,
                        worker_id = ?,
                        lease_until = ?,
                        fencing_token = ?,
                        updated_at = ?
                    WHERE task_id = ?
                      AND status = 'queued'
                    """,
                    (
                        attempts,
                        worker_id,
                        lease_until,
                        fencing_token,
                        now,
                        row["task_id"],
                    ),
                )

                metadata = {}

                if row["metadata_json"]:
                    try:
                        metadata = json.loads(
                            row["metadata_json"]
                        )
                    except Exception:
                        metadata = {}

                claimed.append(
                    DomainExpansionTask(
                        hostname=row["hostname"],
                        source=row["source"],
                        priority=float(
                            row["priority"]
                        ),
                        discovered_at=float(
                            row["discovered_at"]
                        ),
                        metadata=metadata,
                    )
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        return claimed

    # ------------------------------------------------------------------
    # Expansion generation
    # ------------------------------------------------------------------

    @staticmethod
    def _domain_url(hostname: str) -> str:
        return (
            "https://"
            + hostname
        )

    @staticmethod
    def _www_url(hostname: str) -> str:
        if hostname.startswith("www."):
            return (
                "https://"
                + hostname
            )

        return (
            "https://www."
            + hostname
        )

    @staticmethod
    def _http_url(hostname: str) -> str:
        return (
            "http://"
            + hostname
        )

    @staticmethod
    def _well_known_urls(
        hostname: str,
    ) -> tuple[str, ...]:
        return (
            "https://"
            + hostname
            + "/robots.txt",
            "https://"
            + hostname
            + "/sitemap.xml",
        )

    def generate_urls(
        self,
        task: DomainExpansionTask,
    ) -> tuple[str, ...]:
        hostname = self.normalize_hostname(
            task.hostname
        )

        if not hostname:
            return ()

        candidates = [
            self._domain_url(hostname),
            self._http_url(hostname),
            *self._well_known_urls(hostname),
        ]

        metadata = task.metadata or {}

        explicit_urls = metadata.get(
            "urls"
        )

        if isinstance(
            explicit_urls,
            (list, tuple, set),
        ):
            candidates.extend(
                str(value)
                for value in explicit_urls
                if value
            )

        aliases = metadata.get(
            "aliases"
        )

        if isinstance(
            aliases,
            (list, tuple, set),
        ):
            for alias in aliases:
                alias_hostname = (
                    self.normalize_hostname(
                        str(alias)
                    )
                )

                if alias_hostname:
                    candidates.append(
                        self._domain_url(
                            alias_hostname
                        )
                    )

        unique = sorted(
            {
                value.strip()
                for value in candidates
                if value
            }
        )

        return tuple(unique)

    # ------------------------------------------------------------------
    # Durable outputs
    # ------------------------------------------------------------------

    def _record_output(
        self,
        task_id: str,
        output_type: str,
        value: str,
    ) -> bool:
        now = time.time()

        output_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                task_id
                + "|"
                + output_type
                + "|"
                + value,
            )
        )

        connection = self._connect()

        try:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO expansion_outputs (
                    output_id,
                    task_id,
                    output_type,
                    value,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    output_id,
                    task_id,
                    output_type,
                    value,
                    now,
                ),
            )

            connection.commit()

            return cursor.rowcount > 0

        finally:
            connection.close()

    def _record_outputs(
        self,
        task_id: str,
        output_type: str,
        values: Iterable[str],
    ) -> tuple[int, int]:
        accepted = 0
        duplicate = 0

        for value in values:
            if self._record_output(
                task_id,
                output_type,
                value,
            ):
                accepted += 1
            else:
                duplicate += 1

        return accepted, duplicate

    # ------------------------------------------------------------------
    # Work-queue integration
    # ------------------------------------------------------------------

    def _make_work_item(
        self,
        hostname: str,
        source: str,
        priority: float,
    ) -> Any:
        try:
            from crawler_system.domain_discovery_work_queue import (
                DiscoveryWorkItem,
            )

            return DiscoveryWorkItem(
                work_id=self.task_id_for(
                    hostname,
                    source,
                ),
                logical_partition=0,
                hostname=hostname,
                priority=priority,
                source=source,
            )

        except Exception:
            return {
                "work_id": self.task_id_for(
                    hostname,
                    source,
                ),
                "logical_partition": 0,
                "hostname": hostname,
                "priority": priority,
                "source": source,
            }

    def _enqueue_work(
        self,
        hostname: str,
        source: str,
        priority: float,
    ) -> bool:
        work_item = self._make_work_item(
            hostname,
            source,
            priority,
        )

        enqueue = getattr(
            self.work_queue,
            "enqueue",
            None,
        )

        if not callable(enqueue):
            return False

        try:
            return bool(
                enqueue(work_item)
            )

        except TypeError:
            try:
                return bool(
                    enqueue(
                        work_item
                    )
                )
            except Exception:
                return False

        except Exception:
            return False

    # ------------------------------------------------------------------
    # Complete / failure
    # ------------------------------------------------------------------

    def complete(
        self,
        task: DomainExpansionTask,
        worker_id: str,
        outputs: Iterable[str] = (),
    ) -> bool:
        task_id = self.task_id_for(
            task.hostname,
            task.source,
        )

        now = time.time()

        connection = self._connect()

        try:
            cursor = connection.execute(
                """
                UPDATE expansion_tasks
                SET status = 'completed',
                    completed_at = ?,
                    updated_at = ?,
                    worker_id = NULL,
                    lease_until = NULL
                WHERE task_id = ?
                  AND status = 'processing'
                  AND worker_id = ?
                """,
                (
                    now,
                    now,
                    task_id,
                    worker_id,
                ),
            )

            connection.commit()

            if cursor.rowcount == 0:
                return False

        finally:
            connection.close()

        if self.coverage_control_plane is not None:
            try:
                self.coverage_control_plane.record_completion(
                    task.hostname
                )
            except Exception:
                pass

        return True

    def fail(
        self,
        task: DomainExpansionTask,
        worker_id: str,
        error: str,
        retry_delay: float = 30.0,
    ) -> bool:
        task_id = self.task_id_for(
            task.hostname,
            task.source,
        )

        now = time.time()

        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT attempts
                FROM expansion_tasks
                WHERE task_id = ?
                  AND status = 'processing'
                  AND worker_id = ?
                """,
                (
                    task_id,
                    worker_id,
                ),
            ).fetchone()

            if row is None:
                return False

            attempts = int(
                row["attempts"]
            )

            if attempts >= self.max_attempts:
                status = "failed"
                available_at = now
                failed_at = now
            else:
                status = "queued"
                available_at = (
                    now + max(
                        0.0,
                        retry_delay,
                    )
                )
                failed_at = None

            connection.execute(
                """
                UPDATE expansion_tasks
                SET status = ?,
                    available_at = ?,
                    failed_at = ?,
                    worker_id = NULL,
                    lease_until = NULL,
                    updated_at = ?,
                    last_error = ?
                WHERE task_id = ?
                  AND status = 'processing'
                  AND worker_id = ?
                """,
                (
                    status,
                    available_at,
                    failed_at,
                    now,
                    str(error),
                    task_id,
                    worker_id,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        if self.coverage_control_plane is not None:
            try:
                self.coverage_control_plane.record_failure(
                    task.hostname
                )
            except Exception:
                pass

        return True

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def recover_expired(
        self,
        limit: int = 10000,
    ) -> int:
        now = time.time()

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            rows = connection.execute(
                """
                SELECT task_id, attempts
                FROM expansion_tasks
                WHERE status = 'processing'
                  AND lease_until IS NOT NULL
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
                attempts = int(
                    row["attempts"]
                )

                if attempts >= self.max_attempts:
                    connection.execute(
                        """
                        UPDATE expansion_tasks
                        SET status = 'failed',
                            failed_at = ?,
                            updated_at = ?,
                            worker_id = NULL,
                            lease_until = NULL,
                            last_error = ?
                        WHERE task_id = ?
                        """,
                        (
                            now,
                            now,
                            "lease expired after maximum attempts",
                            row["task_id"],
                        ),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE expansion_tasks
                        SET status = 'queued',
                            available_at = ?,
                            updated_at = ?,
                            worker_id = NULL,
                            lease_until = NULL,
                            last_error = ?
                        WHERE task_id = ?
                        """,
                        (
                            now,
                            now,
                            "worker lease expired",
                            row["task_id"],
                        ),
                    )

                recovered += 1

            connection.commit()

            return recovered

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Expansion cycle
    # ------------------------------------------------------------------

    def expand_task(
        self,
        task: DomainExpansionTask,
        worker_id: str,
    ) -> ExpansionResult:
        task_id = self.task_id_for(
            task.hostname,
            task.source,
        )

        generated_urls = self.generate_urls(
            task
        )

        accepted_urls, duplicate_urls = (
            self._record_outputs(
                task_id,
                "url",
                generated_urls,
            )
        )

        generated_domains = (
            task.metadata.get(
                "domains",
                (),
            )
            if task.metadata
            else ()
        )

        generated_domains = tuple(
            self.normalize_hostname(
                value
            )
            for value in generated_domains
            if value
        )

        accepted_domains, duplicate_domains = (
            self._record_outputs(
                task_id,
                "domain",
                generated_domains,
            )
        )

        rejected = 0

        # Convert discovered domains into durable
        # global discovery work.
        for hostname in generated_domains:
            if not hostname:
                rejected += 1
                continue

            try:
                self._enqueue_work(
                    hostname,
                    task.source,
                    task.priority,
                )

            except Exception:
                rejected += 1

        # Current task completion is durable even if a
        # downstream queue is temporarily unavailable.
        self.complete(
            task,
            worker_id,
            generated_urls,
        )

        return ExpansionResult(
            task_id=task_id,
            hostname=task.hostname,
            source=task.source,
            generated_urls=generated_urls,
            generated_domains=generated_domains,
            accepted=(
                accepted_urls
                + accepted_domains
            ),
            rejected=rejected,
            duplicate=(
                duplicate_urls
                + duplicate_domains
            ),
        )

    def cycle(
        self,
        worker_id: str = "continuous-domain-expansion",
        batch_size: Optional[int] = None,
    ) -> list[ExpansionResult]:
        with self._lock:
            now = time.time()

            if (
                self.expansion_interval > 0
                and self._last_cycle_at
                and (
                    now - self._last_cycle_at
                    < self.expansion_interval
                )
            ):
                return []

            self._last_cycle_at = now
            self._cycle_count += 1
            self._last_error = None

        self.recover_expired()

        tasks = self.claim(
            worker_id=worker_id,
            limit=batch_size
            or self.max_batch_size,
        )

        results: list[ExpansionResult] = []

        for task in tasks:
            try:
                results.append(
                    self.expand_task(
                        task,
                        worker_id,
                    )
                )

            except Exception as exc:
                self._last_error = str(exc)

                try:
                    self.fail(
                        task,
                        worker_id,
                        str(exc),
                    )
                except Exception:
                    pass

        return results

    # ------------------------------------------------------------------
    # Continuous operation
    # ------------------------------------------------------------------

    def run(
        self,
        worker_id: str = "continuous-domain-expansion",
        interval: Optional[float] = None,
    ) -> None:
        sleep_interval = (
            self.expansion_interval
            if interval is None
            else max(0.0, interval)
        )

        with self._lock:
            self._running = True

        while True:
            with self._lock:
                if not self._running:
                    break

            self.cycle(
                worker_id=worker_id
            )

            if sleep_interval > 0:
                time.sleep(
                    sleep_interval
                )

    def stop(self) -> None:
        with self._lock:
            self._running = False

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def checkpoint(
        self,
        key: str,
        value: Any,
    ) -> None:
        now = time.time()

        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        )

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO expansion_checkpoints (
                    checkpoint_key,
                    checkpoint_value,
                    updated_at
                )
                VALUES (?, ?, ?)
                ON CONFLICT(checkpoint_key)
                DO UPDATE SET
                    checkpoint_value = excluded.checkpoint_value,
                    updated_at = excluded.updated_at
                """,
                (
                    str(key),
                    encoded,
                    now,
                ),
            )

            connection.commit()

        finally:
            connection.close()

    def read_checkpoint(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT checkpoint_value
                FROM expansion_checkpoints
                WHERE checkpoint_key = ?
                """,
                (str(key),),
            ).fetchone()

            if row is None:
                return default

            try:
                return json.loads(
                    row["checkpoint_value"]
                )
            except Exception:
                return row["checkpoint_value"]

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Capacity / statistics
    # ------------------------------------------------------------------

    def capacity(self) -> ExpansionCapacity:
        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM expansion_tasks
                GROUP BY status
                """
            ).fetchall()

            counts = {
                row["status"]: int(
                    row["count"]
                )
                for row in rows
            }

        finally:
            connection.close()

        queued = counts.get(
            "queued",
            0,
        )

        processing = counts.get(
            "processing",
            0,
        )

        completed = counts.get(
            "completed",
            0,
        )

        failed = counts.get(
            "failed",
            0,
        )

        return ExpansionCapacity(
            queued=queued,
            processing=processing,
            completed=completed,
            failed=failed,
            max_batch_size=self.max_batch_size,
            admission_limit=self.admission_limit,
            admission_available=self.can_admit(),
        )

    def stats(self) -> dict[str, Any]:
        capacity = self.capacity()

        return {
            "version": VERSION,
            "queued": capacity.queued,
            "processing": capacity.processing,
            "completed": capacity.completed,
            "failed": capacity.failed,
            "cycle_count": self._cycle_count,
            "last_cycle_at": self._last_cycle_at,
            "last_error": self._last_error,
            "running": self.running,
            "max_batch_size": self.max_batch_size,
            "admission_limit": self.admission_limit,
            "fixed_global_execution_limit": False,
        }
