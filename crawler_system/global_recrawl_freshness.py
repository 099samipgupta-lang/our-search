"""
OUR SEARCH — Global Recrawl & Freshness Expansion

PHASE 8 / BRICK 8.4

Continuously schedules previously discovered URLs for recrawling
according to freshness, change signals, importance, failures,
observed crawl intervals, and durable scheduling state.

Architecture:

    URL Universe
         ↓
    Freshness State
         ↓
    Recrawl Eligibility
         ↓
    Adaptive Priority
         ↓
    Durable Recrawl Queue
         ↓
    Partition-aware Scheduling
         ↓
    Existing Crawler Frontier
         ↓
    Crawl Result / Change Signal
         ↓
    Freshness State Update
         ↓
    Next Recrawl

Design principles:

- durable freshness state
- deterministic URL identity
- partition-aware scheduling
- adaptive recrawl timing
- ETag / Last-Modified support
- change-rate feedback
- importance-aware priority
- failure backoff
- lease recovery
- bounded batches
- downstream backpressure
- restart-safe operation
- no fixed global URL ceiling
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional


VERSION = "global-recrawl-freshness.v1"


@dataclass(frozen=True)
class FreshnessRecord:
    url: str
    priority: float = 50.0
    discovered_at: float = 0.0
    last_crawled_at: float = 0.0
    last_changed_at: float = 0.0
    last_status_code: Optional[int] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    content_hash: Optional[str] = None
    change_count: int = 0
    crawl_count: int = 0
    failure_count: int = 0
    observed_interval: float = 86400.0
    metadata: dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class RecrawlWorkItem:
    url_id: str
    url: str
    hostname: str
    priority: float
    partition: int
    due_at: float
    attempt: int
    metadata: dict[str, Any]


@dataclass(frozen=True)
class RecrawlResult:
    url_id: str
    url: str
    changed: bool
    next_crawl_at: float
    priority: float
    success: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class FreshnessCapacity:
    tracked: int
    queued: int
    processing: int
    completed: int
    failed: int
    stale: int
    max_batch_size: int
    partition_count: int


class GlobalRecrawlFreshness:
    """
    Durable global recrawl/freshness scheduler.

    This component does not perform HTTP crawling itself. It maintains
    the durable recrawl universe and forwards eligible URLs to the
    existing crawler frontier.
    """

    DEFAULT_PARTITION_COUNT = 1_048_576

    DEFAULT_INTERVAL = 86400.0

    MIN_INTERVAL = 300.0

    MAX_INTERVAL = 2_592_000.0

    def __init__(
        self,
        storage_root: str,
        frontier: Any = None,
        coverage_control_plane: Any = None,
        partition_count: int = DEFAULT_PARTITION_COUNT,
        max_batch_size: int = 10000,
        lease_seconds: float = 300.0,
        max_attempts: int = 8,
        admission_limit: Optional[int] = None,
        default_interval: float = DEFAULT_INTERVAL,
    ):
        if not storage_root:
            raise ValueError(
                "storage_root is required"
            )

        if partition_count < 1:
            raise ValueError(
                "partition_count must be >= 1"
            )

        if max_batch_size < 1:
            raise ValueError(
                "max_batch_size must be >= 1"
            )

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be > 0"
            )

        if max_attempts < 1:
            raise ValueError(
                "max_attempts must be >= 1"
            )

        if admission_limit is not None and admission_limit < 1:
            raise ValueError(
                "admission_limit must be >= 1"
            )

        if default_interval <= 0:
            raise ValueError(
                "default_interval must be > 0"
            )

        self.storage_root = storage_root
        self.frontier = frontier
        self.coverage_control_plane = (
            coverage_control_plane
        )

        self.partition_count = partition_count
        self.max_batch_size = max_batch_size
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.admission_limit = admission_limit
        self.default_interval = min(
            self.MAX_INTERVAL,
            max(
                self.MIN_INTERVAL,
                default_interval,
            ),
        )

        self.db_path = (
            f"{storage_root}/global_recrawl_freshness.db"
        )

        self._lock = threading.RLock()
        self._running = False
        self._cycle_count = 0
        self._last_cycle_at = 0.0
        self._last_error: Optional[str] = None

        self._init_db()

    # ------------------------------------------------------------------
    # DATABASE
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

        connection.execute(
            "PRAGMA busy_timeout=30000"
        )

        return connection

    def _init_db(self) -> None:
        connection = self._connect()

        try:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS freshness (
                    url_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL UNIQUE,
                    hostname TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    priority REAL NOT NULL DEFAULT 50.0,
                    discovered_at REAL NOT NULL,
                    last_crawled_at REAL,
                    last_changed_at REAL,
                    last_status_code INTEGER,
                    etag TEXT,
                    last_modified TEXT,
                    content_hash TEXT,
                    change_count INTEGER NOT NULL DEFAULT 0,
                    crawl_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    observed_interval REAL NOT NULL,
                    next_crawl_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    worker_id TEXT,
                    fencing_token INTEGER,
                    lease_until REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    completed_at REAL,
                    failed_at REAL,
                    last_error TEXT,
                    metadata_json TEXT
                );

                CREATE INDEX IF NOT EXISTS
                idx_freshness_due
                ON freshness(
                    status,
                    next_crawl_at,
                    priority DESC,
                    partition_id,
                    url_id
                );

                CREATE INDEX IF NOT EXISTS
                idx_freshness_partition
                ON freshness(
                    partition_id,
                    status,
                    next_crawl_at
                );

                CREATE INDEX IF NOT EXISTS
                idx_freshness_hostname
                ON freshness(
                    hostname,
                    status
                );

                CREATE INDEX IF NOT EXISTS
                idx_freshness_worker
                ON freshness(
                    worker_id,
                    status
                );

                CREATE INDEX IF NOT EXISTS
                idx_freshness_changed
                ON freshness(
                    last_changed_at
                );

                CREATE TABLE IF NOT EXISTS
                freshness_history (
                    event_id TEXT PRIMARY KEY,
                    url_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    content_hash TEXT,
                    etag TEXT,
                    last_modified TEXT,
                    status_code INTEGER,
                    metadata_json TEXT
                );

                CREATE INDEX IF NOT EXISTS
                idx_freshness_history_url
                ON freshness_history(
                    url_id,
                    timestamp DESC
                );

                CREATE TABLE IF NOT EXISTS
                freshness_checkpoints (
                    checkpoint_key TEXT PRIMARY KEY,
                    checkpoint_value TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS
                freshness_epochs (
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
    # IDENTITY / PARTITION
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_url(url: str) -> Optional[str]:
        value = str(url).strip()

        if not value:
            return None

        if not (
            value.startswith("http://")
            or value.startswith("https://")
        ):
            return None

        fragment_index = value.find("#")

        if fragment_index >= 0:
            value = value[:fragment_index]

        return value

    @staticmethod
    def url_id_for(url: str) -> str:
        return hashlib.sha256(
            url.encode("utf-8")
        ).hexdigest()

    def partition_for(self, value: str) -> int:
        digest = hashlib.sha256(
            str(value).encode("utf-8")
        ).digest()

        number = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return number % self.partition_count

    @staticmethod
    def hostname_from_url(
        url: str,
    ) -> Optional[str]:
        try:
            from urllib.parse import urlsplit

            hostname = urlsplit(
                url
            ).hostname

        except Exception:
            return None

        if not hostname:
            return None

        return hostname.lower().rstrip(".")

    # ------------------------------------------------------------------
    # TRACKING
    # ------------------------------------------------------------------

    def track(
        self,
        record: FreshnessRecord,
    ) -> bool:
        normalized = self.normalize_url(
            record.url
        )

        if normalized is None:
            return False

        hostname = (
            record.metadata.get(
                "hostname"
            )
            if record.metadata
            else None
        )

        hostname = (
            hostname
            or record.url.split(
                "/",
                3,
            )[2]
            if "/" in record.url
            else None
        )

        hostname = (
            str(hostname)
            .lower()
            .split(":")[0]
            if hostname
            else self.hostname_from_url(
                normalized
            )
        )

        if not hostname:
            return False

        url_id = self.url_id_for(
            normalized
        )

        partition = self.partition_for(
            url_id
        )

        now = time.time()

        discovered_at = (
            record.discovered_at
            or now
        )

        interval = min(
            self.MAX_INTERVAL,
            max(
                self.MIN_INTERVAL,
                float(
                    record.observed_interval
                    or self.default_interval
                ),
            ),
        )

        next_crawl_at = (
            record.last_crawled_at
            + interval
            if record.last_crawled_at
            else now
        )

        metadata_json = json.dumps(
            record.metadata or {},
            sort_keys=True,
            separators=(",", ":"),
        )

        connection = self._connect()

        try:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO freshness (
                    url_id,
                    url,
                    hostname,
                    partition_id,
                    priority,
                    discovered_at,
                    last_crawled_at,
                    last_changed_at,
                    last_status_code,
                    etag,
                    last_modified,
                    content_hash,
                    change_count,
                    crawl_count,
                    failure_count,
                    observed_interval,
                    next_crawl_at,
                    status,
                    attempts,
                    created_at,
                    updated_at,
                    metadata_json
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?
                )
                """,
                (
                    url_id,
                    normalized,
                    hostname,
                    partition,
                    float(record.priority),
                    float(discovered_at),
                    (
                        record.last_crawled_at
                        or None
                    ),
                    (
                        record.last_changed_at
                        or None
                    ),
                    record.last_status_code,
                    record.etag,
                    record.last_modified,
                    record.content_hash,
                    int(record.change_count),
                    int(record.crawl_count),
                    int(record.failure_count),
                    interval,
                    next_crawl_at,
                    now,
                    now,
                    metadata_json,
                ),
            )

            inserted = (
                cursor.rowcount > 0
            )

            connection.commit()

        finally:
            connection.close()

        if (
            inserted
            and self.coverage_control_plane
            is not None
        ):
            try:
                self.coverage_control_plane.record_discovery(
                    normalized
                )
            except Exception:
                pass

        return inserted

    def track_many(
        self,
        records: Iterable[
            FreshnessRecord
        ],
    ) -> int:
        accepted = 0

        batch = []

        for record in records:
            batch.append(record)

            if (
                len(batch)
                >= self.max_batch_size
            ):
                accepted += (
                    self._track_batch(
                        batch
                    )
                )
                batch.clear()

        if batch:
            accepted += (
                self._track_batch(
                    batch
                )
            )

        return accepted

    def _track_batch(
        self,
        records: list[
            FreshnessRecord
        ],
    ) -> int:
        if not records:
            return 0

        connection = self._connect()

        accepted = 0
        now = time.time()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            for record in records:
                normalized = (
                    self.normalize_url(
                        record.url
                    )
                )

                if normalized is None:
                    continue

                hostname = (
                    self.hostname_from_url(
                        normalized
                    )
                )

                if not hostname:
                    continue

                url_id = self.url_id_for(
                    normalized
                )

                partition = (
                    self.partition_for(
                        url_id
                    )
                )

                discovered_at = (
                    record.discovered_at
                    or now
                )

                interval = min(
                    self.MAX_INTERVAL,
                    max(
                        self.MIN_INTERVAL,
                        float(
                            record.observed_interval
                            or self.default_interval
                        ),
                    ),
                )

                next_crawl_at = (
                    (
                        record.last_crawled_at
                        + interval
                    )
                    if record.last_crawled_at
                    else now
                )

                metadata_json = json.dumps(
                    record.metadata or {},
                    sort_keys=True,
                    separators=(
                        ",",
                        ":",
                    ),
                )

                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO freshness (
                        url_id,
                        url,
                        hostname,
                        partition_id,
                        priority,
                        discovered_at,
                        last_crawled_at,
                        last_changed_at,
                        last_status_code,
                        etag,
                        last_modified,
                        content_hash,
                        change_count,
                        crawl_count,
                        failure_count,
                        observed_interval,
                        next_crawl_at,
                        status,
                        attempts,
                        created_at,
                        updated_at,
                        metadata_json
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?
                    )
                    """,
                    (
                        url_id,
                        normalized,
                        hostname,
                        partition,
                        float(record.priority),
                        float(discovered_at),
                        record.last_crawled_at
                        or None,
                        record.last_changed_at
                        or None,
                        record.last_status_code,
                        record.etag,
                        record.last_modified,
                        record.content_hash,
                        int(record.change_count),
                        int(record.crawl_count),
                        int(record.failure_count),
                        interval,
                        next_crawl_at,
                        now,
                        now,
                        metadata_json,
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
    # ADAPTIVE INTERVAL
    # ------------------------------------------------------------------

    def _adaptive_interval(
        self,
        current_interval: float,
        changed: bool,
        failure_count: int,
        priority: float,
    ) -> float:
        interval = float(
            current_interval
            or self.default_interval
        )

        if changed:
            interval *= 0.5
        else:
            interval *= 1.35

        if failure_count > 0:
            interval *= min(
                16.0,
                2.0 ** min(
                    failure_count,
                    4,
                ),
            )

        priority_factor = max(
            0.25,
            min(
                4.0,
                100.0
                / max(
                    1.0,
                    priority,
                ),
            ),
        )

        interval *= (
            0.75
            + (
                0.25
                * priority_factor
            )
        )

        return min(
            self.MAX_INTERVAL,
            max(
                self.MIN_INTERVAL,
                interval,
            ),
        )

    # ------------------------------------------------------------------
    # PRIORITY
    # ------------------------------------------------------------------

    @staticmethod
    def _effective_priority(
        priority: float,
        due_at: float,
        now: float,
        change_count: int,
        failure_count: int,
    ) -> float:
        age_seconds = max(
            0.0,
            now - due_at,
        )

        age_bonus = min(
            50.0,
            math.log1p(
                age_seconds
                / 3600.0
            )
            * 5.0,
        )

        change_bonus = min(
            40.0,
            float(change_count)
            * 2.0,
        )

        failure_penalty = min(
            50.0,
            float(failure_count)
            * 5.0,
        )

        return (
            float(priority)
            + age_bonus
            + change_bonus
            - failure_penalty
        )

    # ------------------------------------------------------------------
    # CAPACITY
    # ------------------------------------------------------------------

    def _downstream_capacity(
        self,
    ) -> Optional[dict[str, int]]:
        for target in (
            self.frontier,
        ):
            if target is None:
                continue

            method = getattr(
                target,
                "capacity",
                None,
            )

            if not callable(method):
                continue

            try:
                result = method()

                if isinstance(
                    result,
                    dict,
                ):
                    return {
                        key: int(value)
                        for key, value
                        in result.items()
                        if isinstance(
                            value,
                            (int, float),
                        )
                    }

                return {
                    "queued": int(
                        getattr(
                            result,
                            "queued",
                            0,
                        )
                    ),
                    "processing": int(
                        getattr(
                            result,
                            "processing",
                            0,
                        )
                    ),
                }

            except Exception:
                continue

        return None

    def can_admit(
        self,
        amount: int = 1,
    ) -> bool:
        if amount <= 0:
            return True

        if self.admission_limit is None:
            return True

        capacity = (
            self._downstream_capacity()
        )

        if capacity is None:
            return True

        current = (
            capacity.get(
                "queued",
                0,
            )
            + capacity.get(
                "processing",
                0,
            )
        )

        return (
            current
            + amount
            <= self.admission_limit
        )

    # ------------------------------------------------------------------
    # CLAIM
    # ------------------------------------------------------------------

    def claim(
        self,
        worker_id: str,
        limit: Optional[int] = None,
        partition: Optional[int] = None,
    ) -> list[
        RecrawlWorkItem
    ]:
        if not worker_id:
            raise ValueError(
                "worker_id is required"
            )

        limit = min(
            limit
            or self.max_batch_size,
            self.max_batch_size,
        )

        now = time.time()
        lease_until = (
            now + self.lease_seconds
        )

        claimed = []

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            if partition is None:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM freshness
                    WHERE status = 'queued'
                      AND next_crawl_at <= ?
                    ORDER BY
                        priority DESC,
                        next_crawl_at ASC,
                        partition_id ASC,
                        url_id ASC
                    LIMIT ?
                    """,
                    (
                        now,
                        limit,
                    ),
                ).fetchall()

            else:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM freshness
                    WHERE status = 'queued'
                      AND next_crawl_at <= ?
                      AND partition_id = ?
                    ORDER BY
                        priority DESC,
                        next_crawl_at ASC,
                        url_id ASC
                    LIMIT ?
                    """,
                    (
                        now,
                        partition,
                        limit,
                    ),
                ).fetchall()

            for row in rows:
                attempts = (
                    int(
                        row["attempts"]
                    )
                    + 1
                )

                if (
                    attempts
                    > self.max_attempts
                ):
                    connection.execute(
                        """
                        UPDATE freshness
                        SET status = 'failed',
                            failed_at = ?,
                            updated_at = ?,
                            last_error = ?
                        WHERE url_id = ?
                        """,
                        (
                            now,
                            now,
                            "maximum attempts exceeded",
                            row["url_id"],
                        ),
                    )

                    continue

                fencing_token = (
                    int(
                        row[
                            "fencing_token"
                        ]
                        or 0
                    )
                    + 1
                )

                cursor = connection.execute(
                    """
                    UPDATE freshness
                    SET status = 'processing',
                        attempts = ?,
                        worker_id = ?,
                        fencing_token = ?,
                        lease_until = ?,
                        updated_at = ?
                    WHERE url_id = ?
                      AND status = 'queued'
                    """,
                    (
                        attempts,
                        worker_id,
                        fencing_token,
                        lease_until,
                        now,
                        row["url_id"],
                    ),
                )

                if cursor.rowcount == 0:
                    continue

                metadata = {}

                if row["metadata_json"]:
                    try:
                        metadata = json.loads(
                            row["metadata_json"]
                        )
                    except Exception:
                        metadata = {}

                effective_priority = (
                    self._effective_priority(
                        float(
                            row["priority"]
                        ),
                        float(
                            row["next_crawl_at"]
                        ),
                        now,
                        int(
                            row[
                                "change_count"
                            ]
                        ),
                        int(
                            row[
                                "failure_count"
                            ]
                        ),
                    )
                )

                claimed.append(
                    RecrawlWorkItem(
                        url_id=row[
                            "url_id"
                        ],
                        url=row[
                            "url"
                        ],
                        hostname=row[
                            "hostname"
                        ],
                        priority=effective_priority,
                        partition=int(
                            row[
                                "partition_id"
                            ]
                        ),
                        due_at=float(
                            row[
                                "next_crawl_at"
                            ]
                        ),
                        attempt=attempts,
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
    # ACTIVATION
    # ------------------------------------------------------------------

    def activate(
        self,
        item: RecrawlWorkItem,
    ) -> bool:
        target = self.frontier

        if target is None:
            return False

        methods = (
            "add_url",
            "add",
            "enqueue",
            "submit",
            "push",
        )

        for method_name in methods:
            method = getattr(
                target,
                method_name,
                None,
            )

            if not callable(method):
                continue

            try:
                result = method(
                    item.url
                )

                if result is None:
                    return True

                return bool(result)

            except TypeError:
                try:
                    result = method(
                        item.url,
                        item.priority,
                    )

                    if result is None:
                        return True

                    return bool(result)

                except Exception:
                    continue

            except Exception:
                continue

        return False

    # ------------------------------------------------------------------
    # RESULT UPDATE
    # ------------------------------------------------------------------

    def record_result(
        self,
        item: RecrawlWorkItem,
        worker_id: str,
        success: bool,
        changed: bool = False,
        status_code: Optional[int] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        content_hash: Optional[str] = None,
        error: Optional[str] = None,
        metadata: Optional[
            dict[str, Any]
        ] = None,
    ) -> RecrawlResult:
        now = time.time()

        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT *
                FROM freshness
                WHERE url_id = ?
                  AND status = 'processing'
                  AND worker_id = ?
                """,
                (
                    item.url_id,
                    worker_id,
                ),
            ).fetchone()

            if row is None:
                return RecrawlResult(
                    url_id=item.url_id,
                    url=item.url,
                    changed=changed,
                    next_crawl_at=now,
                    priority=item.priority,
                    success=False,
                    error="stale or fenced worker",
                )

            previous_crawl = float(
                row[
                    "last_crawled_at"
                ]
                or 0.0
            )

            observed_interval = (
                now - previous_crawl
                if previous_crawl > 0
                else float(
                    row[
                        "observed_interval"
                    ]
                )
            )

            observed_interval = min(
                self.MAX_INTERVAL,
                max(
                    self.MIN_INTERVAL,
                    observed_interval,
                ),
            )

            change_count = int(
                row[
                    "change_count"
                ]
            )

            crawl_count = int(
                row[
                    "crawl_count"
                ]
            )

            failure_count = int(
                row[
                    "failure_count"
                ]
            )

            if success:
                crawl_count += 1

                if changed:
                    change_count += 1
                    failure_count = 0

                else:
                    failure_count = 0

                next_interval = (
                    self._adaptive_interval(
                        observed_interval,
                        changed,
                        0,
                        float(
                            row[
                                "priority"
                            ]
                        ),
                    )
                )

                next_crawl_at = (
                    now
                    + next_interval
                )

                status = "queued"
                failed_at = None
                last_error = None

                if (
                    changed
                    and self.coverage_control_plane
                    is not None
                ):
                    try:
                        self.coverage_control_plane.record_completion(
                            item.url
                        )
                    except Exception:
                        pass

            else:
                failure_count += 1

                next_interval = (
                    self._adaptive_interval(
                        observed_interval,
                        False,
                        failure_count,
                        float(
                            row[
                                "priority"
                            ]
                        ),
                    )
                )

                next_crawl_at = (
                    now
                    + next_interval
                )

                status = (
                    "failed"
                    if failure_count
                    >= self.max_attempts
                    else "queued"
                )

                failed_at = (
                    now
                    if status == "failed"
                    else None
                )

                last_error = (
                    str(error)
                    if error
                    else "recrawl failed"
                )

                if (
                    self.coverage_control_plane
                    is not None
                ):
                    try:
                        self.coverage_control_plane.record_failure(
                            item.url
                        )
                    except Exception:
                        pass

            metadata_json = json.dumps(
                metadata or {},
                sort_keys=True,
                separators=(",", ":"),
            )

            connection.execute(
                """
                UPDATE freshness
                SET
                    status = ?,
                    last_crawled_at = ?,
                    last_changed_at = CASE
                        WHEN ? = 1 THEN ?
                        ELSE last_changed_at
                    END,
                    last_status_code = ?,
                    etag = COALESCE(?, etag),
                    last_modified = COALESCE(?, last_modified),
                    content_hash = COALESCE(?, content_hash),
                    change_count = ?,
                    crawl_count = ?,
                    failure_count = ?,
                    observed_interval = ?,
                    next_crawl_at = ?,
                    worker_id = NULL,
                    lease_until = NULL,
                    completed_at = CASE
                        WHEN ? = 1 THEN ?
                        ELSE completed_at
                    END,
                    failed_at = ?,
                    updated_at = ?,
                    last_error = ?,
                    metadata_json = CASE
                        WHEN ? = '{}' THEN metadata_json
                        ELSE ?
                    END
                WHERE url_id = ?
                  AND status = 'processing'
                  AND worker_id = ?
                """,
                (
                    status,
                    now,
                    1 if changed else 0,
                    now,
                    status_code,
                    etag,
                    last_modified,
                    content_hash,
                    change_count,
                    crawl_count,
                    failure_count,
                    next_interval,
                    next_crawl_at,
                    1 if success else 0,
                    now,
                    failed_at,
                    now,
                    last_error,
                    metadata_json,
                    metadata_json,
                    item.url_id,
                    worker_id,
                ),
            )

            event_payload = json.dumps(
                {
                    "metadata": (
                        metadata or {}
                    ),
                    "error": error,
                },
                sort_keys=True,
                separators=(",", ":"),
            )

            event_id = hashlib.sha256(
                (
                    item.url_id
                    + "|"
                    + str(now)
                    + "|"
                    + (
                        "changed"
                        if changed
                        else (
                            "success"
                            if success
                            else "failure"
                        )
                    )
                ).encode(
                    "utf-8"
                )
            ).hexdigest()

            connection.execute(
                """
                INSERT OR IGNORE INTO
                freshness_history (
                    event_id,
                    url_id,
                    event_type,
                    timestamp,
                    content_hash,
                    etag,
                    last_modified,
                    status_code,
                    metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    item.url_id,
                    (
                        "changed"
                        if changed
                        else (
                            "success"
                            if success
                            else "failure"
                        )
                    ),
                    now,
                    content_hash,
                    etag,
                    last_modified,
                    status_code,
                    event_payload,
                ),
            )

            connection.commit()

        finally:
            connection.close()

        return RecrawlResult(
            url_id=item.url_id,
            url=item.url,
            changed=changed,
            next_crawl_at=next_crawl_at,
            priority=item.priority,
            success=success,
            error=error,
        )

    # ------------------------------------------------------------------
    # LEASE RECOVERY
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
                SELECT url_id, attempts
                FROM freshness
                WHERE status = 'processing'
                  AND lease_until IS NOT NULL
                  AND lease_until <= ?
                ORDER BY
                    lease_until ASC,
                    url_id ASC
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

                if (
                    attempts
                    >= self.max_attempts
                ):
                    connection.execute(
                        """
                        UPDATE freshness
                        SET status = 'failed',
                            failed_at = ?,
                            worker_id = NULL,
                            lease_until = NULL,
                            updated_at = ?,
                            last_error = ?
                        WHERE url_id = ?
                        """,
                        (
                            now,
                            now,
                            "lease expired after maximum attempts",
                            row["url_id"],
                        ),
                    )

                else:
                    connection.execute(
                        """
                        UPDATE freshness
                        SET status = 'queued',
                            next_crawl_at = ?,
                            worker_id = NULL,
                            lease_until = NULL,
                            updated_at = ?,
                            last_error = ?
                        WHERE url_id = ?
                        """,
                        (
                            now,
                            now,
                            "recrawl worker lease expired",
                            row["url_id"],
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
    # STALE DISCOVERY
    # ------------------------------------------------------------------

    def stale_partitions(
        self,
        limit: Optional[int] = None,
    ) -> list[int]:
        now = time.time()

        connection = self._connect()

        try:
            sql = """
                SELECT partition_id
                FROM freshness
                WHERE status = 'queued'
                  AND next_crawl_at <= ?
                GROUP BY partition_id
                ORDER BY partition_id
            """

            parameters = (
                now,
            )

            if limit is not None:
                sql += " LIMIT ?"
                parameters = (
                    now,
                    int(limit),
                )

            rows = connection.execute(
                sql,
                parameters,
            ).fetchall()

            return [
                int(
                    row[
                        "partition_id"
                    ]
                )
                for row in rows
            ]

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # CYCLE
    # ------------------------------------------------------------------

    def cycle(
        self,
        worker_id: str = "global-recrawl-freshness",
        limit: Optional[int] = None,
    ) -> list[
        RecrawlResult
    ]:
        with self._lock:
            self._cycle_count += 1
            self._last_cycle_at = (
                time.time()
            )
            self._last_error = None

        self.recover_expired()

        if not self.can_admit():
            return []

        items = self.claim(
            worker_id=worker_id,
            limit=limit,
        )

        results = []

        for item in items:
            try:
                activated = self.activate(
                    item
                )

                if activated:
                    result = self.record_result(
                        item,
                        worker_id,
                        success=True,
                        changed=False,
                    )
                else:
                    result = self.record_result(
                        item,
                        worker_id,
                        success=False,
                        error=(
                            "downstream crawler admission unavailable"
                        ),
                    )

                results.append(result)

            except Exception as exc:
                self._last_error = str(
                    exc
                )

                try:
                    results.append(
                        self.record_result(
                            item,
                            worker_id,
                            success=False,
                            error=str(exc),
                        )
                    )
                except Exception:
                    pass

        return results

    # ------------------------------------------------------------------
    # CONTINUOUS OPERATION
    # ------------------------------------------------------------------

    def run(
        self,
        worker_id: str = "global-recrawl-freshness",
        interval: float = 5.0,
    ) -> None:
        interval = max(
            0.0,
            interval,
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

            if interval > 0:
                time.sleep(interval)

    def stop(self) -> None:
        with self._lock:
            self._running = False

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    # ------------------------------------------------------------------
    # CHECKPOINTS
    # ------------------------------------------------------------------

    def checkpoint(
        self,
        key: str,
        value: Any,
    ) -> None:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        )

        now = time.time()

        connection = self._connect()

        try:
            connection.execute(
                """
                INSERT INTO freshness_checkpoints (
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
                FROM freshness_checkpoints
                WHERE checkpoint_key = ?
                """,
                (str(key),),
            ).fetchone()

            if row is None:
                return default

            try:
                return json.loads(
                    row[
                        "checkpoint_value"
                    ]
                )
            except Exception:
                return row[
                    "checkpoint_value"
                ]

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # STATS
    # ------------------------------------------------------------------

    def capacity(
        self,
    ) -> FreshnessCapacity:
        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT
                    status,
                    COUNT(*) AS count
                FROM freshness
                GROUP BY status
                """
            ).fetchall()

            counts = {
                row["status"]: int(
                    row["count"]
                )
                for row in rows
            }

            tracked = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM freshness
                    """
                ).fetchone()[0]
            )

            stale = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM freshness
                    WHERE status = 'queued'
                      AND next_crawl_at <= ?
                    """,
                    (time.time(),),
                ).fetchone()[0]
            )

        finally:
            connection.close()

        return FreshnessCapacity(
            tracked=tracked,
            queued=counts.get(
                "queued",
                0,
            ),
            processing=counts.get(
                "processing",
                0,
            ),
            completed=counts.get(
                "completed",
                0,
            ),
            failed=counts.get(
                "failed",
                0,
            ),
            stale=stale,
            max_batch_size=(
                self.max_batch_size
            ),
            partition_count=(
                self.partition_count
            ),
        )

    def stats(self) -> dict[str, Any]:
        capacity = self.capacity()

        return {
            "version": VERSION,
            "tracked": capacity.tracked,
            "queued": capacity.queued,
            "processing": capacity.processing,
            "completed": capacity.completed,
            "failed": capacity.failed,
            "stale": capacity.stale,
            "partition_count": (
                self.partition_count
            ),
            "max_batch_size": (
                self.max_batch_size
            ),
            "lease_seconds": (
                self.lease_seconds
            ),
            "max_attempts": (
                self.max_attempts
            ),
            "admission_limit": (
                self.admission_limit
            ),
            "default_interval": (
                self.default_interval
            ),
            "cycle_count": (
                self._cycle_count
            ),
            "last_cycle_at": (
                self._last_cycle_at
            ),
            "last_error": (
                self._last_error
            ),
            "running": self.running,
            "fixed_global_url_limit": False,
        }


__all__ = [
    "VERSION",
    "FreshnessRecord",
    "RecrawlWorkItem",
    "RecrawlResult",
    "FreshnessCapacity",
    "GlobalRecrawlFreshness",
]
