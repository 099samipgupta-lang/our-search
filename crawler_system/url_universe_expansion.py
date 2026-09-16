"""
OUR SEARCH — URL Universe Expansion

PHASE 8 / BRICK 8.3

Expands the discovered Web from domain-level knowledge into a durable,
effectively unbounded URL universe.

Pipeline:

    Domain Discovery
          ↓
    URL Candidate Generation
          ↓
    URL Normalization
          ↓
    Deterministic Identity
          ↓
    Partition Assignment
          ↓
    Durable URL Admission
          ↓
    Priority / Provenance
          ↓
    Backpressure
          ↓
    Crawler Frontier / Discovery Queue
          ↓
    Crawled URLs produce more URLs
          ↓
    URL Universe Expansion again

Design principles:

- URL identity is deterministic.
- Duplicate URLs are admitted only once.
- URL storage is partition-aware.
- No fixed global URL-count ceiling.
- Work is processed in bounded batches.
- Sources remain independently attributable.
- Failed work is retryable.
- Expired leases are recoverable.
- Checkpoints survive process restart.
- Admission respects downstream capacity when available.
- The component does not replace the existing crawler.
- The component feeds the existing crawler architecture.
- Large-scale execution is achieved through partitioning rather than
  one giant global queue.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional
from urllib.parse import (
    parse_qsl,
    urlencode,
    urlsplit,
    urlunsplit,
)


VERSION = "url-universe-expansion.v1"


@dataclass(frozen=True)
class URLCandidate:
    url: str
    source: str = "unknown"
    priority: float = 50.0
    discovered_at: float = 0.0
    parent_url: Optional[str] = None
    hostname: Optional[str] = None
    metadata: dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class URLWorkItem:
    url_id: str
    url: str
    hostname: str
    source: str
    priority: float
    partition: int
    parent_url: Optional[str]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class URLExpansionResult:
    url_id: str
    url: str
    hostname: str
    accepted: int
    duplicate: int
    rejected: int
    generated: int
    failed: bool = False
    error: Optional[str] = None


@dataclass(frozen=True)
class URLUniverseCapacity:
    discovered: int
    queued: int
    processing: int
    completed: int
    failed: int
    max_batch_size: int
    partition_count: int


class URLUniverseExpansion:
    """
    Durable URL-universe expansion controller.

    This component is deliberately independent from the crawler's
    execution engine. It manages URL discovery/admission and exposes
    work to the existing crawler architecture.
    """

    DEFAULT_PARTITION_COUNT = 1_048_576

    def __init__(
        self,
        storage_root: str,
        frontier: Any = None,
        work_queue: Any = None,
        coverage_control_plane: Any = None,
        partition_count: int = DEFAULT_PARTITION_COUNT,
        max_batch_size: int = 10000,
        max_attempts: int = 8,
        lease_seconds: float = 300.0,
        retry_delay: float = 30.0,
        admission_limit: Optional[int] = None,
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

        if max_attempts < 1:
            raise ValueError(
                "max_attempts must be >= 1"
            )

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be > 0"
            )

        if retry_delay < 0:
            raise ValueError(
                "retry_delay must be >= 0"
            )

        if (
            admission_limit is not None
            and admission_limit < 1
        ):
            raise ValueError(
                "admission_limit must be >= 1"
            )

        self.storage_root = storage_root
        self.frontier = frontier
        self.work_queue = work_queue
        self.coverage_control_plane = (
            coverage_control_plane
        )

        self.partition_count = partition_count
        self.max_batch_size = max_batch_size
        self.max_attempts = max_attempts
        self.lease_seconds = lease_seconds
        self.retry_delay = retry_delay
        self.admission_limit = admission_limit

        self.db_path = (
            f"{storage_root}/url_universe.db"
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
                CREATE TABLE IF NOT EXISTS urls (
                    url_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL UNIQUE,
                    hostname TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    priority REAL NOT NULL DEFAULT 50.0,
                    parent_url TEXT,
                    metadata_json TEXT,
                    discovered_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    worker_id TEXT,
                    fencing_token INTEGER,
                    lease_until REAL,
                    available_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    completed_at REAL,
                    failed_at REAL,
                    last_error TEXT
                );

                CREATE INDEX IF NOT EXISTS
                idx_urls_ready
                ON urls(
                    status,
                    available_at,
                    priority DESC,
                    partition_id,
                    url_id
                );

                CREATE INDEX IF NOT EXISTS
                idx_urls_partition
                ON urls(
                    partition_id,
                    status,
                    priority DESC
                );

                CREATE INDEX IF NOT EXISTS
                idx_urls_hostname
                ON urls(
                    hostname,
                    status
                );

                CREATE INDEX IF NOT EXISTS
                idx_urls_worker
                ON urls(
                    worker_id,
                    status
                );

                CREATE INDEX IF NOT EXISTS
                idx_urls_source
                ON urls(
                    source,
                    status
                );

                CREATE TABLE IF NOT EXISTS url_sources (
                    url_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    discovered_at REAL NOT NULL,
                    parent_url TEXT,
                    evidence_json TEXT,
                    PRIMARY KEY(url_id, source)
                );

                CREATE INDEX IF NOT EXISTS
                idx_url_sources_source
                ON url_sources(source);

                CREATE TABLE IF NOT EXISTS
                url_expansion_outputs (
                    output_id TEXT PRIMARY KEY,
                    parent_url_id TEXT NOT NULL,
                    output_url_id TEXT,
                    output_url TEXT NOT NULL,
                    source TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    UNIQUE(
                        parent_url_id,
                        output_url
                    )
                );

                CREATE INDEX IF NOT EXISTS
                idx_url_expansion_parent
                ON url_expansion_outputs(
                    parent_url_id
                );

                CREATE TABLE IF NOT EXISTS
                url_universe_checkpoints (
                    checkpoint_key TEXT PRIMARY KEY,
                    checkpoint_value TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS
                url_universe_epochs (
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
    # NORMALIZATION
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_url(
        url: str,
    ) -> Optional[str]:
        """
        Canonicalize a public HTTP(S) URL.

        The normalizer intentionally avoids aggressive transformations
        that could merge genuinely different resources.
        """

        if url is None:
            return None

        value = str(url).strip()

        if not value:
            return None

        if any(
            character.isspace()
            for character in value
        ):
            return None

        try:
            parts = urlsplit(value)
        except Exception:
            return None

        scheme = parts.scheme.lower()

        if scheme not in {
            "http",
            "https",
        }:
            return None

        hostname = parts.hostname

        if not hostname:
            return None

        hostname = hostname.lower().rstrip(".")

        if not hostname:
            return None

        try:
            port = parts.port
        except ValueError:
            return None

        username = parts.username
        password = parts.password

        # Credentials are not accepted into the public-Web URL universe.
        if username is not None or password is not None:
            return None

        netloc = hostname

        if ":" in hostname and not hostname.startswith("["):
            netloc = f"[{hostname}]"

        if port is not None:
            default_port = (
                port == 80
                and scheme == "http"
            ) or (
                port == 443
                and scheme == "https"
            )

            if not default_port:
                netloc = (
                    f"{netloc}:{port}"
                )

        path = parts.path or "/"

        # Normalize repeated empty path forms.
        if not path.startswith("/"):
            path = "/" + path

        # Remove fragment because fragments are client-side and do
        # not identify a separate HTTP resource.
        fragment = ""

        # Normalize query ordering while preserving repeated keys.
        query = parts.query

        if query:
            try:
                query_pairs = parse_qsl(
                    query,
                    keep_blank_values=True,
                )

                query = urlencode(
                    sorted(query_pairs),
                    doseq=True,
                )
            except Exception:
                query = parts.query

        normalized = urlunsplit(
            (
                scheme,
                netloc,
                path,
                query,
                fragment,
            )
        )

        if len(normalized) > 8192:
            return None

        return normalized

    @staticmethod
    def hostname_from_url(
        url: str,
    ) -> Optional[str]:
        try:
            hostname = urlsplit(
                url
            ).hostname
        except Exception:
            return None

        if not hostname:
            return None

        return hostname.lower().rstrip(".")

    # ------------------------------------------------------------------
    # IDENTITY / PARTITIONING
    # ------------------------------------------------------------------

    @staticmethod
    def url_id_for(
        url: str,
    ) -> str:
        return hashlib.sha256(
            url.encode("utf-8")
        ).hexdigest()

    def partition_for(
        self,
        url_or_id: str,
    ) -> int:
        digest = hashlib.sha256(
            str(url_or_id).encode(
                "utf-8"
            )
        ).digest()

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return value % self.partition_count

    # ------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------

    @classmethod
    def validate_candidate(
        cls,
        candidate: URLCandidate,
    ) -> Optional[
        tuple[str, str]
    ]:
        normalized = cls.normalize_url(
            candidate.url
        )

        if normalized is None:
            return None

        hostname = (
            candidate.hostname
            or cls.hostname_from_url(
                normalized
            )
        )

        if not hostname:
            return None

        if len(hostname) > 253:
            return None

        labels = hostname.split(".")

        if len(labels) < 2:
            return None

        for label in labels:
            if not label:
                return None

            if len(label) > 63:
                return None

            if (
                label.startswith("-")
                or label.endswith("-")
            ):
                return None

            if not re.fullmatch(
                r"[a-z0-9-]+",
                label,
            ):
                return None

        return (
            normalized,
            hostname,
        )

    # ------------------------------------------------------------------
    # ADMISSION
    # ------------------------------------------------------------------

    def _downstream_capacity(
        self,
    ) -> Optional[dict[str, int]]:
        targets = (
            self.work_queue,
            self.frontier,
        )

        for target in targets:
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

        if (
            self.admission_limit
            is None
        ):
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

    def admit(
        self,
        candidate: URLCandidate,
    ) -> bool:
        prepared = (
            self.validate_candidate(
                candidate
            )
        )

        if prepared is None:
            return False

        normalized, hostname = prepared

        if not self.can_admit():
            return False

        url_id = self.url_id_for(
            normalized
        )

        partition = self.partition_for(
            url_id
        )

        now = time.time()

        discovered_at = (
            candidate.discovered_at
            or now
        )

        metadata_json = json.dumps(
            candidate.metadata or {},
            sort_keys=True,
            separators=(",", ":"),
        )

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO urls (
                    url_id,
                    url,
                    hostname,
                    partition_id,
                    source,
                    priority,
                    parent_url,
                    metadata_json,
                    discovered_at,
                    status,
                    attempts,
                    available_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    'queued', 0, ?, ?, ?
                )
                """,
                (
                    url_id,
                    normalized,
                    hostname,
                    partition,
                    str(candidate.source),
                    float(candidate.priority),
                    candidate.parent_url,
                    metadata_json,
                    float(discovered_at),
                    now,
                    now,
                    now,
                ),
            )

            inserted = (
                cursor.rowcount > 0
            )

            connection.execute(
                """
                INSERT OR IGNORE INTO
                url_sources (
                    url_id,
                    source,
                    discovered_at,
                    parent_url,
                    evidence_json
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    url_id,
                    str(candidate.source),
                    float(discovered_at),
                    candidate.parent_url,
                    metadata_json,
                ),
            )

            connection.commit()

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

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    def admit_many(
        self,
        candidates: Iterable[
            URLCandidate
        ],
    ) -> dict[str, int]:
        accepted = 0
        duplicate = 0
        rejected = 0

        batch = []

        for candidate in candidates:
            batch.append(candidate)

            if (
                len(batch)
                >= self.max_batch_size
            ):
                result = (
                    self._admit_batch(
                        batch
                    )
                )

                accepted += result[
                    "accepted"
                ]

                duplicate += result[
                    "duplicate"
                ]

                rejected += result[
                    "rejected"
                ]

                batch.clear()

        if batch:
            result = (
                self._admit_batch(
                    batch
                )
            )

            accepted += result[
                "accepted"
            ]

            duplicate += result[
                "duplicate"
            ]

            rejected += result[
                "rejected"
            ]

        return {
            "accepted": accepted,
            "duplicate": duplicate,
            "rejected": rejected,
        }

    def _admit_batch(
        self,
        candidates: list[
            URLCandidate
        ],
    ) -> dict[str, int]:
        if not candidates:
            return {
                "accepted": 0,
                "duplicate": 0,
                "rejected": 0,
            }

        accepted = 0
        duplicate = 0
        rejected = 0

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            now = time.time()

            for candidate in candidates:
                prepared = (
                    self.validate_candidate(
                        candidate
                    )
                )

                if prepared is None:
                    rejected += 1
                    continue

                normalized, hostname = (
                    prepared
                )

                url_id = self.url_id_for(
                    normalized
                )

                partition = (
                    self.partition_for(
                        url_id
                    )
                )

                discovered_at = (
                    candidate.discovered_at
                    or now
                )

                metadata_json = json.dumps(
                    candidate.metadata
                    or {},
                    sort_keys=True,
                    separators=(
                        ",",
                        ":",
                    ),
                )

                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO urls (
                        url_id,
                        url,
                        hostname,
                        partition_id,
                        source,
                        priority,
                        parent_url,
                        metadata_json,
                        discovered_at,
                        status,
                        attempts,
                        available_at,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, 'queued', 0, ?, ?, ?
                    )
                    """,
                    (
                        url_id,
                        normalized,
                        hostname,
                        partition,
                        str(candidate.source),
                        float(
                            candidate.priority
                        ),
                        candidate.parent_url,
                        metadata_json,
                        float(
                            discovered_at
                        ),
                        now,
                        now,
                        now,
                    ),
                )

                inserted = (
                    cursor.rowcount > 0
                )

                if inserted:
                    accepted += 1
                else:
                    duplicate += 1

                connection.execute(
                    """
                    INSERT OR IGNORE INTO
                    url_sources (
                        url_id,
                        source,
                        discovered_at,
                        parent_url,
                        evidence_json
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        url_id,
                        str(candidate.source),
                        float(
                            discovered_at
                        ),
                        candidate.parent_url,
                        metadata_json,
                    ),
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        return {
            "accepted": accepted,
            "duplicate": duplicate,
            "rejected": rejected,
        }

    # ------------------------------------------------------------------
    # CLAIMING
    # ------------------------------------------------------------------

    def claim(
        self,
        worker_id: str,
        limit: Optional[int] = None,
        partition: Optional[int] = None,
    ) -> list[URLWorkItem]:
        if not worker_id:
            raise ValueError(
                "worker_id is required"
            )

        requested = (
            limit
            or self.max_batch_size
        )

        limit = max(
            1,
            min(
                requested,
                self.max_batch_size,
            ),
        )

        now = time.time()
        lease_until = (
            now + self.lease_seconds
        )

        claimed: list[
            URLWorkItem
        ] = []

        connection = self._connect()

        try:
            connection.execute(
                "BEGIN IMMEDIATE"
            )

            if partition is None:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM urls
                    WHERE status = 'queued'
                      AND available_at <= ?
                    ORDER BY
                        priority DESC,
                        available_at ASC,
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
                    FROM urls
                    WHERE status = 'queued'
                      AND available_at <= ?
                      AND partition_id = ?
                    ORDER BY
                        priority DESC,
                        available_at ASC,
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
                        UPDATE urls
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
                    UPDATE urls
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

                if row[
                    "metadata_json"
                ]:
                    try:
                        metadata = json.loads(
                            row[
                                "metadata_json"
                            ]
                        )
                    except Exception:
                        metadata = {}

                claimed.append(
                    URLWorkItem(
                        url_id=row[
                            "url_id"
                        ],
                        url=row["url"],
                        hostname=row[
                            "hostname"
                        ],
                        source=row[
                            "source"
                        ],
                        priority=float(
                            row[
                                "priority"
                            ]
                        ),
                        partition=int(
                            row[
                                "partition_id"
                            ]
                        ),
                        parent_url=row[
                            "parent_url"
                        ],
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
    # DOWNSTREAM ACTIVATION
    # ------------------------------------------------------------------

    def activate(
        self,
        item: URLWorkItem,
    ) -> bool:
        """
        Forward a claimed URL to the existing crawler frontier.

        Multiple frontier APIs are supported without making the
        frontier implementation a dependency of this component.
        """

        target = (
            self.frontier
            or self.work_queue
        )

        if target is None:
            return False

        methods = (
            "add",
            "add_url",
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
                        URLCandidate(
                            url=item.url,
                            source=item.source,
                            priority=item.priority,
                            parent_url=item.parent_url,
                            hostname=item.hostname,
                            metadata=item.metadata,
                        )
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
    # COMPLETION / FAILURE
    # ------------------------------------------------------------------

    def complete(
        self,
        item: URLWorkItem,
        worker_id: str,
    ) -> bool:
        now = time.time()

        connection = self._connect()

        try:
            cursor = connection.execute(
                """
                UPDATE urls
                SET status = 'completed',
                    completed_at = ?,
                    updated_at = ?,
                    worker_id = NULL,
                    lease_until = NULL
                WHERE url_id = ?
                  AND status = 'processing'
                  AND worker_id = ?
                  AND fencing_token = ?
                """,
                (
                    now,
                    now,
                    item.url_id,
                    worker_id,
                    self._fencing_token(
                        item.url_id
                    ),
                ),
            )

            connection.commit()

            if cursor.rowcount == 0:
                return False

        finally:
            connection.close()

        if (
            self.coverage_control_plane
            is not None
        ):
            try:
                self.coverage_control_plane.record_completion(
                    item.url
                )
            except Exception:
                pass

        return True

    def _fencing_token(
        self,
        url_id: str,
    ) -> int:
        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT fencing_token
                FROM urls
                WHERE url_id = ?
                """,
                (url_id,),
            ).fetchone()

            if row is None:
                return 0

            return int(
                row[
                    "fencing_token"
                ]
                or 0
            )

        finally:
            connection.close()

    def renew(
        self,
        item: URLWorkItem,
        worker_id: str,
    ) -> bool:
        now = time.time()
        lease_until = (
            now + self.lease_seconds
        )

        connection = self._connect()

        try:
            cursor = connection.execute(
                """
                UPDATE urls
                SET lease_until = ?,
                    updated_at = ?
                WHERE url_id = ?
                  AND status = 'processing'
                  AND worker_id = ?
                """,
                (
                    lease_until,
                    now,
                    item.url_id,
                    worker_id,
                ),
            )

            connection.commit()

            return cursor.rowcount > 0

        finally:
            connection.close()

    def fail(
        self,
        item: URLWorkItem,
        worker_id: str,
        error: str,
    ) -> bool:
        now = time.time()

        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT attempts
                FROM urls
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
                return False

            attempts = int(
                row["attempts"]
            )

            if (
                attempts
                >= self.max_attempts
            ):
                status = "failed"
                available_at = now
                failed_at = now
            else:
                status = "queued"
                available_at = (
                    now
                    + self.retry_delay
                )
                failed_at = None

            cursor = connection.execute(
                """
                UPDATE urls
                SET status = ?,
                    available_at = ?,
                    failed_at = ?,
                    worker_id = NULL,
                    lease_until = NULL,
                    updated_at = ?,
                    last_error = ?
                WHERE url_id = ?
                  AND status = 'processing'
                  AND worker_id = ?
                """,
                (
                    status,
                    available_at,
                    failed_at,
                    now,
                    str(error),
                    item.url_id,
                    worker_id,
                ),
            )

            connection.commit()

            updated = (
                cursor.rowcount > 0
            )

        finally:
            connection.close()

        if (
            updated
            and self.coverage_control_plane
            is not None
        ):
            try:
                self.coverage_control_plane.record_failure(
                    item.url
                )
            except Exception:
                pass

        return updated

    # ------------------------------------------------------------------
    # RECOVERY
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
                FROM urls
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
                        UPDATE urls
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
                        UPDATE urls
                        SET status = 'queued',
                            available_at = ?,
                            worker_id = NULL,
                            lease_until = NULL,
                            updated_at = ?,
                            last_error = ?
                        WHERE url_id = ?
                        """,
                        (
                            now,
                            now,
                            "worker lease expired",
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
    # URL GENERATION
    # ------------------------------------------------------------------

    def generate_domain_urls(
        self,
        hostname: str,
    ) -> tuple[str, ...]:
        hostname = (
            str(hostname)
            .strip()
            .lower()
            .rstrip(".")
        )

        if not hostname:
            return ()

        return (
            f"https://{hostname}/",
            f"http://{hostname}/",
            f"https://{hostname}/robots.txt",
            f"https://{hostname}/sitemap.xml",
        )

    def generate_from_links(
        self,
        parent_url: str,
        links: Iterable[str],
        source: str = "link",
        priority: float = 50.0,
    ) -> list[URLCandidate]:
        candidates = []

        for value in links:
            normalized = (
                self.normalize_url(
                    str(value)
                )
            )

            if normalized is None:
                continue

            candidates.append(
                URLCandidate(
                    url=normalized,
                    source=source,
                    priority=priority,
                    parent_url=parent_url,
                )
            )

        return candidates

    def generate_from_sitemap(
        self,
        urls: Iterable[str],
        parent_url: Optional[str] = None,
        priority: float = 60.0,
    ) -> list[URLCandidate]:
        return [
            URLCandidate(
                url=value,
                source="sitemap",
                priority=priority,
                parent_url=parent_url,
            )
            for value in urls
            if value
        ]

    def generate_from_feed(
        self,
        urls: Iterable[str],
        parent_url: Optional[str] = None,
        priority: float = 70.0,
    ) -> list[URLCandidate]:
        return [
            URLCandidate(
                url=value,
                source="feed",
                priority=priority,
                parent_url=parent_url,
            )
            for value in urls
            if value
        ]

    # ------------------------------------------------------------------
    # EXPANSION OUTPUTS
    # ------------------------------------------------------------------

    def record_expansion_output(
        self,
        parent_url_id: str,
        output_url: str,
        source: str,
    ) -> bool:
        normalized = (
            self.normalize_url(
                output_url
            )
        )

        if normalized is None:
            return False

        output_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                parent_url_id
                + "|"
                + normalized,
            )
        )

        output_url_id = (
            self.url_id_for(
                normalized
            )
        )

        connection = self._connect()

        try:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO
                url_expansion_outputs (
                    output_id,
                    parent_url_id,
                    output_url_id,
                    output_url,
                    source,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    output_id,
                    parent_url_id,
                    output_url_id,
                    normalized,
                    str(source),
                    time.time(),
                ),
            )

            connection.commit()

            return cursor.rowcount > 0

        finally:
            connection.close()

    def expand(
        self,
        parent: URLWorkItem,
        candidates: Iterable[
            URLCandidate
        ],
    ) -> URLExpansionResult:
        accepted = 0
        duplicate = 0
        rejected = 0
        generated = 0

        for candidate in candidates:
            generated += 1

            prepared = (
                self.validate_candidate(
                    candidate
                )
            )

            if prepared is None:
                rejected += 1
                continue

            normalized, _ = prepared

            if self.record_expansion_output(
                parent.url_id,
                normalized,
                candidate.source,
            ):
                pass

            admitted = self.admit(
                candidate
            )

            if admitted:
                accepted += 1
            else:
                # Distinguish duplicate from
                # invalid where possible.
                connection = (
                    self._connect()
                )

                try:
                    row = connection.execute(
                        """
                        SELECT url_id
                        FROM urls
                        WHERE url = ?
                        """,
                        (normalized,),
                    ).fetchone()
                finally:
                    connection.close()

                if row is not None:
                    duplicate += 1
                else:
                    rejected += 1

        return URLExpansionResult(
            url_id=parent.url_id,
            url=parent.url,
            hostname=parent.hostname,
            accepted=accepted,
            duplicate=duplicate,
            rejected=rejected,
            generated=generated,
        )

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
                INSERT INTO
                url_universe_checkpoints (
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
                FROM url_universe_checkpoints
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
    # CYCLE
    # ------------------------------------------------------------------

    def process_batch(
        self,
        worker_id: str,
        limit: Optional[int] = None,
        partition: Optional[int] = None,
    ) -> list[
        URLExpansionResult
    ]:
        items = self.claim(
            worker_id=worker_id,
            limit=limit,
            partition=partition,
        )

        results = []

        for item in items:
            try:
                activated = self.activate(
                    item
                )

                if activated:
                    self.complete(
                        item,
                        worker_id,
                    )
                else:
                    self.fail(
                        item,
                        worker_id,
                        "downstream crawler admission unavailable",
                    )

                results.append(
                    URLExpansionResult(
                        url_id=item.url_id,
                        url=item.url,
                        hostname=item.hostname,
                        accepted=(
                            1
                            if activated
                            else 0
                        ),
                        duplicate=0,
                        rejected=(
                            0
                            if activated
                            else 1
                        ),
                        generated=0,
                        failed=not activated,
                        error=(
                            None
                            if activated
                            else
                            "downstream crawler admission unavailable"
                        ),
                    )
                )

            except Exception as exc:
                self._last_error = str(
                    exc
                )

                try:
                    self.fail(
                        item,
                        worker_id,
                        str(exc),
                    )
                except Exception:
                    pass

                results.append(
                    URLExpansionResult(
                        url_id=item.url_id,
                        url=item.url,
                        hostname=item.hostname,
                        accepted=0,
                        duplicate=0,
                        rejected=1,
                        generated=0,
                        failed=True,
                        error=str(exc),
                    )
                )

        return results

    def cycle(
        self,
        worker_id: str = "url-universe-expansion",
        limit: Optional[int] = None,
    ) -> list[
        URLExpansionResult
    ]:
        with self._lock:
            self._cycle_count += 1
            self._last_cycle_at = (
                time.time()
            )
            self._last_error = None

        self.recover_expired()

        return self.process_batch(
            worker_id=worker_id,
            limit=limit,
        )

    # ------------------------------------------------------------------
    # CONTINUOUS RUN
    # ------------------------------------------------------------------

    def run(
        self,
        worker_id: str = "url-universe-expansion",
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
    # CAPACITY / STATS
    # ------------------------------------------------------------------

    def capacity(
        self,
    ) -> URLUniverseCapacity:
        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT
                    status,
                    COUNT(*) AS count
                FROM urls
                GROUP BY status
                """
            ).fetchall()

            counts = {
                row["status"]: int(
                    row["count"]
                )
                for row in rows
            }

            discovered = connection.execute(
                """
                SELECT COUNT(*)
                FROM urls
                """
            ).fetchone()[0]

        finally:
            connection.close()

        return URLUniverseCapacity(
            discovered=int(
                discovered
            ),
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
            max_batch_size=(
                self.max_batch_size
            ),
            partition_count=(
                self.partition_count
            ),
        )

    def partition_stats(
        self,
        partition: int,
    ) -> dict[str, int]:
        if not (
            0
            <= partition
            < self.partition_count
        ):
            raise ValueError(
                "invalid partition"
            )

        connection = self._connect()

        try:
            rows = connection.execute(
                """
                SELECT
                    status,
                    COUNT(*) AS count
                FROM urls
                WHERE partition_id = ?
                GROUP BY status
                """,
                (partition,),
            ).fetchall()

            return {
                row["status"]: int(
                    row["count"]
                )
                for row in rows
            }

        finally:
            connection.close()

    def active_partitions(
        self,
        limit: Optional[int] = None,
    ) -> list[int]:
        connection = self._connect()

        try:
            sql = """
                SELECT partition_id
                FROM urls
                WHERE status IN (
                    'queued',
                    'processing'
                )
                GROUP BY partition_id
                ORDER BY partition_id
            """

            parameters = ()

            if limit is not None:
                sql += " LIMIT ?"
                parameters = (
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

    def stats(self) -> dict[str, Any]:
        capacity = self.capacity()

        return {
            "version": VERSION,
            "discovered": capacity.discovered,
            "queued": capacity.queued,
            "processing": capacity.processing,
            "completed": capacity.completed,
            "failed": capacity.failed,
            "partition_count": (
                self.partition_count
            ),
            "max_batch_size": (
                self.max_batch_size
            ),
            "max_attempts": (
                self.max_attempts
            ),
            "lease_seconds": (
                self.lease_seconds
            ),
            "retry_delay": (
                self.retry_delay
            ),
            "admission_limit": (
                self.admission_limit
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
    "URLCandidate",
    "URLWorkItem",
    "URLExpansionResult",
    "URLUniverseCapacity",
    "URLUniverseExpansion",
]
