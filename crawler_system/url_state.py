import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional


class URLStateStore:
    """
    Durable SQLite-backed state store for OUR SEARCH crawler URLs.

    This store contains the durable state required by the crawler
    frontier. The interface is intentionally independent from the
    current frontier implementation so the storage layer can later
    be replaced by distributed storage.
    """

    SCHEMA_VERSION = 3

    def __init__(
        self,
        database_path: str = "crawler_storage/url_state.db"
    ):
        self.database_path = os.path.abspath(
            database_path
        )

        directory = os.path.dirname(
            self.database_path
        )

        if directory:
            os.makedirs(
                directory,
                exist_ok=True
            )

        self._lock = threading.RLock()

        self._connection = sqlite3.connect(
            self.database_path,
            timeout=30,
            check_same_thread=False
        )

        self._connection.row_factory = sqlite3.Row

        self._configure()
        self._initialize_schema()

    # ============================================================
    # SQLITE CONFIGURATION
    # ============================================================

    def _configure(self) -> None:
        """Configure SQLite for reliable local crawler state."""

        with self._connection:
            self._connection.execute(
                "PRAGMA foreign_keys = ON"
            )

            self._connection.execute(
                "PRAGMA busy_timeout = 30000"
            )

    # ============================================================
    # SCHEMA
    # ============================================================

    def _initialize_schema(self) -> None:
        """Create or upgrade the crawler state schema."""

        with self._lock:

            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS urls (
                    url TEXT PRIMARY KEY,

                    document_id TEXT NOT NULL,
                    host TEXT NOT NULL,

                    state TEXT NOT NULL DEFAULT 'discovered',

                    priority REAL NOT NULL DEFAULT 50.0,

                    attempts INTEGER NOT NULL DEFAULT 0,

                    source TEXT,

                    discovered_at REAL NOT NULL,

                    last_crawled_at REAL,
                    next_crawl_at REAL,

                    last_status INTEGER,
                    last_error TEXT,

                    lease_owner TEXT,
                    leased_at REAL
                );

                CREATE TABLE IF NOT EXISTS hosts (
                    host TEXT PRIMARY KEY,

                    crawl_delay REAL NOT NULL DEFAULT 2.0,

                    last_crawl_time REAL NOT NULL DEFAULT 0,

                    next_allowed_time REAL NOT NULL DEFAULT 0,

                    failures INTEGER NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_urls_state_priority
                ON urls(
                    state,
                    priority DESC,
                    next_crawl_at
                );

                CREATE INDEX IF NOT EXISTS idx_urls_host
                ON urls(host);

                CREATE INDEX IF NOT EXISTS idx_urls_state_host
                ON urls(state, host);

                CREATE INDEX IF NOT EXISTS idx_urls_lease
                ON urls(state, leased_at);

                CREATE INDEX IF NOT EXISTS idx_hosts_next_allowed
                ON hosts(next_allowed_time);

                INSERT OR IGNORE INTO metadata(
                    key,
                    value
                )
                VALUES (
                    'schema_version',
                    '2'
                );
                """
            )

            # ----------------------------------------------------
            # MIGRATION: v2 -> v3
            # ----------------------------------------------------

            columns = {
                row["name"]
                for row in self._connection.execute(
                    "PRAGMA table_info(urls)"
                ).fetchall()
            }

            if "etag" not in columns:
                self._connection.execute(
                    """
                    ALTER TABLE urls
                    ADD COLUMN etag TEXT
                    """
                )

            if "last_modified" not in columns:
                self._connection.execute(
                    """
                    ALTER TABLE urls
                    ADD COLUMN last_modified TEXT
                    """
                )

            self._connection.execute(
                """
                INSERT OR IGNORE INTO metadata(
                    key,
                    value
                )
                VALUES (
                    'schema_version',
                    ?
                )
                """,
                (
                    str(self.SCHEMA_VERSION),
                )
            )

            self._connection.execute(
                """
                UPDATE metadata
                SET value = ?
                WHERE key = 'schema_version'
                """,
                (
                    str(self.SCHEMA_VERSION),
                )
            )

            self._connection.commit()

    # ============================================================
    # URL INSERTION
    # ============================================================

    def add_discovered(
        self,
        url: str,
        document_id: str,
        host: str,
        priority: float = 50.0,
        source: Optional[str] = None,
        discovered_at: Optional[float] = None
    ) -> bool:
        """
        Add a URL if it does not already exist.

        Returns True only when a new URL is inserted.
        """

        if not url:
            raise ValueError(
                "url must not be empty"
            )

        if not document_id:
            raise ValueError(
                "document_id must not be empty"
            )

        if not host:
            raise ValueError(
                "host must not be empty"
            )

        timestamp = (
            time.time()
            if discovered_at is None
            else discovered_at
        )

        with self._lock:

            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO urls (
                    url,
                    document_id,
                    host,
                    state,
                    priority,
                    attempts,
                    source,
                    discovered_at
                )
                VALUES (
                    ?,
                    ?,
                    ?,
                    'discovered',
                    ?,
                    0,
                    ?,
                    ?
                )
                """,
                (
                    url,
                    document_id,
                    host,
                    float(priority),
                    source,
                    timestamp,
                )
            )

            self._connection.execute(
                """
                INSERT OR IGNORE INTO hosts (
                    host
                )
                VALUES (?)
                """,
                (
                    host,
                )
            )

            self._connection.commit()

            return cursor.rowcount == 1

    # ============================================================
    # URL READ
    # ============================================================

    def get(
        self,
        url: str
    ) -> Optional[Dict[str, Any]]:
        """Return one URL record or None."""

        with self._lock:

            row = self._connection.execute(
                """
                SELECT *
                FROM urls
                WHERE url = ?
                """,
                (
                    url,
                )
            ).fetchone()

        if row is None:
            return None

        return dict(row)

    def exists(
        self,
        url: str
    ) -> bool:
        """Return True when the URL exists."""

        with self._lock:

            row = self._connection.execute(
                """
                SELECT 1
                FROM urls
                WHERE url = ?
                LIMIT 1
                """,
                (
                    url,
                )
            ).fetchone()

        return row is not None

    # ============================================================
    # URL QUEUE STATE
    # ============================================================

    def mark_queued(
        self,
        url: str,
        priority: Optional[float] = None,
        next_crawl_at: Optional[float] = None
    ) -> bool:
        """
        Move a discovered/retry URL into queued state.

        Priority and next_crawl_at can be updated atomically.
        """

        with self._lock:

            if priority is None:

                cursor = self._connection.execute(
                    """
                    UPDATE urls
                    SET
                        state = 'queued',
                        next_crawl_at = ?
                    WHERE url = ?
                      AND state IN (
                          'discovered',
                          'retry'
                      )
                    """,
                    (
                        next_crawl_at,
                        url,
                    )
                )

            else:

                cursor = self._connection.execute(
                    """
                    UPDATE urls
                    SET
                        state = 'queued',
                        priority = ?,
                        next_crawl_at = ?
                    WHERE url = ?
                      AND state IN (
                          'discovered',
                          'retry'
                      )
                    """,
                    (
                        float(priority),
                        next_crawl_at,
                        url,
                    )
                )

            self._connection.commit()

            return cursor.rowcount == 1

    # ============================================================
    # HOST STATE
    # ============================================================

    def ensure_host(
        self,
        host: str,
        crawl_delay: float = 2.0
    ) -> Dict[str, Any]:
        """Create a host record if necessary and return it."""

        if not host:
            raise ValueError(
                "host must not be empty"
            )

        with self._lock:

            self._connection.execute(
                """
                INSERT OR IGNORE INTO hosts (
                    host,
                    crawl_delay
                )
                VALUES (
                    ?,
                    ?
                )
                """,
                (
                    host,
                    float(crawl_delay),
                )
            )

            self._connection.commit()

            return self.get_host(
                host
            )

    def get_host(
        self,
        host: str
    ) -> Optional[Dict[str, Any]]:
        """Return host scheduling state."""

        with self._lock:

            row = self._connection.execute(
                """
                SELECT *
                FROM hosts
                WHERE host = ?
                """,
                (
                    host,
                )
            ).fetchone()

        if row is None:
            return None

        return dict(row)

    def set_host_delay(
        self,
        host: str,
        crawl_delay: float
    ) -> bool:
        """Set the crawl delay for a host."""

        if crawl_delay < 0:
            raise ValueError(
                "crawl_delay must not be negative"
            )

        with self._lock:

            cursor = self._connection.execute(
                """
                UPDATE hosts
                SET crawl_delay = ?
                WHERE host = ?
                """,
                (
                    float(crawl_delay),
                    host,
                )
            )

            self._connection.commit()

            return cursor.rowcount == 1

    def reserve_host(
        self,
        host: str,
        now: Optional[float] = None
    ) -> Optional[float]:
        """
        Reserve the next crawl slot for a host.

        Returns the reserved timestamp.
        """

        timestamp = (
            time.time()
            if now is None
            else float(now)
        )

        with self._lock:

            row = self._connection.execute(
                """
                SELECT
                    crawl_delay,
                    next_allowed_time
                FROM hosts
                WHERE host = ?
                """,
                (
                    host,
                )
            ).fetchone()

            if row is None:
                return None

            current_ready = float(
                row["next_allowed_time"]
            )

            delay = float(
                row["crawl_delay"]
            )

            reserved_time = (
                max(
                    current_ready,
                    timestamp
                )
                + delay
            )

            self._connection.execute(
                """
                UPDATE hosts
                SET next_allowed_time = ?
                WHERE host = ?
                """,
                (
                    reserved_time,
                    host,
                )
            )

            self._connection.commit()

            return reserved_time

    def mark_host_crawled(
        self,
        host: str,
        crawled_at: Optional[float] = None
    ) -> bool:
        """Record the last crawl time for a host."""

        timestamp = (
            time.time()
            if crawled_at is None
            else float(crawled_at)
        )

        with self._lock:

            cursor = self._connection.execute(
                """
                UPDATE hosts
                SET last_crawl_time = ?
                WHERE host = ?
                """,
                (
                    timestamp,
                    host,
                )
            )

            self._connection.commit()

            return cursor.rowcount == 1

    # ============================================================
    # ATOMIC CLAIM
    # ============================================================

    def claim_next(
        self,
        owner: str,
        now: Optional[float] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Atomically claim the highest-priority ready URL.

        Host crawl timing is respected inside the same transaction.
        """

        if not owner:
            raise ValueError(
                "owner must not be empty"
            )

        timestamp = (
            time.time()
            if now is None
            else float(now)
        )

        with self._lock:

            connection = self._connection

            try:

                connection.execute(
                    "BEGIN IMMEDIATE"
                )

                row = connection.execute(
                    """
                    SELECT
                        u.*,
                        h.crawl_delay,
                        h.last_crawl_time,
                        h.next_allowed_time
                    FROM urls AS u
                    JOIN hosts AS h
                        ON h.host = u.host
                    WHERE u.state IN (
                        'discovered',
                        'queued',
                        'retry'
                    )
                    AND (
                        u.next_crawl_at IS NULL
                        OR u.next_crawl_at <= ?
                    )
                    AND MAX(
                        h.last_crawl_time
                        + h.crawl_delay,
                        h.next_allowed_time
                    ) <= ?
                    ORDER BY
                        u.priority DESC,
                        COALESCE(
                            u.next_crawl_at,
                            0
                        ) ASC,
                        u.discovered_at ASC,
                        u.url ASC
                    LIMIT 1
                    """,
                    (
                        timestamp,
                        timestamp,
                    )
                ).fetchone()

                if row is None:

                    connection.rollback()

                    return None

                url = row["url"]
                host = row["host"]

                cursor = connection.execute(
                    """
                    UPDATE urls
                    SET
                        state = 'leased',
                        lease_owner = ?,
                        leased_at = ?
                    WHERE url = ?
                      AND state IN (
                          'discovered',
                          'queued',
                          'retry'
                      )
                    """,
                    (
                        owner,
                        timestamp,
                        url,
                    )
                )

                if cursor.rowcount != 1:

                    connection.rollback()

                    return None

                current_ready = max(
                    float(
                        row["last_crawl_time"]
                    )
                    + float(
                        row["crawl_delay"]
                    ),
                    float(
                        row["next_allowed_time"]
                    ),
                    timestamp
                )

                reserved_time = (
                    current_ready
                    + float(
                        row["crawl_delay"]
                    )
                )

                connection.execute(
                    """
                    UPDATE hosts
                    SET next_allowed_time = ?
                    WHERE host = ?
                    """,
                    (
                        reserved_time,
                        host,
                    )
                )

                claimed = connection.execute(
                    """
                    SELECT *
                    FROM urls
                    WHERE url = ?
                    """,
                    (
                        url,
                    )
                ).fetchone()

                connection.commit()

                return dict(
                    claimed
                )

            except Exception:

                connection.rollback()

                raise

    # ============================================================
    # COMPLETE
    # ============================================================

    def mark_crawled(
        self,
        url: str,
        status: Optional[int] = None,
        crawled_at: Optional[float] = None,
        next_crawl_at: Optional[float] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None
    ) -> bool:
        """
        Mark a URL as successfully crawled.

        HTTP cache validators are persisted so future recrawls
        can use conditional requests.
        """

        timestamp = (
            time.time()
            if crawled_at is None
            else float(crawled_at)
        )

        with self._lock:

            row = self._connection.execute(
                """
                SELECT host
                FROM urls
                WHERE url = ?
                """,
                (
                    url,
                )
            ).fetchone()

            if row is None:
                return False

            host = row["host"]

            cursor = self._connection.execute(
                """
                UPDATE urls
                SET
                    state = 'crawled',
                    last_crawled_at = ?,
                    next_crawl_at = ?,
                    last_status = ?,
                    last_error = NULL,
                    lease_owner = NULL,
                    leased_at = NULL,
                    etag = ?,
                    last_modified = ?
                WHERE url = ?
                """,
                (
                    timestamp,
                    next_crawl_at,
                    status,
                    etag,
                    last_modified,
                    url,
                )
            )

            if cursor.rowcount == 1:

                self._connection.execute(
                    """
                    UPDATE hosts
                    SET last_crawl_time = ?
                    WHERE host = ?
                    """,
                    (
                        timestamp,
                        host,
                    )
                )

            self._connection.commit()

            return cursor.rowcount == 1

    # ============================================================
    # FAILURE / RETRY
    # ============================================================

    def mark_failed(
        self,
        url: str,
        error: Optional[str] = None,
        status: Optional[int] = None,
        retry_at: Optional[float] = None
    ) -> bool:
        """Mark a URL as failed or retry."""

        state = (
            "retry"
            if retry_at is not None
            else "failed"
        )

        with self._lock:

            cursor = self._connection.execute(
                """
                UPDATE urls
                SET
                    state = ?,
                    attempts = attempts + 1,
                    last_status = ?,
                    last_error = ?,
                    next_crawl_at = ?,
                    lease_owner = NULL,
                    leased_at = NULL
                WHERE url = ?
                """,
                (
                    state,
                    status,
                    error,
                    retry_at,
                    url,
                )
            )

            self._connection.commit()

            return cursor.rowcount == 1

    def release_lease(
        self,
        url: str,
        retry_at: Optional[float] = None,
        increment_attempts: bool = False
    ) -> bool:
        """Release a leased URL back to the queue."""

        state = (
            "retry"
            if retry_at is not None
            else "queued"
        )

        with self._lock:

            if increment_attempts:

                cursor = self._connection.execute(
                    """
                    UPDATE urls
                    SET
                        state = ?,
                        attempts = attempts + 1,
                        next_crawl_at = ?,
                        lease_owner = NULL,
                        leased_at = NULL
                    WHERE url = ?
                      AND state = 'leased'
                    """,
                    (
                        state,
                        retry_at,
                        url,
                    )
                )

            else:

                cursor = self._connection.execute(
                    """
                    UPDATE urls
                    SET
                        state = ?,
                        next_crawl_at = ?,
                        lease_owner = NULL,
                        leased_at = NULL
                    WHERE url = ?
                      AND state = 'leased'
                    """,
                    (
                        state,
                        retry_at,
                        url,
                    )
                )

            self._connection.commit()

            return cursor.rowcount == 1

    # ============================================================
    # LEASE RECOVERY
    # ============================================================

    def recover_expired_leases(
        self,
        lease_timeout: float,
        now: Optional[float] = None
    ) -> int:
        """Recover leases belonging to dead workers."""

        if lease_timeout < 0:
            raise ValueError(
                "lease_timeout must not be negative"
            )

        timestamp = (
            time.time()
            if now is None
            else float(now)
        )

        cutoff = (
            timestamp
            - float(lease_timeout)
        )

        with self._lock:

            cursor = self._connection.execute(
                """
                UPDATE urls
                SET
                    state = 'retry',
                    attempts = attempts + 1,
                    next_crawl_at = ?,
                    lease_owner = NULL,
                    leased_at = NULL
                WHERE state = 'leased'
                  AND leased_at IS NOT NULL
                  AND leased_at <= ?
                """,
                (
                    timestamp,
                    cutoff,
                )
            )

            self._connection.commit()

            return cursor.rowcount

    # ============================================================
    # PRIORITY
    # ============================================================

    def update_priority(
        self,
        url: str,
        priority: float
    ) -> bool:
        """Update the priority of an existing URL."""

        with self._lock:

            cursor = self._connection.execute(
                """
                UPDATE urls
                SET priority = ?
                WHERE url = ?
                """,
                (
                    float(priority),
                    url,
                )
            )

            self._connection.commit()

            return cursor.rowcount == 1

    # ============================================================
    # CLEAR
    # ============================================================

    def clear(self) -> None:
        """Clear all durable URL and host state."""

        with self._lock:

            self._connection.execute(
                "DELETE FROM urls"
            )

            self._connection.execute(
                "DELETE FROM hosts"
            )

            self._connection.commit()

    # ============================================================
    # COUNTS
    # ============================================================

    def count(
        self,
        state: Optional[str] = None
    ) -> int:
        """Return the number of URLs, optionally filtered by state."""

        with self._lock:

            if state is None:

                row = self._connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM urls
                    """
                ).fetchone()

            else:

                row = self._connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM urls
                    WHERE state = ?
                    """,
                    (
                        state,
                    )
                ).fetchone()

        return int(
            row["count"]
        )

    def counts(self) -> Dict[str, int]:
        """Return URL counts grouped by state."""

        with self._lock:

            rows = self._connection.execute(
                """
                SELECT
                    state,
                    COUNT(*) AS count
                FROM urls
                GROUP BY state
                """
            ).fetchall()

        return {
            row["state"]: int(
                row["count"]
            )
            for row in rows
        }

    def list_by_state(
        self,
        state: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Return a limited list of URLs in a state."""

        if limit <= 0:
            return []

        with self._lock:

            rows = self._connection.execute(
                """
                SELECT *
                FROM urls
                WHERE state = ?
                ORDER BY
                    priority DESC,
                    discovered_at ASC,
                    url ASC
                LIMIT ?
                """,
                (
                    state,
                    int(limit),
                )
            ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    # ============================================================
    # CLOSE
    # ============================================================

    def close(self) -> None:
        """Close the SQLite connection."""

        with self._lock:

            if self._connection is not None:

                self._connection.close()
                self._connection = None

    # ============================================================
    # CONTEXT MANAGER
    # ============================================================

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback
    ):
        self.close()
