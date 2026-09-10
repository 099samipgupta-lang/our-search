import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional


class URLStateStore:
    """
    Durable SQLite-backed state store for OUR SEARCH crawler URLs.

    This is the first local persistence layer for crawler state.
    The interface is intentionally independent from the existing
    frontier and crawler implementation so it can be replaced by
    distributed storage later.
    """

    SCHEMA_VERSION = 1

    def __init__(self, database_path: str = "crawler_storage/url_state.db"):
        self.database_path = os.path.abspath(database_path)

        directory = os.path.dirname(self.database_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            self.database_path,
            timeout=30,
            check_same_thread=False,
        )

        self._connection.row_factory = sqlite3.Row

        self._configure()
        self._initialize_schema()

    def _configure(self) -> None:
        """Configure SQLite for reliable local crawler-state storage."""

        with self._connection:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA busy_timeout = 30000")

    def _initialize_schema(self) -> None:
        """Create the crawler URL-state schema if it does not exist."""

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

                CREATE INDEX IF NOT EXISTS idx_urls_state_priority
                ON urls(state, priority DESC, next_crawl_at);

                CREATE INDEX IF NOT EXISTS idx_urls_host
                ON urls(host);

                CREATE INDEX IF NOT EXISTS idx_urls_state_host
                ON urls(state, host);

                CREATE INDEX IF NOT EXISTS idx_urls_lease
                ON urls(state, leased_at);

                INSERT OR IGNORE INTO metadata(key, value)
                VALUES ('schema_version', '1');
                """
            )

            self._connection.commit()

    def add_discovered(
        self,
        url: str,
        document_id: str,
        host: str,
        priority: float = 50.0,
        source: Optional[str] = None,
        discovered_at: Optional[float] = None,
    ) -> bool:
        """
        Add a URL if it does not already exist.

        Returns:
            True  -> URL was newly inserted.
            False -> URL already existed.
        """

        if not url:
            raise ValueError("url must not be empty")

        if not document_id:
            raise ValueError("document_id must not be empty")

        if not host:
            raise ValueError("host must not be empty")

        timestamp = time.time() if discovered_at is None else discovered_at

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
                VALUES (?, ?, ?, 'discovered', ?, 0, ?, ?)
                """,
                (
                    url,
                    document_id,
                    host,
                    float(priority),
                    source,
                    timestamp,
                ),
            )

            self._connection.commit()

            return cursor.rowcount == 1

    def get(self, url: str) -> Optional[Dict[str, Any]]:
        """Return one URL record or None."""

        with self._lock:
            row = self._connection.execute(
                """
                SELECT *
                FROM urls
                WHERE url = ?
                """,
                (url,),
            ).fetchone()

        if row is None:
            return None

        return dict(row)

    def exists(self, url: str) -> bool:
        """Return True if the URL is known to the crawler."""

        with self._lock:
            row = self._connection.execute(
                """
                SELECT 1
                FROM urls
                WHERE url = ?
                LIMIT 1
                """,
                (url,),
            ).fetchone()

        return row is not None

    def claim_next(
        self,
        owner: str,
        now: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Atomically claim the highest-priority URL that is ready.

        Only one claimant can successfully transition the selected
        URL into the 'leased' state during the transaction.
        """

        if not owner:
            raise ValueError("owner must not be empty")

        timestamp = time.time() if now is None else now

        with self._lock:
            connection = self._connection

            try:
                connection.execute("BEGIN IMMEDIATE")

                row = connection.execute(
                    """
                    SELECT *
                    FROM urls
                    WHERE state IN ('discovered', 'queued', 'retry')
                      AND (
                          next_crawl_at IS NULL
                          OR next_crawl_at <= ?
                      )
                    ORDER BY
                        priority DESC,
                        COALESCE(next_crawl_at, 0) ASC,
                        discovered_at ASC,
                        url ASC
                    LIMIT 1
                    """,
                    (timestamp,),
                ).fetchone()

                if row is None:
                    connection.rollback()
                    return None

                url = row["url"]

                cursor = connection.execute(
                    """
                    UPDATE urls
                    SET
                        state = 'leased',
                        lease_owner = ?,
                        leased_at = ?
                    WHERE url = ?
                      AND state IN ('discovered', 'queued', 'retry')
                    """,
                    (
                        owner,
                        timestamp,
                        url,
                    ),
                )

                if cursor.rowcount != 1:
                    connection.rollback()
                    return None

                claimed = connection.execute(
                    """
                    SELECT *
                    FROM urls
                    WHERE url = ?
                    """,
                    (url,),
                ).fetchone()

                connection.commit()

                return dict(claimed)

            except Exception:
                connection.rollback()
                raise

    def mark_queued(self, url: str) -> bool:
        """Move a discovered URL into the queued state."""

        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE urls
                SET state = 'queued'
                WHERE url = ?
                  AND state = 'discovered'
                """,
                (url,),
            )

            self._connection.commit()

            return cursor.rowcount == 1

    def mark_crawled(
        self,
        url: str,
        status: Optional[int] = None,
        crawled_at: Optional[float] = None,
        next_crawl_at: Optional[float] = None,
    ) -> bool:
        """Mark a leased URL as successfully crawled."""

        timestamp = time.time() if crawled_at is None else crawled_at

        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE urls
                SET
                    state = 'crawled',
                    last_crawled_at = ?,
                    next_crawl_at = ?,
                    last_status = ?,
                    lease_owner = NULL,
                    leased_at = NULL
                WHERE url = ?
                """,
                (
                    timestamp,
                    next_crawl_at,
                    status,
                    url,
                ),
            )

            self._connection.commit()

            return cursor.rowcount == 1

    def mark_failed(
        self,
        url: str,
        error: Optional[str] = None,
        status: Optional[int] = None,
        retry_at: Optional[float] = None,
    ) -> bool:
        """
        Mark a URL as failed.

        If retry_at is supplied, the URL becomes 'retry'.
        Otherwise it becomes 'failed'.
        """

        state = "retry" if retry_at is not None else "failed"

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
                ),
            )

            self._connection.commit()

            return cursor.rowcount == 1

    def release_lease(
        self,
        url: str,
        retry_at: Optional[float] = None,
        increment_attempts: bool = False,
    ) -> bool:
        """
        Release a leased URL back into the crawler.

        The URL becomes 'retry' when retry_at is supplied,
        otherwise it becomes 'queued'.
        """

        state = "retry" if retry_at is not None else "queued"

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
                    ),
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
                    ),
                )

            self._connection.commit()

            return cursor.rowcount == 1

    def recover_expired_leases(
        self,
        lease_timeout: float,
        now: Optional[float] = None,
    ) -> int:
        """
        Recover leases older than lease_timeout.

        This protects the crawler from permanently losing URLs when
        a worker disappears.
        """

        if lease_timeout < 0:
            raise ValueError("lease_timeout must not be negative")

        timestamp = time.time() if now is None else now
        cutoff = timestamp - lease_timeout

        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE urls
                SET
                    state = 'retry',
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
                ),
            )

            self._connection.commit()

            return cursor.rowcount

    def count(self, state: Optional[str] = None) -> int:
        """Return URL count, optionally filtered by state."""

        with self._lock:
            if state is None:
                row = self._connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM urls
                    """
                ).fetchone()
            else:
                row = self._connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM urls
                    WHERE state = ?
                    """,
                    (state,),
                ).fetchone()

        return int(row[0])

    def counts(self) -> Dict[str, int]:
        """Return counts grouped by crawler state."""

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT state, COUNT(*) AS count
                FROM urls
                GROUP BY state
                ORDER BY state
                """
            ).fetchall()

        return {
            row["state"]: int(row["count"])
            for row in rows
        }

    def list_by_state(
        self,
        state: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return URLs in a particular state."""

        if limit <= 0:
            return []

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT *
                FROM urls
                WHERE state = ?
                ORDER BY priority DESC, discovered_at ASC, url ASC
                LIMIT ?
                """,
                (
                    state,
                    int(limit),
                ),
            ).fetchall()

        return [dict(row) for row in rows]

    def close(self) -> None:
        """Close the SQLite connection."""

        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
