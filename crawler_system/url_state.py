import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional
from crawler_system.ready_frontier import ReadyFrontier
import tldextract


def _organization_domain(host: str) -> str:
    """Return the registrable domain used for organization-level scheduling."""

    host = str(host).strip().lower().rstrip(".")

    extracted = tldextract.extract(host)

    if extracted.domain and extracted.suffix:
        return f"{extracted.domain}.{extracted.suffix}"

    return host


class URLStateStore:
    """
    Durable SQLite-backed state store for OUR SEARCH crawler URLs.

    This store contains the durable state required by the crawler
    frontier. The interface is intentionally independent from the
    current frontier implementation so the storage layer can later
    be replaced by distributed storage.
    """

    SCHEMA_VERSION = 5

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

        # Stage 5.2 indexed scheduling frontier.
        self._ready_frontier = ReadyFrontier(self._connection)
        self._initialize_ready_frontier_triggers()

        # Existing databases may contain URL state created before the
        # ready frontier existed. Rebuild the scheduling index once.
        if self._ready_frontier.count() == 0 and self.count() > 0:
            self._ready_frontier.rebuild()

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

            existing_host_columns = {row[1] for row in self._connection.execute("PRAGMA table_info(hosts)").fetchall()}
            if existing_host_columns and "organization_domain" not in existing_host_columns:
                self._connection.execute("ALTER TABLE hosts ADD COLUMN organization_domain TEXT")
            self._connection.execute("CREATE TABLE IF NOT EXISTS organization_scheduler (organization_domain TEXT PRIMARY KEY, scheduler_last_claim REAL NOT NULL DEFAULT 0)")

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

                CREATE TABLE IF NOT EXISTS organization_scheduler (
                    organization_domain TEXT PRIMARY KEY,
                    scheduler_last_claim REAL NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS hosts (
                    host TEXT PRIMARY KEY,
                    organization_domain TEXT,

                    crawl_delay REAL NOT NULL DEFAULT 2.0,

                    last_crawl_time REAL NOT NULL DEFAULT 0,

                    next_allowed_time REAL NOT NULL DEFAULT 0,

                    failures INTEGER NOT NULL DEFAULT 0,
                    active_concurrency INTEGER NOT NULL DEFAULT 0,
                    max_concurrency INTEGER NOT NULL DEFAULT 1,
                    backoff_until REAL NOT NULL DEFAULT 0,
                    scheduler_last_claim REAL NOT NULL DEFAULT 0
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

            # ----------------------------------------------------
            # MIGRATION: v4 -> v5 domain scheduling state
            # ----------------------------------------------------
            host_columns = {
                row["name"]
                for row in self._connection.execute(
                    "PRAGMA table_info(hosts)"
                ).fetchall()
            }

            if "active_concurrency" not in host_columns:
                self._connection.execute(
                    """
                    ALTER TABLE hosts
                    ADD COLUMN active_concurrency INTEGER NOT NULL DEFAULT 0
                    """
                )

            if "max_concurrency" not in host_columns:
                self._connection.execute(
                    """
                    ALTER TABLE hosts
                    ADD COLUMN max_concurrency INTEGER NOT NULL DEFAULT 1
                    """
                )

            if "backoff_until" not in host_columns:
                self._connection.execute(
                    """
                    ALTER TABLE hosts
                    ADD COLUMN backoff_until REAL NOT NULL DEFAULT 0
                    """
                )

            self._connection.execute(
                """
                UPDATE hosts
                SET
                    active_concurrency = COALESCE(active_concurrency, 0),
                    max_concurrency = CASE
                        WHEN max_concurrency IS NULL OR max_concurrency <= 0
                        THEN 1
                        ELSE max_concurrency
                    END,
                    backoff_until = COALESCE(backoff_until, 0)
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
                CREATE INDEX IF NOT EXISTS idx_hosts_scheduler
                ON hosts(
                    next_allowed_time,
                    backoff_until,
                    active_concurrency
                )
                """
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

    # ============================================================
    # STAGE 5.2 — INDEXED READY FRONTIER
    # ============================================================

    def _initialize_ready_frontier_triggers(self) -> None:
        """
        Maintain ready_frontier transactionally from authoritative URL
        state. The urls table remains the source of truth.
        """
        with self._lock:
            self._connection.executescript(
                """
                DROP TRIGGER IF EXISTS trg_urls_ready_insert;
                DROP TRIGGER IF EXISTS trg_urls_ready_update;
                DROP TRIGGER IF EXISTS trg_urls_ready_delete;

                CREATE TRIGGER trg_urls_ready_insert
                AFTER INSERT ON urls
                WHEN NEW.state IN ('discovered', 'queued', 'retry')
                BEGIN
                    INSERT INTO ready_frontier (
                        url,
                        host,
                        priority,
                        ready_at,
                        sequence
                    )
                    VALUES (
                        NEW.url,
                        NEW.host,
                        NEW.priority,
                        COALESCE(
                            NEW.next_crawl_at,
                            NEW.discovered_at
                        ),
                        NEW.rowid
                    )
                    ON CONFLICT(url)
                    DO UPDATE SET
                        host = excluded.host,
                        priority = excluded.priority,
                        ready_at = excluded.ready_at,
                        sequence = excluded.sequence;
                END;

                CREATE TRIGGER trg_urls_ready_update
                AFTER UPDATE OF
                    state,
                    host,
                    priority,
                    next_crawl_at,
                    discovered_at
                ON urls
                BEGIN
                    DELETE FROM ready_frontier
                    WHERE url = NEW.url;

                    INSERT INTO ready_frontier (
                        url,
                        host,
                        priority,
                        ready_at,
                        sequence
                    )
                    SELECT
                        NEW.url,
                        NEW.host,
                        NEW.priority,
                        COALESCE(
                            NEW.next_crawl_at,
                            NEW.discovered_at
                        ),
                        NEW.rowid
                    WHERE NEW.state IN (
                        'discovered',
                        'queued',
                        'retry'
                    );
                END;

                CREATE TRIGGER trg_urls_ready_delete
                AFTER DELETE ON urls
                BEGIN
                    DELETE FROM ready_frontier
                    WHERE url = OLD.url;
                END;
                """
            )
            self._connection.commit()

    def ready_size(self) -> int:
        """Return the number of indexed ready-work entries."""
        with self._lock:
            return self._ready_frontier.count()

    def rebuild_ready_frontier(self) -> int:
        """Rebuild the scheduling index from authoritative URL state."""
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                count = self._ready_frontier.rebuild(
                    commit=False
                )
                self._connection.commit()
                return count
            except Exception:
                self._connection.rollback()
                raise

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

            organization_domain = _organization_domain(host)

            self._connection.execute(
                """
                INSERT OR IGNORE INTO hosts (
                    host,
                    organization_domain
                )
                VALUES (?, ?)
                """,
                (
                    host,
                    organization_domain,
                )
            )

            self._connection.execute(
                """
                INSERT OR IGNORE INTO organization_scheduler (
                    organization_domain
                )
                VALUES (?)
                """,
                (
                    organization_domain,
                )
            )

            self._connection.commit()

            return cursor.rowcount == 1

    # ============================================================
    # DUE URL REACTIVATION
    # ============================================================

    def list_due_urls(
        self,
        now: Optional[float] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Return crawled URLs whose next crawl time has arrived.

        These URLs remain durable in SQLite while they wait.
        The crawler can periodically move them back to the
        frontier without treating them as newly discovered URLs.
        """

        if now is None:
            now = time.time()

        limit = max(1, int(limit))

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT *
                FROM urls
                WHERE state = 'crawled'
                  AND next_crawl_at IS NOT NULL
                  AND next_crawl_at <= ?
                ORDER BY
                    priority DESC,
                    next_crawl_at ASC
                LIMIT ?
                """,
                (
                    float(now),
                    limit,
                )
            ).fetchall()

            return [dict(row) for row in rows]

    # ============================================================
    # DUE URL REACTIVATION
    # ============================================================

    def reactivate_due(
        self,
        url: str,
        now: Optional[float] = None
    ) -> bool:
        """
        Reactivate one crawled URL whose recrawl time has arrived.

        This is intentionally different from add_discovered():
        the URL already exists and must not be treated as a new
        discovery.
        """

        if now is None:
            now = time.time()

        with self._lock:

            cursor = self._connection.execute(
                """
                UPDATE urls
                SET
                    state = 'queued',
                    lease_owner = NULL,
                    leased_at = NULL,
                    last_error = NULL
                WHERE url = ?
                  AND state = 'crawled'
                  AND next_crawl_at IS NOT NULL
                  AND next_crawl_at <= ?
                """,
                (
                    url,
                    float(now),
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

    def force_requeue(
        self,
        url: str,
        priority: Optional[float] = None,
    ) -> bool:
        """Force an existing crawled URL back into the crawl queue."""

        with self._lock:
            if priority is None:
                cursor = self._connection.execute(
                    """
                    UPDATE urls
                    SET
                        state = 'queued',
                        lease_owner = NULL,
                        leased_at = NULL,
                        last_error = NULL,
                        next_crawl_at = NULL
                    WHERE url = ?
                      AND state = 'crawled'
                    """,
                    (url,),
                )
            else:
                cursor = self._connection.execute(
                    """
                    UPDATE urls
                    SET
                        state = 'queued',
                        priority = ?,
                        lease_owner = NULL,
                        leased_at = NULL,
                        last_error = NULL,
                        next_crawl_at = NULL
                    WHERE url = ?
                      AND state = 'crawled'
                    """,
                    (float(priority), url),
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

            organization_domain = _organization_domain(host)

            self._connection.execute(
                """
                INSERT OR IGNORE INTO hosts (
                    host,
                    organization_domain,
                    crawl_delay
                )
                VALUES (
                    ?,
                    ?,
                    ?
                )
                """,
                (
                    host,
                    organization_domain,
                    float(crawl_delay),
                )
            )

            self._connection.execute(
                """
                INSERT OR IGNORE INTO organization_scheduler (
                    organization_domain
                )
                VALUES (?)
                """,
                (
                    organization_domain,
                )
            )

            self._connection.commit()

            return self.get_host(
                host
            )

    def ensure_domain_scheduler_state(
        self,
        host: str,
        max_concurrency: int = 1,
    ) -> Dict[str, Any]:
        """Ensure durable scheduler state exists for a host."""

        if not host:
            raise ValueError("host must not be empty")

        if int(max_concurrency) <= 0:
            raise ValueError(
                "max_concurrency must be greater than zero"
            )

        with self._lock:
            self._connection.execute(
                """
                UPDATE hosts
                SET
                    active_concurrency = COALESCE(active_concurrency, 0),
                    max_concurrency = CASE
                        WHEN max_concurrency IS NULL OR max_concurrency <= 0
                        THEN ?
                        ELSE max_concurrency
                    END,
                    backoff_until = COALESCE(backoff_until, 0)
                WHERE host = ?
                """,
                (
                    int(max_concurrency),
                    host,
                ),
            )

            self._connection.commit()

            record = self.get_host(host)

            if record is None:
                raise KeyError(f"unknown host: {host}")

            return record

    def set_host_max_concurrency(
        self,
        host: str,
        max_concurrency: int,
    ) -> Dict[str, Any]:
        """Set the durable maximum concurrency for a host."""

        if int(max_concurrency) <= 0:
            raise ValueError(
                "max_concurrency must be greater than zero"
            )

        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE hosts
                SET max_concurrency = ?
                WHERE host = ?
                """,
                (
                    int(max_concurrency),
                    host,
                ),
            )

            if cursor.rowcount != 1:
                self._connection.commit()
                raise KeyError(f"unknown host: {host}")

            self._connection.commit()

            record = self.get_host(host)

            if record is None:
                raise KeyError(f"unknown host: {host}")

            return record

    def acquire_host_slot(
        self,
        host: str,
        now: Optional[float] = None,
    ) -> bool:
        """Atomically acquire one available domain scheduling slot."""

        timestamp = (
            time.time()
            if now is None
            else float(now)
        )

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")

            row = self._connection.execute(
                """
                SELECT
                    crawl_delay,
                    last_crawl_time,
                    next_allowed_time,
                    active_concurrency,
                    max_concurrency,
                    backoff_until
                FROM hosts
                WHERE host = ?
                """,
                (host,),
            ).fetchone()

            if row is None:
                self._connection.rollback()
                return False

            ready_at = max(
                float(row["last_crawl_time"]) + float(row["crawl_delay"]),
                float(row["next_allowed_time"]),
                float(row["backoff_until"]),
            )

            if ready_at > timestamp:
                self._connection.rollback()
                return False

            if int(row["active_concurrency"]) >= int(
                row["max_concurrency"]
            ):
                self._connection.rollback()
                return False

            reserved_until = max(
                ready_at,
                timestamp,
            ) + float(row["crawl_delay"])

            cursor = self._connection.execute(
                """
                UPDATE hosts
                SET
                    active_concurrency = active_concurrency + 1,
                    next_allowed_time = ?
                WHERE host = ?
                  AND active_concurrency < max_concurrency
                """,
                (
                    reserved_until,
                    host,
                ),
            )

            if cursor.rowcount != 1:
                self._connection.rollback()
                return False

            self._connection.commit()
            return True

    def release_host_slot(self, host: str) -> bool:
        """Release one active domain scheduling slot."""

        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE hosts
                SET active_concurrency =
                    CASE
                        WHEN active_concurrency > 0
                        THEN active_concurrency - 1
                        ELSE 0
                    END
                WHERE host = ?
                """,
                (host,),
            )

            self._connection.commit()
            return cursor.rowcount == 1

    def record_domain_success(
        self,
        host: str,
        crawled_at: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Reset domain failure state after successful crawling."""

        timestamp = (
            time.time()
            if crawled_at is None
            else float(crawled_at)
        )

        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE hosts
                SET
                    last_crawl_time = ?,
                    failures = 0,
                    backoff_until = 0
                WHERE host = ?
                """,
                (
                    timestamp,
                    host,
                ),
            )

            if cursor.rowcount != 1:
                self._connection.commit()
                raise KeyError(f"unknown host: {host}")

            self._connection.commit()

            record = self.get_host(host)

            if record is None:
                raise KeyError(f"unknown host: {host}")

            return record

    def record_domain_failure(
        self,
        host: str,
        failed_at: Optional[float] = None,
        backoff_base: float = 30.0,
        backoff_max: float = 3600.0,
        retryable: bool = True,
    ) -> Dict[str, Any]:
        """Record a domain failure and apply bounded exponential backoff."""

        if backoff_base < 0:
            raise ValueError(
                "backoff_base must not be negative"
            )

        if backoff_max < backoff_base:
            raise ValueError(
                "backoff_max must be greater than or equal to backoff_base"
            )

        timestamp = (
            time.time()
            if failed_at is None
            else float(failed_at)
        )

        with self._lock:
            row = self._connection.execute(
                """
                SELECT failures
                FROM hosts
                WHERE host = ?
                """,
                (host,),
            ).fetchone()

            if row is None:
                self._connection.rollback()
                raise KeyError(f"unknown host: {host}")

            failures = int(row["failures"]) + 1

            if retryable and backoff_base > 0:
                delay = min(
                    float(backoff_base) * (2 ** (failures - 1)),
                    float(backoff_max),
                )
                backoff_until = timestamp + delay
            else:
                backoff_until = timestamp

            self._connection.execute(
                """
                UPDATE hosts
                SET
                    failures = ?,
                    backoff_until = ?
                WHERE host = ?
                """,
                (
                    failures,
                    backoff_until,
                    host,
                ),
            )

            self._connection.commit()

            record = self.get_host(host)

            if record is None:
                raise KeyError(f"unknown host: {host}")

            return record

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
        now: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """Claim the next ready URL for crawling."""

        if not owner:
            raise ValueError("owner must not be empty")

        timestamp = (
            time.time()
            if now is None
            else float(now)
        )

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")

            frontier_rows = self._connection.execute(
                """
                SELECT
                    rf.url,
                    rf.host,
                    rf.priority,
                    rf.ready_at,
                    rf.sequence
                FROM ready_frontier AS rf
                JOIN hosts AS h
                  ON h.host = rf.host
                JOIN organization_scheduler AS os
                  ON os.organization_domain = h.organization_domain
                WHERE
                    h.active_concurrency < h.max_concurrency
                ORDER BY
                    os.scheduler_last_claim ASC,
                    h.scheduler_last_claim ASC,
                    rf.ready_at ASC,
                    rf.priority DESC,
                    rf.sequence ASC
                LIMIT 256
                """
            ).fetchall()

            row = None

            for frontier_row in frontier_rows:
                url = str(frontier_row["url"])

                url_row = self._connection.execute(
                    """
                    SELECT *
                    FROM urls
                    WHERE url = ?
                      AND state IN (
                          'discovered',
                          'queued',
                          'retry'
                      )
                    """,
                    (url,),
                ).fetchone()

                if url_row is None:
                    continue

                next_crawl_at = url_row["next_crawl_at"]

                if (
                    next_crawl_at is not None
                    and float(next_crawl_at) > timestamp
                ):
                    continue

                host = str(url_row["host"])

                host_row = self._connection.execute(
                    """
                    SELECT *
                    FROM hosts
                    WHERE host = ?
                    """,
                    (host,),
                ).fetchone()

                if host_row is None:
                    continue

                host_ready_at = max(
                    float(host_row["last_crawl_time"])
                    + float(host_row["crawl_delay"]),
                    float(host_row["next_allowed_time"]),
                    float(host_row["backoff_until"]),
                )

                if host_ready_at > timestamp:
                    continue

                if (
                    int(host_row["active_concurrency"])
                    >= int(host_row["max_concurrency"])
                ):
                    continue

                row = url_row
                break

            if row is None:
                self._connection.rollback()
                return None

            host = str(row["host"])

            host_row = self._connection.execute(
                """
                SELECT
                    crawl_delay,
                    last_crawl_time,
                    next_allowed_time,
                    backoff_until,
                    active_concurrency,
                    max_concurrency
                FROM hosts
                WHERE host = ?
                """,
                (host,),
            ).fetchone()

            if host_row is None:
                self._connection.rollback()
                return None

            delay = float(host_row["crawl_delay"])

            reserved_time = (
                max(
                    float(host_row["last_crawl_time"]) + delay,
                    float(host_row["next_allowed_time"]),
                    float(host_row["backoff_until"]),
                    timestamp,
                )
                + delay
            )

            host_cursor = self._connection.execute(
                """
                UPDATE hosts
                SET
                    active_concurrency =
                        active_concurrency + 1,
                    next_allowed_time = ?
                WHERE host = ?
                  AND active_concurrency < max_concurrency
                  AND MAX(
                      last_crawl_time + crawl_delay,
                      next_allowed_time,
                      backoff_until
                  ) <= ?
                """,
                (
                    reserved_time,
                    host,
                    timestamp,
                ),
            )

            if host_cursor.rowcount != 1:
                self._connection.rollback()
                return None

            url_cursor = self._connection.execute(
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
                    row["url"],
                ),
            )

            if url_cursor.rowcount != 1:
                self._connection.rollback()
                return None

            organization_domain = str(
                row["host"]
            )

            organization_row = self._connection.execute(
                """
                SELECT organization_domain
                FROM hosts
                WHERE host = ?
                """,
                (host,),
            ).fetchone()

            if organization_row is None:
                self._connection.rollback()
                raise RuntimeError(
                    f"host disappeared after lease: {host}"
                )

            organization_domain = str(
                organization_row["organization_domain"]
            )

            self._connection.execute(
                """
                UPDATE organization_scheduler
                SET scheduler_last_claim = ?
                WHERE organization_domain = ?
                """,
                (
                    timestamp,
                    organization_domain,
                ),
            )

            self._connection.execute(
                """
                UPDATE hosts
                SET scheduler_last_claim = ?
                WHERE host = ?
                """,
                (timestamp, host),
            )

            result = self._connection.execute(
                """
                SELECT *
                FROM urls
                WHERE url = ?
                """,
                (row["url"],),
            ).fetchone()

            if result is None:
                self._connection.rollback()
                raise RuntimeError(
                    f"claimed URL disappeared: {row['url']}"
                )

            self._connection.commit()

            return dict(result)

    def claim_next_batch(
        self,
        owner: str,
        limit: int,
        now: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Atomically claim up to ``limit`` ready URLs."""

        if not owner:
            raise ValueError("owner must not be empty")

        if limit <= 0:
            return []

        timestamp = time.time() if now is None else float(now)
        claimed: List[Dict[str, Any]] = []

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")

            # Inspect only a bounded candidate window instead of loading
            # the entire ready frontier into memory.
            candidate_limit = max(int(limit) * 512, 4096)

            frontier_rows = self._connection.execute(
                   """
                   WITH eligible AS (
                       SELECT
                           rf.url,
                           rf.host,
                           rf.priority,
                           rf.ready_at,
                           rf.sequence,
                           h.max_concurrency - h.active_concurrency AS available_slots,
                           ROW_NUMBER() OVER (
                               PARTITION BY rf.host
                               ORDER BY
                                   rf.priority DESC,
                                   rf.ready_at ASC,
                                   rf.sequence ASC
                           ) AS host_rank
                       FROM ready_frontier AS rf
                       JOIN urls AS u
                           ON u.url = rf.url
                       JOIN hosts AS h
                           ON h.host = rf.host
                       WHERE u.state IN ('discovered', 'queued', 'retry')
                         AND (
                               u.next_crawl_at IS NULL
                               OR u.next_crawl_at <= ?
                         )
                         AND MAX(
                               h.last_crawl_time + h.crawl_delay,
                               h.next_allowed_time,
                               h.backoff_until
                             ) <= ?
                         AND h.active_concurrency < h.max_concurrency
                   )
                   SELECT
                       url,
                       host,
                       priority,
                       ready_at,
                       sequence
                   FROM eligible
                   WHERE host_rank <= available_slots
                   ORDER BY
                       priority DESC,
                       ready_at ASC,
                       sequence ASC
                   LIMIT ?
                   """,
                   (
                       timestamp,
                       timestamp,
                       candidate_limit,
                   ),
               ).fetchall()


            for frontier_row in frontier_rows:
                if len(claimed) >= limit:
                    break

                url = str(frontier_row["url"])
                host = str(frontier_row["host"])

                # Re-read the host inside this transaction because an
                # earlier claim in this same batch may have changed it.
                host_row = self._connection.execute(
                    """
                    SELECT
                        active_concurrency,
                        max_concurrency,
                        last_crawl_time,
                        crawl_delay,
                        next_allowed_time,
                        backoff_until
                    FROM hosts
                    WHERE host = ?
                    """,
                    (host,),
                ).fetchone()

                if host_row is None:
                    continue

                active = int(host_row["active_concurrency"])
                maximum = int(host_row["max_concurrency"])

                if active >= maximum:
                    continue

                host_ready_at = max(
                    float(host_row["last_crawl_time"])
                    + float(host_row["crawl_delay"]),
                    float(host_row["next_allowed_time"]),
                    float(host_row["backoff_until"]),
                )

                if host_ready_at > timestamp:
                    continue

                delay = float(host_row["crawl_delay"])

                reserved_time = (
                    max(
                        float(host_row["last_crawl_time"]) + delay,
                        float(host_row["next_allowed_time"]),
                        float(host_row["backoff_until"]),
                        timestamp,
                    )
                    + delay
                )

                host_cursor = self._connection.execute(
                    """
                    UPDATE hosts
                    SET
                        active_concurrency = active_concurrency + 1,
                        next_allowed_time = ?
                    WHERE host = ?
                      AND active_concurrency < max_concurrency
                      AND MAX(
                            last_crawl_time + crawl_delay,
                            next_allowed_time,
                            backoff_until
                          ) <= ?
                    """,
                    (
                        reserved_time,
                        host,
                        timestamp,
                    ),
                )

                if host_cursor.rowcount != 1:
                    continue

                url_cursor = self._connection.execute(
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

                if url_cursor.rowcount != 1:
                    self._connection.execute(
                        """
                        UPDATE hosts
                        SET active_concurrency =
                            CASE
                                WHEN active_concurrency > 0
                                THEN active_concurrency - 1
                                ELSE 0
                            END
                        WHERE host = ?
                        """,
                        (host,),
                    )
                    continue

                result = self._connection.execute(
                    """
                    SELECT *
                    FROM urls
                    WHERE url = ?
                    """,
                    (url,),
                ).fetchone()

                if result is None:
                    self._connection.execute(
                        """
                        UPDATE hosts
                        SET active_concurrency =
                            CASE
                                WHEN active_concurrency > 0
                                THEN active_concurrency - 1
                                ELSE 0
                            END
                        WHERE host = ?
                        """,
                        (host,),
                    )
                    continue

                claimed.append(dict(result))

            self._connection.commit()

        return claimed

    def mark_crawled(
        self,
        url: str,
        status: Optional[int] = None,
        error: Optional[str] = None,
        crawled_at: Optional[float] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        next_crawl_at: Optional[float] = None,
    ) -> bool:
        """Mark a leased URL crawled and release its domain slot."""

        timestamp = (
            time.time()
            if crawled_at is None
            else float(crawled_at)
        )

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")

            row = self._connection.execute(
                """
                SELECT host, state
                FROM urls
                WHERE url = ?
                """,
                (url,),
            ).fetchone()

            if row is None or row["state"] != "leased":
                self._connection.rollback()
                return False

            host = str(row["host"])

            cursor = self._connection.execute(
                """
                UPDATE urls
                SET
                    state = 'crawled',
                    last_crawled_at = ?,
                    last_status = ?,
                    last_error = ?,
                    lease_owner = NULL,
                    leased_at = NULL,
                    etag = ?,
                    last_modified = ?,
                    next_crawl_at = ?
                WHERE url = ?
                  AND state = 'leased'
                """,
                (
                    timestamp,
                    status,
                    error,
                    etag,
                    last_modified,
                    None if next_crawl_at is None else float(next_crawl_at),
                    url,
                ),
            )

            if cursor.rowcount != 1:
                self._connection.rollback()
                return False

            self._connection.execute(
                """
                UPDATE hosts
                SET
                    active_concurrency =
                        CASE
                            WHEN active_concurrency > 0
                            THEN active_concurrency - 1
                            ELSE 0
                        END,
                    last_crawl_time = ?,
                    failures = 0,
                    backoff_until = 0
                WHERE host = ?
                """,
                (
                    timestamp,
                    host,
                ),
            )

            self._connection.commit()
            return True

    def mark_failed(
        self,
        url: str,
        error: Optional[str] = None,
        status: Optional[int] = None,
        retry_at: Optional[float] = None,
        backoff_base: float = 30.0,
        backoff_max: float = 3600.0,
        retryable: bool = True,
    ) -> bool:
        """Mark a leased URL failed, release its slot, and adapt backoff."""

        if backoff_base < 0:
            raise ValueError(
                "backoff_base must not be negative"
            )

        if backoff_max < backoff_base:
            raise ValueError(
                "backoff_max must be greater than or equal to backoff_base"
            )

        timestamp = time.time()

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")

            row = self._connection.execute(
                """
                SELECT host, state
                FROM urls
                WHERE url = ?
                """,
                (url,),
            ).fetchone()

            if row is None or row["state"] != "leased":
                self._connection.rollback()
                return False

            host = str(row["host"])

            host_row = self._connection.execute(
                """
                SELECT failures
                FROM hosts
                WHERE host = ?
                """,
                (host,),
            ).fetchone()

            if host_row is None:
                self._connection.rollback()
                return False

            failures = int(host_row["failures"]) + 1

            if retryable and backoff_base > 0:
                delay = min(
                    float(backoff_base) * (2 ** (failures - 1)),
                    float(backoff_max),
                )
                backoff_until = timestamp + delay
            else:
                backoff_until = timestamp

            state = (
                "retry"
                if retry_at is not None
                else "failed"
            )

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
                  AND state = 'leased'
                """,
                (
                    state,
                    status,
                    error,
                    retry_at,
                    url,
                ),
            )

            if cursor.rowcount != 1:
                self._connection.rollback()
                return False

            self._connection.execute(
                """
                UPDATE hosts
                SET
                    active_concurrency =
                        CASE
                            WHEN active_concurrency > 0
                            THEN active_concurrency - 1
                            ELSE 0
                        END,
                    failures = ?,
                    backoff_until = ?
                WHERE host = ?
                """,
                (
                    failures,
                    backoff_until,
                    host,
                ),
            )

            self._connection.commit()
            return True

    def release_lease(
        self,
        url: str,
        retry_at: Optional[float] = None,
        increment_attempts: bool = False
    ) -> bool:
        """Release a leased URL and exactly one domain slot."""

        state = (
            "retry"
            if retry_at is not None
            else "queued"
        )

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")

            row = self._connection.execute(
                """
                SELECT host, state
                FROM urls
                WHERE url = ?
                """,
                (url,),
            ).fetchone()

            if row is None or row["state"] != "leased":
                self._connection.rollback()
                return False

            host = str(row["host"])

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

            if cursor.rowcount != 1:
                self._connection.rollback()
                return False

            self._connection.execute(
                """
                UPDATE hosts
                SET active_concurrency =
                    CASE
                        WHEN active_concurrency > 0
                        THEN active_concurrency - 1
                        ELSE 0
                    END
                WHERE host = ?
                """,
                (host,),
            )

            self._connection.commit()
            return True

    def recover_expired_leases(
        self,
        lease_timeout: float,
        now: Optional[float] = None
    ) -> int:
        """Recover expired leases and release their domain slots."""

        if lease_timeout < 0:
            raise ValueError(
                "lease_timeout must not be negative"
            )

        timestamp = (
            time.time()
            if now is None
            else float(now)
        )

        cutoff = timestamp - float(lease_timeout)

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")

            rows = self._connection.execute(
                """
                SELECT host, COUNT(*) AS count
                FROM urls
                WHERE state = 'leased'
                  AND leased_at IS NOT NULL
                  AND leased_at <= ?
                GROUP BY host
                """,
                (cutoff,),
            ).fetchall()

            if not rows:
                self._connection.rollback()
                return 0

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
                ),
            )

            recovered = cursor.rowcount

            for row in rows:
                amount = int(row["count"])

                self._connection.execute(
                    """
                    UPDATE hosts
                    SET active_concurrency =
                        CASE
                            WHEN active_concurrency >= ?
                            THEN active_concurrency - ?
                            ELSE 0
                        END
                    WHERE host = ?
                    """,
                    (
                        amount,
                        amount,
                        row["host"],
                    ),
                )

            self._connection.commit()
            return recovered

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
