import sqlite3
import time
from pathlib import Path
from threading import Lock


class ExpansionQueue:

    def __init__(self, database_path, lease_timeout=60.0):
        self.database_path = str(database_path)
        self.lease_timeout = max(1.0, float(lease_timeout))
        self._lock = Lock()

        Path(self.database_path).parent.mkdir(
            parents=True,
            exist_ok=True
        )

        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(
            self.database_path,
            timeout=30
        )
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS expansion_queue (
                        hostname TEXT PRIMARY KEY,
                        first_url TEXT NOT NULL,
                        source_hostname TEXT,
                        priority REAL NOT NULL,
                        discovered_at REAL NOT NULL,
                        status TEXT NOT NULL DEFAULT 'queued',
                        processing_started_at REAL
                    )
                    """
                )

                columns = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(expansion_queue)"
                    ).fetchall()
                }

                if "processing_started_at" not in columns:
                    connection.execute(
                        """
                        ALTER TABLE expansion_queue
                        ADD COLUMN processing_started_at REAL
                        """
                    )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_expansion_queue_ready
                    ON expansion_queue(
                        status,
                        priority DESC,
                        discovered_at ASC
                    )
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_expansion_queue_processing
                    ON expansion_queue(
                        status,
                        processing_started_at
                    )
                    """
                )

                connection.commit()

            finally:
                connection.close()

    def enqueue(self, candidate, priority):
        if not isinstance(candidate, dict):
            return False

        hostname = candidate.get("hostname")
        first_url = candidate.get("first_url")

        if not hostname or not first_url:
            return False

        hostname = hostname.rstrip(".").lower()

        source_hostname = candidate.get(
            "source_hostname"
        )

        discovered_at = float(
            candidate.get(
                "discovered_at",
                0.0
            )
        )

        priority = float(priority)

        with self._lock:
            connection = self._connect()

            try:
                connection.execute("BEGIN IMMEDIATE")

                existing = connection.execute(
                    """
                    SELECT status
                    FROM expansion_queue
                    WHERE hostname = ?
                    """,
                    (hostname,)
                ).fetchone()

                if existing is None:

                    cursor = connection.execute(
                        """
                        INSERT INTO expansion_queue (
                            hostname,
                            first_url,
                            source_hostname,
                            priority,
                            discovered_at,
                            status,
                            processing_started_at
                        )
                        VALUES (?, ?, ?, ?, ?, 'queued', NULL)
                        """,
                        (
                            hostname,
                            first_url,
                            source_hostname,
                            priority,
                            discovered_at
                        )
                    )

                    connection.commit()

                    return cursor.rowcount == 1

                # A failed expansion can be safely retried.
                #
                # Keep the same durable queue row and reset
                # the processing lease.
                if existing["status"] == "failed":

                    cursor = connection.execute(
                        """
                        UPDATE expansion_queue
                        SET
                            first_url = ?,
                            source_hostname = ?,
                            priority = ?,
                            discovered_at = ?,
                            status = 'queued',
                            processing_started_at = NULL
                        WHERE hostname = ?
                          AND status = 'failed'
                        """,
                        (
                            first_url,
                            source_hostname,
                            priority,
                            discovered_at,
                            hostname
                        )
                    )

                    connection.commit()

                    return cursor.rowcount == 1

                # queued / processing / complete entries already
                # exist, so do not create duplicate work.
                connection.commit()

                return False

            except Exception:
                connection.rollback()
                raise

            finally:
                connection.close()

    def next(self):
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT
                        hostname,
                        first_url,
                        source_hostname,
                        priority,
                        discovered_at,
                        status,
                        processing_started_at
                    FROM expansion_queue
                    WHERE status = 'queued'
                    ORDER BY priority DESC, discovered_at ASC
                    LIMIT 1
                    """
                ).fetchone()

                if row is None:
                    return None

                return dict(row)

            finally:
                connection.close()

    def claim_next(self):
        with self._lock:
            connection = self._connect()

            try:
                connection.execute("BEGIN IMMEDIATE")

                row = connection.execute(
                    """
                    SELECT hostname
                    FROM expansion_queue
                    WHERE status = 'queued'
                    ORDER BY priority DESC, discovered_at ASC
                    LIMIT 1
                    """
                ).fetchone()

                if row is None:
                    connection.commit()
                    return None

                hostname = row["hostname"]
                now = time.time()

                updated = connection.execute(
                    """
                    UPDATE expansion_queue
                    SET
                        status = 'processing',
                        processing_started_at = ?
                    WHERE hostname = ?
                      AND status = 'queued'
                    """,
                    (
                        now,
                        hostname
                    )
                )

                if updated.rowcount != 1:
                    connection.rollback()
                    return None

                result = connection.execute(
                    """
                    SELECT
                        hostname,
                        first_url,
                        source_hostname,
                        priority,
                        discovered_at,
                        status,
                        processing_started_at
                    FROM expansion_queue
                    WHERE hostname = ?
                    """,
                    (hostname,)
                ).fetchone()

                connection.commit()

                if result is None:
                    return None

                return dict(result)

            except Exception:
                connection.rollback()
                raise

            finally:
                connection.close()

    def recover_expired_leases(self, now=None):
        """
        Recover expansion candidates whose processing lease
        has expired.

        Returns the number of recovered candidates.
        """
        if now is None:
            now = time.time()

        cutoff = float(now) - self.lease_timeout

        with self._lock:
            connection = self._connect()

            try:
                connection.execute("BEGIN IMMEDIATE")

                cursor = connection.execute(
                    """
                    UPDATE expansion_queue
                    SET
                        status = 'queued',
                        processing_started_at = NULL
                    WHERE status = 'processing'
                      AND processing_started_at IS NOT NULL
                      AND processing_started_at <= ?
                    """,
                    (cutoff,)
                )

                connection.commit()

                return cursor.rowcount

            except Exception:
                connection.rollback()
                raise

            finally:
                connection.close()

    def mark_complete(self, hostname):
        return self._mark_status(
            hostname,
            "complete"
        )

    def mark_failed(self, hostname):
        return self._mark_status(
            hostname,
            "failed"
        )

    def _mark_status(self, hostname, status):
        if not isinstance(hostname, str):
            return False

        hostname = hostname.rstrip(".").lower()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE expansion_queue
                    SET
                        status = ?,
                        processing_started_at = NULL
                    WHERE hostname = ?
                    """,
                    (
                        status,
                        hostname
                    )
                )

                connection.commit()

                return cursor.rowcount == 1

            finally:
                connection.close()

    def count(self, status=None):
        with self._lock:
            connection = self._connect()

            try:
                if status is None:
                    row = connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM expansion_queue
                        """
                    ).fetchone()
                else:
                    row = connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM expansion_queue
                        WHERE status = ?
                        """,
                        (status,)
                    ).fetchone()

                return int(row[0])

            finally:
                connection.close()
